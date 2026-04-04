import { supabaseRequest } from './supabase.js';
import { normalizeEmail, normalizePhone, strTrim, uuidOk } from './strings.js';
import {
  applyIntentContact,
  applyIntentFirstVisit,
  applyIntentNewPage,
  intentTierFromScore,
} from './intentScore.js';
import {
  attachProfileByFilter,
  attachProfileToRecord,
  ensurePersonProfile,
  patchPersonProfile,
  refreshPersonProfileStats,
} from './personProfiles.js';

function firstTouch(newVal, existing, max) {
  const n = strTrim(newVal, max);
  if (n != null && n !== '') return n;
  const e = strTrim(existing, max);
  return e != null && e !== '' ? e : null;
}

/** Keep the first non-empty value (for landing page / source page on visitors). */
function preserveFirst(existing, newVal, max) {
  const e = strTrim(existing, max);
  if (e != null && e !== '') return e;
  return strTrim(newVal, max);
}

async function fetchLeadForMerge(cfg, leadId) {
  const u =
    `leads?id=eq.${encodeURIComponent(leadId)}` +
    '&select=email,name,phone,first_seen_at,last_seen_at,source_page,landing_path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,meta,intent_score,intent_tier&limit=1';
  const r = await supabaseRequest(cfg, 'GET', u);
  if (r.code < 200 || r.code >= 300) return null;
  const rows = JSON.parse(r.body || '[]');
  return Array.isArray(rows) && rows[0] ? rows[0] : null;
}

async function patchLeadIntent(cfg, leadId, score, meta) {
  const tier = intentTierFromScore(score);
  const patch = JSON.stringify({
    intent_score: score,
    intent_tier: tier,
    meta,
    updated_at: new Date().toISOString(),
  });
  await supabaseRequest(cfg, 'PATCH', `leads?id=eq.${encodeURIComponent(leadId)}`, patch, 'return=minimal');
}

async function syncVisitorIntentFromLead(cfg, visitorId, score, tier) {
  if (!visitorId) return;
  await supabaseRequest(
    cfg,
    'PATCH',
    `visitors?id=eq.${encodeURIComponent(visitorId)}`,
    JSON.stringify({ intent_score: score, intent_tier: tier, updated_at: new Date().toISOString() }),
    'return=minimal'
  );
}

async function fetchVisitorForMerge(cfg, visitorId) {
  const u =
    `visitors?id=eq.${encodeURIComponent(visitorId)}` +
    '&select=email,name,phone,first_seen_at,last_seen_at,landing_path,source_page,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term&limit=1';
  const r = await supabaseRequest(cfg, 'GET', u);
  if (r.code < 200 || r.code >= 300) return null;
  const rows = JSON.parse(r.body || '[]');
  return Array.isArray(rows) && rows[0] ? rows[0] : null;
}

function isMissingVisitorsTable(body) {
  const b = String(body || '').toLowerCase();
  return b.includes('visitors') && (b.includes('does not exist') || b.includes('relation') || b.includes('schema cache'));
}

function deriveLifecycleStage({ hasContact = false, hasCheckoutContact = false, hasPaid = false } = {}) {
  if (hasPaid) return 'customer';
  if (hasCheckoutContact) return 'checkout_contact';
  if (hasContact) return 'lead';
  return 'visitor';
}

async function resolveOrCreateProfileId(cfg, seed = {}) {
  const profileId = await ensurePersonProfile(cfg, seed);
  if (!profileId) return null;
  await patchPersonProfile(cfg, profileId, {
    canonical_name: strTrim(seed.name, 500),
    canonical_email: normalizeEmail(seed.email),
    canonical_phone: normalizePhone(seed.phone),
    last_seen_at: seed.last_seen_at || new Date().toISOString(),
    last_touch_path: strTrim(seed.last_touch_path, 1000),
    last_touch_source: strTrim(seed.last_touch_source, 500),
    last_touch_referrer: strTrim(seed.last_touch_referrer, 2000),
    lifecycle_stage: deriveLifecycleStage(seed),
    lead_status: strTrim(seed.lead_status, 64) || (seed.hasContact ? 'new' : 'anonymous'),
  });
  return profileId;
}

async function backfillProfileLinks(cfg, profileId, ids = {}) {
  if (!profileId) return;
  const tasks = [];
  if (ids.visitorId) tasks.push(attachProfileToRecord(cfg, 'visitors', ids.visitorId, profileId));
  if (ids.leadId) tasks.push(attachProfileToRecord(cfg, 'leads', ids.leadId, profileId));
  if (ids.visitorId) {
    tasks.push(
      attachProfileByFilter(
        cfg,
        'visitor_events',
        `visitor_id=eq.${encodeURIComponent(String(ids.visitorId))}`,
        profileId
      )
    );
  }
  if (ids.leadId) {
    tasks.push(
      attachProfileByFilter(
        cfg,
        'lead_events',
        `lead_id=eq.${encodeURIComponent(String(ids.leadId))}`,
        profileId
      )
    );
    tasks.push(
      attachProfileByFilter(
        cfg,
        'abandoned_checkouts',
        `lead_id=eq.${encodeURIComponent(String(ids.leadId))}`,
        profileId
      )
    );
  }
  if (ids.sessionId) {
    tasks.push(
      attachProfileByFilter(
        cfg,
        'visitors',
        `session_id=eq.${encodeURIComponent(String(ids.sessionId))}`,
        profileId
      )
    );
    tasks.push(
      attachProfileByFilter(
        cfg,
        'leads',
        `session_id=eq.${encodeURIComponent(String(ids.sessionId))}`,
        profileId
      )
    );
  }
  await Promise.all(tasks);
  await refreshPersonProfileStats(cfg, profileId);
}

function funnelMeta(meta, fallback = {}) {
  const out = meta && typeof meta === 'object' ? { ...meta } : {};
  if (!out.funnel && fallback.funnel) out.funnel = fallback.funnel;
  if (!out.step_name && fallback.step_name) out.step_name = fallback.step_name;
  if (out.step_index == null && fallback.step_index != null) out.step_index = fallback.step_index;
  return out;
}

export async function handleTrackLead(cfg, body) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { status: 503, json: { ok: false, error: 'Tracking unavailable (Supabase not configured)' } };
  }
  const sessionId = String(body.session_id ?? '').trim();
  if (!sessionId) {
    return { status: 400, json: { ok: false, error: 'session_id required' } };
  }

  const now = new Date().toISOString();
  const emailNew = body.email != null ? String(body.email).trim().toLowerCase() : null;
  let phoneNew = body.phone != null ? String(body.phone).replace(/\s+/g, '') : null;
  phoneNew = phoneNew && phoneNew !== '' ? strTrim(phoneNew, 20) : null;
  const nameNew = strTrim(body.name, 500);
  const hasContact = Boolean((emailNew && emailNew !== '') || (phoneNew && phoneNew !== ''));

  const conversionForm = strTrim(body.conversion_form ?? body.form_id, 200);
  const conversionGoal = strTrim(body.goal, 200);

  const vfind = `visitors?select=id,converted_lead_id,person_profile_id&session_id=eq.${encodeURIComponent(sessionId)}&limit=1`;
  const vrf = await supabaseRequest(cfg, 'GET', vfind);
  if ((vrf.code < 200 || vrf.code >= 300) && isMissingVisitorsTable(vrf.body)) {
    return {
      status: 503,
      json: {
        ok: false,
        error:
          'Run Supabase migration server/supabase/migrations/001_visitors_split.sql (visitors tables missing)',
      },
    };
  }

  let visitorId = null;
  let visitorConvertedLeadId = null;
  let visitorProfileId = null;
  let isNewVisitor = false;
  if (vrf.code >= 200 && vrf.code < 300) {
    const rows = JSON.parse(vrf.body || '[]');
    if (rows[0]?.id) {
      visitorId = String(rows[0].id);
      visitorConvertedLeadId = rows[0].converted_lead_id ? String(rows[0].converted_lead_id) : null;
      visitorProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : null;
    }
  }

  const existingVisitor = visitorId ? await fetchVisitorForMerge(cfg, visitorId) : null;

  const currentLanding = strTrim(body.landing_path, 1000);
  const currentSource = strTrim(body.source_page, 500);

  const vpatch = {
    email: emailNew || (existingVisitor?.email != null ? String(existingVisitor.email).toLowerCase() : null),
    name: nameNew || (existingVisitor ? strTrim(existingVisitor.name, 500) : null),
    phone:
      phoneNew ||
      (existingVisitor?.phone != null
        ? strTrim(String(existingVisitor.phone).replace(/\s+/g, ''), 20)
        : null),
    source_page: visitorId
      ? preserveFirst(existingVisitor?.source_page, body.source_page, 500)
      : currentSource,
    landing_path: visitorId
      ? preserveFirst(existingVisitor?.landing_path, body.landing_path, 1000)
      : currentLanding,
    referrer: firstTouch(body.referrer, existingVisitor?.referrer, 2000),
    document_referrer: firstTouch(body.document_referrer, existingVisitor?.document_referrer, 2000),
    utm_source: firstTouch(body.utm_source, existingVisitor?.utm_source, 128),
    utm_medium: firstTouch(body.utm_medium, existingVisitor?.utm_medium, 128),
    utm_campaign: firstTouch(body.utm_campaign, existingVisitor?.utm_campaign, 256),
    utm_content: firstTouch(body.utm_content, existingVisitor?.utm_content, 256),
    utm_term: firstTouch(body.utm_term, existingVisitor?.utm_term, 256),
    user_agent: strTrim(body.user_agent, 500),
    client_language: strTrim(body.client_language, 64),
    screen_width:
      body.screen_width != null && !Number.isNaN(Number(body.screen_width))
        ? parseInt(String(body.screen_width), 10)
        : null,
    screen_height:
      body.screen_height != null && !Number.isNaN(Number(body.screen_height))
        ? parseInt(String(body.screen_height), 10)
        : null,
    last_seen_at: now,
  };

  const profileIdSeed =
    visitorProfileId || hasContact
      ? await resolveOrCreateProfileId(cfg, {
          name: nameNew || existingVisitor?.name,
          email: emailNew || existingVisitor?.email,
          phone: phoneNew || existingVisitor?.phone,
          first_seen_at: existingVisitor?.first_seen_at || now,
          last_seen_at: now,
          first_touch_path: currentLanding,
          first_touch_source: currentSource,
          first_touch_referrer: body.referrer,
          last_touch_path: currentLanding,
          last_touch_source: currentSource,
          last_touch_referrer: body.referrer,
          hasContact,
        })
      : null;
  if (profileIdSeed) vpatch.person_profile_id = profileIdSeed;

  if (visitorId) {
    const patchJson = JSON.stringify(vpatch);
    const pu = await supabaseRequest(cfg, 'PATCH', `visitors?id=eq.${encodeURIComponent(visitorId)}`, patchJson, 'return=minimal');
    if (pu.code < 200 || pu.code >= 300) {
      if (isMissingVisitorsTable(pu.body)) {
        return {
          status: 503,
          json: { ok: false, error: 'Apply visitors migration in Supabase (see server/supabase/migrations/)' },
        };
      }
      return { status: 502, json: { ok: false, error: (pu.body || '').slice(0, 300) } };
    }
  } else {
    const insert = {
      ...vpatch,
      session_id: sessionId,
      first_seen_at: now,
    };
    const ins = await supabaseRequest(cfg, 'POST', 'visitors', JSON.stringify(insert), 'return=minimal');
    if (ins.code < 200 || ins.code >= 300) {
      if (isMissingVisitorsTable(ins.body)) {
        return {
          status: 503,
          json: { ok: false, error: 'Apply visitors migration in Supabase (see server/supabase/migrations/)' },
        };
      }
      return { status: 502, json: { ok: false, error: (ins.body || '').slice(0, 300) } };
    }
    isNewVisitor = true;
    const vrf2 = await supabaseRequest(cfg, 'GET', vfind);
    if (vrf2.code >= 200 && vrf2.code < 300) {
      const rows = JSON.parse(vrf2.body || '[]');
      if (rows[0]?.id) {
        visitorId = String(rows[0].id);
        visitorConvertedLeadId = rows[0].converted_lead_id ? String(rows[0].converted_lead_id) : null;
        visitorProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : profileIdSeed;
      }
    }
  }

  const findLead = `leads?select=id,visitor_id,person_profile_id&session_id=eq.${encodeURIComponent(sessionId)}&limit=1`;
  const lfr = await supabaseRequest(cfg, 'GET', findLead);
  let leadId = null;
  let leadVisitorId = null;
  let leadProfileId = null;
  if (lfr.code >= 200 && lfr.code < 300) {
    const rows = JSON.parse(lfr.body || '[]');
    if (rows[0]?.id) {
      leadId = String(rows[0].id);
      leadVisitorId = rows[0].visitor_id ? String(rows[0].visitor_id) : null;
      leadProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : null;
    }
  }

  let existingLead = leadId ? await fetchLeadForMerge(cfg, leadId) : null;

  const hadContactBefore = Boolean(
    existingLead &&
      ((existingLead.email && String(existingLead.email).trim()) ||
        (existingLead.phone && String(existingLead.phone).replace(/\s+/g, '').trim()))
  );

  if (leadId && visitorId && !leadVisitorId) {
    await supabaseRequest(
      cfg,
      'PATCH',
      `leads?id=eq.${encodeURIComponent(leadId)}`,
      JSON.stringify({ visitor_id: visitorId, person_profile_id: leadProfileId || visitorProfileId || null }),
      'return=minimal'
    );
  }

  if (!leadId && visitorId) {
    const anonInsert = {
      session_id: sessionId,
      visitor_id: visitorId,
      person_profile_id: visitorProfileId,
      source_page: strTrim(body.source_page, 500),
      landing_path: strTrim(body.landing_path, 1000),
      referrer: firstTouch(body.referrer, null, 2000),
      document_referrer: firstTouch(body.document_referrer, null, 2000),
      utm_source: firstTouch(body.utm_source, null, 128),
      utm_medium: firstTouch(body.utm_medium, null, 128),
      utm_campaign: firstTouch(body.utm_campaign, null, 256),
      utm_content: firstTouch(body.utm_content, null, 256),
      utm_term: firstTouch(body.utm_term, null, 256),
      user_agent: strTrim(body.user_agent, 500),
      client_language: strTrim(body.client_language, 64),
      screen_width:
        body.screen_width != null && !Number.isNaN(Number(body.screen_width))
          ? parseInt(String(body.screen_width), 10)
          : null,
      screen_height:
        body.screen_height != null && !Number.isNaN(Number(body.screen_height))
          ? parseInt(String(body.screen_height), 10)
          : null,
      first_seen_at: now,
      last_seen_at: now,
      intent_score: 0,
      intent_tier: 'low',
      meta: {},
    };
    const insA = await supabaseRequest(cfg, 'POST', 'leads', JSON.stringify(anonInsert), 'return=minimal');
    if (insA.code < 200 || insA.code >= 300) {
      return { status: 502, json: { ok: false, error: (insA.body || '').slice(0, 300) } };
    }
    const frA = await supabaseRequest(cfg, 'GET', findLead);
    if (frA.code >= 200 && frA.code < 300) {
      const rows = JSON.parse(frA.body || '[]');
      if (rows[0]?.id) {
        leadId = String(rows[0].id);
        leadVisitorId = rows[0].visitor_id ? String(rows[0].visitor_id) : null;
        leadProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : visitorProfileId;
      }
    }
    existingLead = leadId ? await fetchLeadForMerge(cfg, leadId) : null;
  }

  if (leadId && isNewVisitor) {
    const row = await fetchLeadForMerge(cfg, leadId);
    if (row) {
      const applied = applyIntentFirstVisit(Number(row.intent_score) || 0, row.meta);
      if (applied.changed) {
        await patchLeadIntent(cfg, leadId, applied.score, applied.meta);
        await syncVisitorIntentFromLead(cfg, visitorId, applied.score, intentTierFromScore(applied.score));
      }
    }
  }

  const shouldMaintainProfile = hasContact || Boolean(leadProfileId || visitorProfileId);
  const personProfileId = shouldMaintainProfile
    ? await resolveOrCreateProfileId(cfg, {
        name: nameNew || existingLead?.name || existingVisitor?.name,
        email: emailNew || existingLead?.email || existingVisitor?.email,
        phone: phoneNew || existingLead?.phone || existingVisitor?.phone,
        first_seen_at: existingLead?.first_seen_at || existingVisitor?.first_seen_at || now,
        last_seen_at: now,
        first_touch_path: existingVisitor?.landing_path || currentLanding,
        first_touch_source: existingVisitor?.source_page || currentSource,
        first_touch_referrer: existingVisitor?.referrer || body.referrer,
        last_touch_path: currentLanding,
        last_touch_source: currentSource,
        last_touch_referrer: body.referrer,
        hasContact,
        lead_status: hasContact ? 'new' : 'anonymous',
      })
    : null;
  if (personProfileId) {
    visitorProfileId = personProfileId;
    leadProfileId = personProfileId;
    if (visitorId) {
      await supabaseRequest(
        cfg,
        'PATCH',
        `visitors?id=eq.${encodeURIComponent(visitorId)}`,
        JSON.stringify({ person_profile_id: personProfileId, updated_at: now }),
        'return=minimal'
      );
    }
  }

  let leadPatch = null;
  if (hasContact) {
    leadPatch = {
      email: emailNew || existingLead?.email || null,
      name: nameNew || (existingLead ? strTrim(existingLead.name, 500) : null),
      phone:
        phoneNew ||
        (existingLead?.phone != null
          ? strTrim(String(existingLead.phone).replace(/\s+/g, ''), 20)
          : null),
      source_page: strTrim(body.source_page, 500),
      landing_path: strTrim(body.landing_path, 1000),
      referrer: firstTouch(body.referrer, existingLead?.referrer, 2000),
      document_referrer: firstTouch(body.document_referrer, existingLead?.document_referrer, 2000),
      utm_source: firstTouch(body.utm_source, existingLead?.utm_source, 128),
      utm_medium: firstTouch(body.utm_medium, existingLead?.utm_medium, 128),
      utm_campaign: firstTouch(body.utm_campaign, existingLead?.utm_campaign, 256),
      utm_content: firstTouch(body.utm_content, existingLead?.utm_content, 256),
      utm_term: firstTouch(body.utm_term, existingLead?.utm_term, 256),
      user_agent: strTrim(body.user_agent, 500),
      client_language: strTrim(body.client_language, 64),
      screen_width:
        body.screen_width != null && !Number.isNaN(Number(body.screen_width))
          ? parseInt(String(body.screen_width), 10)
          : null,
      screen_height:
        body.screen_height != null && !Number.isNaN(Number(body.screen_height))
          ? parseInt(String(body.screen_height), 10)
          : null,
      last_seen_at: now,
      visitor_id: visitorId,
      person_profile_id: leadProfileId || visitorProfileId || null,
    };

    if (leadId) {
      await supabaseRequest(cfg, 'PATCH', `leads?id=eq.${encodeURIComponent(leadId)}`, JSON.stringify(leadPatch), 'return=minimal');
    } else {
      const insert = { ...leadPatch, session_id: sessionId, first_seen_at: now };
      const ins = await supabaseRequest(cfg, 'POST', 'leads', JSON.stringify(insert), 'return=minimal');
      if (ins.code < 200 || ins.code >= 300) {
        return { status: 502, json: { ok: false, error: (ins.body || '').slice(0, 300) } };
      }
      const fr2 = await supabaseRequest(cfg, 'GET', findLead);
      if (fr2.code >= 200 && fr2.code < 300) {
        const rows = JSON.parse(fr2.body || '[]');
        if (rows[0]?.id) {
          leadId = String(rows[0].id);
          leadProfileId = rows[0].person_profile_id
            ? String(rows[0].person_profile_id)
            : leadPatch.person_profile_id || visitorProfileId;
        }
      }
    }

    if (visitorId && leadId && !visitorConvertedLeadId) {
      const convSrc = {
        page: leadPatch.source_page || leadPatch.landing_path,
        form: conversionForm,
        goal: conversionGoal,
      };
      await supabaseRequest(
        cfg,
        'PATCH',
        `visitors?id=eq.${encodeURIComponent(visitorId)}`,
        JSON.stringify({
          converted_lead_id: leadId,
          conversion_at: now,
          conversion_source: convSrc,
        }),
        'return=minimal'
      );
      visitorConvertedLeadId = leadId;
    }
  } else if (leadId) {
    const touchPatch = {
      source_page: strTrim(body.source_page, 500),
      landing_path: strTrim(body.landing_path, 1000),
      referrer: firstTouch(body.referrer, existingLead?.referrer, 2000),
      document_referrer: firstTouch(body.document_referrer, existingLead?.document_referrer, 2000),
      utm_source: firstTouch(body.utm_source, existingLead?.utm_source, 128),
      utm_medium: firstTouch(body.utm_medium, existingLead?.utm_medium, 128),
      utm_campaign: firstTouch(body.utm_campaign, existingLead?.utm_campaign, 256),
      utm_content: firstTouch(body.utm_content, existingLead?.utm_content, 256),
      utm_term: firstTouch(body.utm_term, existingLead?.utm_term, 256),
      user_agent: strTrim(body.user_agent, 500),
      client_language: strTrim(body.client_language, 64),
      screen_width:
        body.screen_width != null && !Number.isNaN(Number(body.screen_width))
          ? parseInt(String(body.screen_width), 10)
          : null,
      screen_height:
        body.screen_height != null && !Number.isNaN(Number(body.screen_height))
          ? parseInt(String(body.screen_height), 10)
          : null,
      last_seen_at: now,
      visitor_id: visitorId,
    };
    await supabaseRequest(cfg, 'PATCH', `leads?id=eq.${encodeURIComponent(leadId)}`, JSON.stringify(touchPatch), 'return=minimal');
  }

  if (hasContact && leadId && !hadContactBefore) {
    const row = await fetchLeadForMerge(cfg, leadId);
    if (row) {
      const applied = applyIntentContact(Number(row.intent_score) || 0, row.meta);
      if (applied.changed) {
        await patchLeadIntent(cfg, leadId, applied.score, applied.meta);
        await syncVisitorIntentFromLead(cfg, visitorId, applied.score, intentTierFromScore(applied.score));
      }
    }
  }

  if (visitorId) {
    const veMeta = {
      has_contact: hasContact,
      conversion_form: conversionForm,
      goal: conversionGoal,
      email: emailNew,
      name: nameNew,
      phone: phoneNew,
    };
    const ve = await supabaseRequest(
      cfg,
      'POST',
      'visitor_events',
      JSON.stringify({
        visitor_id: visitorId,
        person_profile_id: personProfileId,
        session_id: sessionId,
        event_type: 'journey',
        event_name: hasContact ? 'contact_shared' : 'session_touch',
        path: currentLanding,
        referrer: vpatch.referrer,
        document_referrer: vpatch.document_referrer,
        utm_source: vpatch.utm_source,
        utm_medium: vpatch.utm_medium,
        utm_campaign: vpatch.utm_campaign,
        meta: veMeta,
        created_at: now,
      }),
      'return=minimal'
    );
    if (ve.code < 200 || ve.code >= 300) {
      if (isMissingVisitorsTable(ve.body)) {
        return {
          status: 503,
          json: { ok: false, error: 'visitor_events table missing — run Supabase migration' },
        };
      }
      return { status: 502, json: { ok: false, error: (ve.body || '').slice(0, 300) } };
    }
  }

  if (leadId && hasContact) {
    const evMeta = {
      email: leadPatch?.email,
      name: leadPatch?.name,
      source_page: leadPatch?.source_page,
      landing_path: leadPatch?.landing_path,
      referrer: leadPatch?.referrer,
      utm_source: leadPatch?.utm_source,
      utm_medium: leadPatch?.utm_medium,
      utm_campaign: leadPatch?.utm_campaign,
      user_agent: leadPatch?.user_agent,
      conversion_form: conversionForm,
    };
    await supabaseRequest(
      cfg,
      'POST',
      'lead_events',
      JSON.stringify({
        lead_id: leadId,
        person_profile_id: personProfileId,
        session_id: sessionId,
        event_type: 'lead',
        event_name: 'lead_upsert',
        path: leadPatch?.landing_path,
        referrer: leadPatch?.referrer,
        document_referrer: leadPatch?.document_referrer,
        utm_source: leadPatch?.utm_source,
        utm_medium: leadPatch?.utm_medium,
        utm_campaign: leadPatch?.utm_campaign,
        utm_content: leadPatch?.utm_content,
        utm_term: leadPatch?.utm_term,
        meta: evMeta,
        created_at: now,
      }),
      'return=minimal'
    );
  }

  let outIntentScore = null;
  let outIntentTier = null;
  if (leadId) {
    const lr = await fetchLeadForMerge(cfg, leadId);
    if (lr) {
      outIntentScore = Number(lr.intent_score) || 0;
      outIntentTier = lr.intent_tier || intentTierFromScore(outIntentScore);
    }
  }

  if (personProfileId) {
    await backfillProfileLinks(cfg, personProfileId, { visitorId, leadId, sessionId });
  }

  return {
    status: 200,
    json: {
      ok: true,
      leadId: leadId || null,
      visitorId: visitorId || null,
      personProfileId: personProfileId || null,
      convertedToLead: Boolean(hasContact && leadId),
      intentScore: outIntentScore,
      intentTier: outIntentTier,
    },
  };
}

export async function handleTrackLeadEvent(cfg, body) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { status: 503, json: { ok: false, error: 'Tracking unavailable (Supabase not configured)' } };
  }
  const sessionId = String(body.session_id ?? '').trim();
  if (!sessionId) {
    return { status: 400, json: { ok: false, error: 'session_id required' } };
  }

  const vfind = `visitors?select=id,person_profile_id&session_id=eq.${encodeURIComponent(sessionId)}&limit=1`;
  const vrf = await supabaseRequest(cfg, 'GET', vfind);
  let visitorId = null;
  let personProfileId = null;
  if (vrf.code >= 200 && vrf.code < 300) {
    const rows = JSON.parse(vrf.body || '[]');
    if (rows[0]?.id) {
      visitorId = String(rows[0].id);
      personProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : null;
    }
  }

  const find = `leads?select=id,person_profile_id&session_id=eq.${encodeURIComponent(sessionId)}&limit=1`;
  const fr = await supabaseRequest(cfg, 'GET', find);
  let leadId = null;
  if (fr.code >= 200 && fr.code < 300) {
    const rows = JSON.parse(fr.body || '[]');
    if (rows[0]?.id) {
      leadId = String(rows[0].id);
      if (!personProfileId && rows[0].person_profile_id) personProfileId = String(rows[0].person_profile_id);
    }
  }

  const meta = funnelMeta(body.meta, {
    funnel: body.funnel,
    step_name: body.step_name || body.stage,
    step_index: body.step_index,
  });
  const occurredAt = new Date().toISOString();

  const row = {
    lead_id: leadId,
    person_profile_id: personProfileId,
    session_id: sessionId,
    event_type: strTrim(body.event_type, 64) || 'activity',
    event_name: strTrim(body.event_name, 128) || 'event',
    stage: strTrim(body.stage, 64),
    path: strTrim(body.path, 1000),
    referrer: strTrim(body.referrer, 2000),
    document_referrer: strTrim(body.document_referrer, 2000),
    utm_source: strTrim(body.utm_source, 128),
    utm_medium: strTrim(body.utm_medium, 128),
    utm_campaign: strTrim(body.utm_campaign, 256),
    utm_content: strTrim(body.utm_content, 256),
    utm_term: strTrim(body.utm_term, 256),
    meta,
    created_at: occurredAt,
  };

  // Visitor-only sessions: store here. If a lead exists, only lead_events (avoids duplicate rows in admin timelines).
  if (visitorId && !leadId) {
    const veRow = {
      visitor_id: visitorId,
      person_profile_id: personProfileId,
      session_id: sessionId,
      event_type: row.event_type,
      event_name: row.event_name,
      path: row.path,
      referrer: row.referrer,
      document_referrer: row.document_referrer,
      utm_source: row.utm_source,
      utm_medium: row.utm_medium,
      utm_campaign: row.utm_campaign,
      meta,
      created_at: occurredAt,
    };
    const ve = await supabaseRequest(cfg, 'POST', 'visitor_events', JSON.stringify(veRow), 'return=minimal');
    if (ve.code < 200 || ve.code >= 300) {
      if (isMissingVisitorsTable(ve.body)) {
        return { status: 503, json: { ok: false, error: 'Run visitors migration in Supabase' } };
      }
      return { status: 502, json: { ok: false, error: (ve.body || '').slice(0, 300) } };
    }
  }

  if (leadId) {
    const ins = await supabaseRequest(cfg, 'POST', 'lead_events', JSON.stringify(row), 'return=minimal');
    if (ins.code < 200 || ins.code >= 300) {
      return { status: 502, json: { ok: false, error: (ins.body || '').slice(0, 300) } };
    }
    const en = String(body.event_name || '').toLowerCase();
    if (row.path && (en === 'page_view' || en === 'step_view')) {
      const lr = await fetchLeadForMerge(cfg, leadId);
      if (lr) {
        const applied = applyIntentNewPage(Number(lr.intent_score) || 0, lr.meta, row.path);
        if (applied.changed) {
          await patchLeadIntent(cfg, leadId, applied.score, applied.meta);
          await syncVisitorIntentFromLead(cfg, visitorId, applied.score, intentTierFromScore(applied.score));
        }
      }
    }
  }

  if (personProfileId) {
    await backfillProfileLinks(cfg, personProfileId, { visitorId, leadId, sessionId });
  }

  let intentScore = null;
  let intentTier = null;
  if (leadId) {
    const lr = await fetchLeadForMerge(cfg, leadId);
    if (lr) {
      intentScore = Number(lr.intent_score) || 0;
      intentTier = lr.intent_tier || intentTierFromScore(intentScore);
    }
  }

  return { status: 200, json: { ok: true, personProfileId, intentScore, intentTier } };
}

export async function handleTrackCheckoutEvent(cfg, body) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { status: 503, json: { ok: false, error: 'Tracking unavailable (Supabase not configured)' } };
  }
  const checkoutSessionId = String(body.checkout_session_id ?? '').trim();
  if (!checkoutSessionId) {
    return { status: 400, json: { ok: false, error: 'checkout_session_id required' } };
  }

  const now = new Date().toISOString();
  const stage = strTrim(body.stage, 64) || 'page_view';

  const find = `abandoned_checkouts?select=id,person_profile_id&checkout_session_id=eq.${encodeURIComponent(checkoutSessionId)}&limit=1`;
  const fr = await supabaseRequest(cfg, 'GET', find);
  let rowId = null;
  let personProfileId = null;
  if (fr.code >= 200 && fr.code < 300) {
    const rows = JSON.parse(fr.body || '[]');
    if (rows[0]?.id) {
      rowId = String(rows[0].id);
      personProfileId = rows[0].person_profile_id ? String(rows[0].person_profile_id) : null;
    }
  }

  const rawLead = String(body.lead_id ?? '').trim();
  const leadUuid = rawLead && uuidOk(rawLead) ? rawLead : null;

  const rawVs = String(body.visitor_session_id ?? body.session_id ?? '').trim();
  let leadSessionForEvent = rawVs || null;
  let visitorIdForCheckout = null;
  if (rawVs) {
    const vf = await supabaseRequest(
      cfg,
      'GET',
      `visitors?select=id,person_profile_id&session_id=eq.${encodeURIComponent(rawVs)}&limit=1`
    );
    if (vf.code >= 200 && vf.code < 300) {
      const rows = JSON.parse(vf.body || '[]');
      if (rows[0]?.id) {
        visitorIdForCheckout = String(rows[0].id);
        if (!personProfileId && rows[0].person_profile_id) personProfileId = String(rows[0].person_profile_id);
      }
    }
  }
  if (!leadSessionForEvent && leadUuid) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `leads?id=eq.${encodeURIComponent(leadUuid)}&select=session_id,person_profile_id&limit=1`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const rows = JSON.parse(lr.body || '[]');
      if (rows[0]?.session_id) leadSessionForEvent = String(rows[0].session_id);
      if (!personProfileId && rows[0]?.person_profile_id) personProfileId = String(rows[0].person_profile_id);
    }
  }

  const patch = {
    email: body.email != null ? String(body.email).trim().toLowerCase() : null,
    name: strTrim(body.name, 500),
    phone: body.phone != null ? strTrim(String(body.phone).replace(/\s+/g, ''), 20) : null,
    product_slug: strTrim(body.product_slug, 128) || 'premium_kundli_report',
    stage,
    razorpay_order_id: body.razorpay_order_id != null ? String(body.razorpay_order_id).trim() : null,
    amount_paise:
      body.amount_paise != null && !Number.isNaN(Number(body.amount_paise))
        ? Math.round(Number(body.amount_paise))
        : null,
    currency: strTrim(body.currency, 8) || 'INR',
    utm_source: strTrim(body.utm_source, 128),
    utm_medium: strTrim(body.utm_medium, 128),
    utm_campaign: strTrim(body.utm_campaign, 256),
    utm_content: strTrim(body.utm_content, 256),
    utm_term: strTrim(body.utm_term, 256),
    referrer: strTrim(body.referrer, 2000),
    landing_path: strTrim(body.landing_path, 1000),
    last_event_at: now,
  };
  if (leadUuid) patch.lead_id = leadUuid;

  const hasCheckoutContact = Boolean(patch.email || patch.phone);
  if (!personProfileId && hasCheckoutContact) {
    personProfileId = await resolveOrCreateProfileId(cfg, {
      name: patch.name,
      email: patch.email,
      phone: patch.phone,
      first_seen_at: now,
      last_seen_at: now,
      first_touch_path: patch.landing_path,
      first_touch_source: body.source_page || 'checkout',
      first_touch_referrer: patch.referrer,
      last_touch_path: patch.landing_path,
      last_touch_source: body.source_page || 'checkout',
      last_touch_referrer: patch.referrer,
      hasContact: hasCheckoutContact,
      hasCheckoutContact: true,
      lead_status: 'checkout_started',
    });
  }
  if (personProfileId) patch.person_profile_id = personProfileId;

  if (stage === 'dismissed' || stage === 'payment_dismissed') {
    patch.abandoned_at = now;
  }

  if (rowId) {
    await supabaseRequest(
      cfg,
      'PATCH',
      `abandoned_checkouts?id=eq.${encodeURIComponent(rowId)}`,
      JSON.stringify(patch),
      'return=minimal'
    );
  }

  if (!rowId && hasCheckoutContact) {
    const insert = { ...patch, checkout_session_id: checkoutSessionId };
    const ins = await supabaseRequest(cfg, 'POST', 'abandoned_checkouts', JSON.stringify(insert), 'return=minimal');
    if (ins.code < 200 || ins.code >= 300) {
      return { status: 502, json: { ok: false, error: (ins.body || '').slice(0, 300) } };
    }
    const fr2 = await supabaseRequest(cfg, 'GET', find);
    if (fr2.code >= 200 && fr2.code < 300) {
      const rows = JSON.parse(fr2.body || '[]');
      if (rows[0]?.id) rowId = String(rows[0].id);
    }
  }

  if (visitorIdForCheckout) {
    await supabaseRequest(
      cfg,
      'POST',
      'visitor_events',
      JSON.stringify({
        visitor_id: visitorIdForCheckout,
        person_profile_id: personProfileId,
        session_id: rawVs,
        event_type: 'checkout',
        event_name: stage,
        path: patch.landing_path,
        referrer: patch.referrer,
        meta: {
          checkout_session_id: checkoutSessionId,
          lead_id: leadUuid,
          product_slug: patch.product_slug,
          razorpay_order_id: patch.razorpay_order_id,
        },
        created_at: now,
      }),
      'return=minimal'
    );
  }

  const evMeta = {
    email: patch.email,
    name: patch.name,
    product_slug: patch.product_slug,
    razorpay_order_id: patch.razorpay_order_id,
    amount_paise: patch.amount_paise,
    currency: patch.currency,
    checkout_session_id: checkoutSessionId,
  };
  if (leadUuid && leadSessionForEvent) {
    await supabaseRequest(
      cfg,
      'POST',
      'lead_events',
      JSON.stringify({
        lead_id: leadUuid,
        person_profile_id: personProfileId,
        session_id: leadSessionForEvent,
        event_type: 'checkout',
        event_name: stage,
        stage,
        path: patch.landing_path,
        referrer: patch.referrer,
        document_referrer: null,
        utm_source: patch.utm_source,
        utm_medium: patch.utm_medium,
        utm_campaign: patch.utm_campaign,
        utm_content: patch.utm_content,
        utm_term: patch.utm_term,
        meta: evMeta,
        created_at: now,
      }),
      'return=minimal'
    );
  }

  if (personProfileId) {
    await backfillProfileLinks(cfg, personProfileId, {
      visitorId: visitorIdForCheckout,
      leadId: leadUuid,
      sessionId: rawVs || leadSessionForEvent,
    });
    if (rowId) await attachProfileToRecord(cfg, 'abandoned_checkouts', rowId, personProfileId);
  }

  return { status: 200, json: { ok: true, id: rowId, personProfileId } };
}
