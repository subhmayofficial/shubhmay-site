import { DateTime } from 'luxon';
import { supabaseRequest, supabaseGetRange, supabaseCountRows, parseContentRangeTotal } from './supabase.js';
import { strTrim } from './strings.js';

/**
 * First URL path in the same merged timeline as /api/admin/visitors/:id/timeline
 * (visitor_events + lead_events when converted), earliest event with a non-empty path.
 */
async function firstSitePathFromVisitorTimeline(cfg, row) {
  if (row?.person_profile_id) {
    const evs = await profileEventsTimeline(cfg, row.person_profile_id);
    const touches = timelineFirstAndLastTouch(evs);
    return touches.firstTouchPath || null;
  }
  const visitorId = row?.id;
  if (!visitorId) return null;
  const leadId = row.converted_lead_id ? String(row.converted_lead_id) : null;
  const veUrl =
    `visitor_events?visitor_id=eq.${encodeURIComponent(String(visitorId))}` +
    '&select=id,event_type,event_name,path,created_at&order=created_at.asc&limit=500';
  const reqs = [supabaseRequest(cfg, 'GET', veUrl)];
  if (leadId) {
    reqs.push(
      supabaseRequest(
        cfg,
        'GET',
        `lead_events?lead_id=eq.${encodeURIComponent(leadId)}&select=id,event_type,event_name,stage,path,created_at&order=created_at.asc&limit=500`
      )
    );
  }
  const results = await Promise.all(reqs);
  const merged = [];
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r.code >= 200 && r.code < 300) {
      const evs = JSON.parse(r.body || '[]');
      if (Array.isArray(evs)) {
        const source = i === 0 ? 'visitor' : 'lead';
        merged.push(...evs.map((ev) => ({ ...ev, _source: source })));
      }
    }
  }
  const deduped = dedupeMergedTimelineEvents(merged);
  for (const ev of deduped) {
    const p = strTrim(ev.path, 1000);
    if (p) return p;
  }
  return null;
}

const ADMIN_TZ = 'Asia/Kolkata';

export const ADMIN_ORDERS_SELECT =
  'id,customer_id,lead_id,person_profile_id,abandoned_checkout_id,product_slug,razorpay_order_id,razorpay_payment_id,receipt,amount_paise,currency,payment_status,order_status,dob,tob,birth_place,language,coupon,paid_at,created_at,updated_at,razorpay_notes,customers(name,email,phone,is_paying_customer,first_paid_at,total_spent_paise,created_at),leads!orders_lead_id_fkey(session_id,utm_source,utm_medium,utm_campaign,landing_path,referrer,source_page,lead_status,first_seen_at,last_seen_at),abandoned_checkouts!orders_abandoned_checkout_id_fkey(checkout_session_id,stage,abandoned_at,last_event_at,utm_source,utm_medium,utm_campaign),person_profiles(id,canonical_name,canonical_email,canonical_phone,first_touch_path,last_touch_path,lifecycle_stage,total_orders,total_revenue_paise)';

// visitors!leads_visitor_id_fkey — disambiguate from visitors.converted_lead_id → leads (PGRST201)
export const ADMIN_LEADS_SELECT =
  'id,session_id,email,name,phone,visitor_id,person_profile_id,source_page,landing_path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,user_agent,client_language,screen_width,screen_height,lead_status,converted_order_id,first_seen_at,last_seen_at,intent_score,intent_tier,meta,created_at,updated_at,visitors!leads_visitor_id_fkey(id,session_id,converted_lead_id,conversion_at,conversion_source),lead_events(id,session_id,event_type,event_name,stage,path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,meta,created_at),orders!orders_lead_id_fkey(id,product_slug,amount_paise,currency,payment_status,order_status,paid_at,created_at,razorpay_order_id,razorpay_payment_id,receipt,coupon),abandoned_checkouts!abandoned_checkouts_lead_id_fkey(id,checkout_session_id,stage,product_slug,amount_paise,currency,last_event_at,abandoned_at,converted_order_id,converted_at,razorpay_order_id,referrer,landing_path),consultancy_bookings!consultancy_bookings_lead_id_fkey(id,plan_code,plan_name,duration_minutes,amount_paise,currency,status,payment_status,slot_start,slot_end,razorpay_order_id,created_at),person_profiles(id,canonical_name,canonical_email,canonical_phone,first_touch_path,last_touch_path,lead_status,lifecycle_stage,total_orders,total_revenue_paise)';

export const ADMIN_VISITORS_SELECT =
  'id,session_id,email,name,phone,person_profile_id,source_page,landing_path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,user_agent,client_language,screen_width,screen_height,first_seen_at,last_seen_at,converted_lead_id,conversion_at,conversion_source,intent_score,intent_tier,meta,created_at,updated_at,person_profiles(id,canonical_name,canonical_email,canonical_phone,first_touch_path,last_touch_path,lead_status,lifecycle_stage,total_orders,total_revenue_paise)';

export const ADMIN_PROFILES_SELECT =
  'id,canonical_name,canonical_email,canonical_phone,first_seen_at,last_seen_at,first_touch_path,first_touch_source,first_touch_referrer,last_touch_path,last_touch_source,last_touch_referrer,lead_status,lifecycle_stage,total_orders,total_revenue_paise,merged_session_ids,merged_visitor_ids,merged_lead_ids,merged_checkout_ids,merged_order_ids,merged_booking_ids,meta,created_at,updated_at';

const ADMIN_LEADS_ROLLUP_SELECT =
  'id,session_id,email,name,phone,person_profile_id,source_page,landing_path,referrer,utm_source,utm_medium,utm_campaign,lead_status,converted_order_id,first_seen_at,last_seen_at,intent_score,intent_tier';

const ADMIN_VISITORS_ROLLUP_SELECT =
  'id,session_id,email,name,phone,person_profile_id,landing_path,source_page,referrer,utm_source,utm_medium,utm_campaign,first_seen_at,last_seen_at,converted_lead_id,conversion_at,intent_score,intent_tier';

const ADMIN_ABANDONED_ROLLUP_SELECT =
  'id,checkout_session_id,person_profile_id,lead_id,email,name,phone,product_slug,stage,utm_source,utm_medium,utm_campaign,last_event_at,abandoned_at,converted_order_id';

function queryFlagOn(q, key) {
  const v = String(q[key] ?? '')
    .trim()
    .toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

function groupLeadsByPerson(rows) {
  const map = new Map();
  for (const row of rows) {
    const sid = String(row.session_id || '').trim();
    const key = row.person_profile_id ? `p:${row.person_profile_id}` : sid ? `s:${sid}` : `l:${row.id}`;
    let g = map.get(key);
    if (!g) {
      g = { leads: [], lastMs: -1, primary: null };
      map.set(key, g);
    }
    g.leads.push(row);
    const t = new Date(row.last_seen_at || 0).getTime();
    if (t >= g.lastMs) {
      g.lastMs = t;
      g.primary = row;
    }
  }
  return [...map.values()]
    .map((g) => ({
      rollup: true,
      person_profile_id: g.primary?.person_profile_id || null,
      lead_count: g.leads.length,
      last_seen_at: g.primary?.last_seen_at || null,
      primary_lead: g.primary,
      leads: g.leads.slice().sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0)),
    }))
    .sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0));
}

function groupVisitorsByPerson(rows) {
  const map = new Map();
  for (const row of rows) {
    const sid = String(row.session_id || '').trim();
    const key = row.person_profile_id ? `p:${row.person_profile_id}` : sid ? `s:${sid}` : `v:${row.id}`;
    let g = map.get(key);
    if (!g) {
      g = { visitors: [], lastMs: -1, primary: null };
      map.set(key, g);
    }
    g.visitors.push(row);
    const t = new Date(row.last_seen_at || 0).getTime();
    if (t >= g.lastMs) {
      g.lastMs = t;
      g.primary = row;
    }
  }
  return [...map.values()]
    .map((g) => ({
      rollup: true,
      person_profile_id: g.primary?.person_profile_id || null,
      visitor_count: g.visitors.length,
      last_seen_at: g.primary?.last_seen_at || null,
      primary_visitor: g.primary,
      visitors: g.visitors.slice().sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0)),
    }))
    .sort((a, b) => new Date(b.last_seen_at || 0) - new Date(a.last_seen_at || 0));
}

function groupAbandonedByPerson(rows) {
  const map = new Map();
  for (const row of rows) {
    const cs = String(row.checkout_session_id || '').trim();
    const sid = String(row.session_id || '').trim();
    const key = row.person_profile_id ? `p:${row.person_profile_id}` : cs ? `c:${cs}` : sid ? `s:${sid}` : `a:${row.id}`;
    let g = map.get(key);
    if (!g) {
      g = { rows: [], lastMs: -1, primary: null };
      map.set(key, g);
    }
    g.rows.push(row);
    const t = new Date(row.last_event_at || 0).getTime();
    if (t >= g.lastMs) {
      g.lastMs = t;
      g.primary = row;
    }
  }
  return [...map.values()]
    .map((g) => ({
      rollup: true,
      person_profile_id: g.primary?.person_profile_id || null,
      checkout_count: g.rows.length,
      last_event_at: g.primary?.last_event_at || null,
      primary_checkout: g.primary,
      abandoned_rows: g.rows.slice().sort((a, b) => new Date(b.last_event_at || 0) - new Date(a.last_event_at || 0)),
    }))
    .sort((a, b) => new Date(b.last_event_at || 0) - new Date(a.last_event_at || 0));
}

/** Drop duplicate rows when the same event was stored on visitor_events and lead_events (legacy data). */
function dedupeMergedTimelineEvents(merged) {
  const sorted = [...merged].sort(
    (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
  );
  const out = [];
  for (const ev of sorted) {
    const prev = out[out.length - 1];
    if (prev) {
      const dt = Math.abs(new Date(ev.created_at || 0) - new Date(prev.created_at || 0));
      const sameKind =
        String(ev.event_type || '') === String(prev.event_type || '') &&
        String(ev.event_name || ev.stage || '') === String(prev.event_name || prev.stage || '') &&
        String(ev.path || '') === String(prev.path || '');
      if (sameKind && dt < 4000) {
        if (ev._source === 'lead' && prev._source === 'visitor') {
          out[out.length - 1] = ev;
        }
        continue;
      }
    }
    out.push(ev);
  }
  return out;
}

function timelineFirstAndLastTouch(events) {
  const deduped = dedupeMergedTimelineEvents(events || []);
  const withPath = deduped.filter((ev) => strTrim(ev.path, 1000));
  const first = withPath[0] || null;
  const last = withPath[withPath.length - 1] || null;
  return {
    firstTouchPath: first?.path || null,
    firstTouchSource: first?.meta?.page || null,
    firstTouchReferrer: first?.referrer || null,
    lastTouchPath: last?.path || null,
    lastTouchSource: last?.meta?.page || null,
    lastTouchReferrer: last?.referrer || null,
  };
}

function deriveDropOffs(events, timeoutMinutes = 10) {
  const out = [];
  const sorted = (events || [])
    .filter((ev) => ev && ev.meta && ev.meta.funnel && (ev.meta.step_name || ev.stage || ev.event_name))
    .slice()
    .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0));
  for (let i = 0; i < sorted.length; i++) {
    const cur = sorted[i];
    const curFunnel = String(cur.meta?.funnel || '');
    const curStep = String(cur.meta?.step_name || cur.stage || cur.event_name || '');
    const curIdx = Number(cur.meta?.step_index ?? -1);
    if (!curFunnel || !curStep || Number.isNaN(curIdx) || curIdx < 0) continue;
    let advanced = false;
    for (let j = i + 1; j < sorted.length; j++) {
      const next = sorted[j];
      if (String(next.meta?.funnel || '') !== curFunnel) continue;
      const nextIdx = Number(next.meta?.step_index ?? -1);
      const dtMin = (new Date(next.created_at || 0) - new Date(cur.created_at || 0)) / 60000;
      if (dtMin > timeoutMinutes) break;
      if (!Number.isNaN(nextIdx) && nextIdx > curIdx) {
        advanced = true;
        break;
      }
    }
    if (!advanced) {
      out.push({
        funnel: curFunnel,
        step_name: curStep,
        step_index: curIdx,
        created_at: cur.created_at,
        path: cur.path || null,
        timeout_minutes: timeoutMinutes,
      });
    }
  }
  return out;
}

async function profileEventsTimeline(cfg, profileId, beforeIso = null) {
  const pid = String(profileId || '').trim();
  if (!pid) return [];
  const cutoff = beforeIso ? `&created_at=lte.${encodeURIComponent(String(beforeIso))}` : '';
  const [ve, le] = await Promise.all([
    supabaseRequest(
      cfg,
      'GET',
      `visitor_events?person_profile_id=eq.${encodeURIComponent(pid)}&select=id,session_id,event_type,event_name,path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,meta,created_at${cutoff}&order=created_at.asc&limit=5000`
    ),
    supabaseRequest(
      cfg,
      'GET',
      `lead_events?person_profile_id=eq.${encodeURIComponent(pid)}&select=id,session_id,event_type,event_name,stage,path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,meta,created_at${cutoff}&order=created_at.asc&limit=5000`
    ),
  ]);
  const visitorEvents = (JSON.parse(ve.body || '[]') || []).map((e) => ({ ...e, _source: 'visitor' }));
  const leadEvents = (JSON.parse(le.body || '[]') || []).map((e) => ({ ...e, _source: 'lead' }));
  return dedupeMergedTimelineEvents(
    [...visitorEvents, ...leadEvents].sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0))
  );
}

function isoDayBoundsIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('day');
  const end = start.plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoYesterdayBoundsIst() {
  const today = isoDayBoundsIst();
  const end = DateTime.fromISO(today.startIso);
  const start = end.minus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoLastNDaysRollingIst(n) {
  n = Math.max(1, Math.min(366, n));
  const t = isoDayBoundsIst();
  const end = DateTime.fromISO(t.endIso);
  const start = DateTime.fromISO(t.startIso).minus({ days: n - 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoThisMonthIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('month');
  const end = now.startOf('day').plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoThisWeekIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('week');
  const end = now.startOf('day').plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

export function resolveAnalyticsWindow(q) {
  const from = strTrim(q.date_from, 100) || '';
  const to = strTrim(q.date_to, 100) || '';
  if (from && to) {
    const a = Date.parse(from);
    const b = Date.parse(to);
    if (!Number.isNaN(a) && !Number.isNaN(b) && a < b) {
      return { startIso: from, endIso: to, label: 'custom' };
    }
  }
  const preset = String(q.preset ?? '')
    .trim()
    .toLowerCase();
  if (preset === 'yesterday') {
    const y = isoYesterdayBoundsIst();
    return { ...y, label: 'yesterday' };
  }
  if (preset === 'last3') {
    const r = isoLastNDaysRollingIst(3);
    return { ...r, label: 'last3' };
  }
  if (preset === 'last7') {
    const r = isoLastNDaysRollingIst(7);
    return { ...r, label: 'last7' };
  }
  if (preset === 'last30') {
    const r = isoLastNDaysRollingIst(30);
    return { ...r, label: 'last30' };
  }
  if (preset === 'this_month') {
    const r = isoThisMonthIst();
    return { ...r, label: 'this_month' };
  }
  if (preset === 'this_week') {
    const r = isoThisWeekIst();
    return { ...r, label: 'this_week' };
  }
  const t = isoDayBoundsIst();
  return { ...t, label: 'today' };
}

export function pageFromQuery(q) {
  const page = Math.max(1, parseInt(String(q.page ?? '1'), 10) || 1);
  const perPage = Math.min(100, Math.max(1, parseInt(String(q.per_page ?? '10'), 10) || 10));
  const offset = (page - 1) * perPage;
  const end = offset + perPage - 1;
  return { page, perPage, range: `${offset}-${end}` };
}

function buildOrdersFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  if (s(q.order_status)) parts.push(`order_status=eq.${encodeURIComponent(s(q.order_status))}`);
  if (s(q.payment_status)) parts.push(`payment_status=eq.${encodeURIComponent(s(q.payment_status))}`);
  if (s(q.product_slug)) parts.push(`product_slug=eq.${encodeURIComponent(s(q.product_slug))}`);
  const acq = s(q.acquisition).toLowerCase();
  if (acq === 'direct') parts.push('lead_id=is.null');
  if (acq === 'lead') parts.push('lead_id=not.is.null');
  const df = s(q.date_from);
  const dt = s(q.date_to);
  /* PostgREST ORs repeated column filters — must use and=(...) for a real range. */
  if (df && dt) {
    parts.push(`and=(paid_at.gte.${encodeURIComponent(df)},paid_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`paid_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`paid_at=lte.${encodeURIComponent(dt)}`);
  }
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(razorpay_order_id.ilike.*${x}*,razorpay_payment_id.ilike.*${x}*,receipt.ilike.*${x}*)`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildCustomersFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const paying = s(q.paying);
  if (paying === '1' || paying === 'true' || paying === 'yes') parts.push('is_paying_customer=eq.true');
  else if (paying === '0' || paying === 'false' || paying === 'no') parts.push('is_paying_customer=eq.false');
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(email.ilike.*${x}*,name.ilike.*${x}*,phone.ilike.*${x}*)`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(created_at.gte.${encodeURIComponent(df)},created_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`created_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`created_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

/** Normalize pasted lead UUID (with or without hyphens) for id=eq filter. */
function normalizeLeadUuidSearch(raw) {
  const t = String(raw ?? '')
    .trim()
    .replace(/\s+/g, '');
  if (!t) return null;
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(t)) {
    return t.toLowerCase();
  }
  const compact = t.replace(/-/g, '');
  if (/^[0-9a-f]{32}$/i.test(compact)) {
    return `${compact.slice(0, 8)}-${compact.slice(8, 12)}-${compact.slice(12, 16)}-${compact.slice(16, 20)}-${compact.slice(20, 32)}`.toLowerCase();
  }
  return null;
}

function buildLeadsFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const contactsOnly = s(q.contacts_only).toLowerCase();
  if (contactsOnly === '1' || contactsOnly === 'true' || contactsOnly === 'yes') {
    parts.push('or=(phone.not.is.null,email.not.is.null)');
  }
  if (s(q.utm_source)) parts.push(`utm_source=eq.${encodeURIComponent(s(q.utm_source))}`);
  if (s(q.utm_medium)) parts.push(`utm_medium=eq.${encodeURIComponent(s(q.utm_medium))}`);
  if (s(q.utm_campaign)) parts.push(`utm_campaign=ilike.*${encodeURIComponent(s(q.utm_campaign))}*`);
  if (s(q.lead_status)) parts.push(`lead_status=eq.${encodeURIComponent(s(q.lead_status))}`);
  const conv = s(q.converted).toLowerCase();
  if (['yes', '1', 'true'].includes(conv)) parts.push('converted_order_id=not.is.null');
  else if (['no', '0', 'false'].includes(conv)) parts.push('converted_order_id=is.null');
  if (s(q.search)) {
    const raw = s(q.search);
    const x = encodeURIComponent(raw);
    const orParts = [
      `email.ilike.*${x}*`,
      `session_id.ilike.*${x}*`,
      `phone.ilike.*${x}*`,
      `name.ilike.*${x}*`,
      `id_text.ilike.*${x}*`,
    ];
    const uuidEq = normalizeLeadUuidSearch(raw);
    if (uuidEq) {
      orParts.push(`id.eq.${encodeURIComponent(uuidEq)}`);
    }
    parts.push(`or=(${orParts.join(',')})`);
  }
  const it = s(q.intent_tier).toLowerCase();
  if (it === 'high' || it === 'medium' || it === 'low') {
    parts.push(`intent_tier=eq.${encodeURIComponent(it)}`);
  }
  const imin = parseInt(s(q.intent_min), 10);
  const imax = parseInt(s(q.intent_max), 10);
  const hasMin = !Number.isNaN(imin);
  const hasMax = !Number.isNaN(imax);
  if (hasMin && hasMax) {
    parts.push(`and=(intent_score.gte.${imin},intent_score.lte.${imax})`);
  } else if (hasMin) {
    parts.push(`intent_score=gte.${imin}`);
  } else if (hasMax) {
    parts.push(`intent_score=lte.${imax}`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  const dfld = s(q.date_field);
  const field = dfld === 'created' ? 'created_at' : dfld === 'first_seen' ? 'first_seen_at' : 'last_seen_at';
  if (df && dt) {
    parts.push(`and=(${field}.gte.${encodeURIComponent(df)},${field}.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`${field}=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`${field}=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildVisitorsFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const conv = s(q.converted).toLowerCase();
  if (conv === 'yes' || conv === '1' || conv === 'true') parts.push('converted_lead_id=not.is.null');
  else if (conv === 'no' || conv === '0' || conv === 'false') parts.push('converted_lead_id=is.null');
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(session_id.ilike.*${x}*,email.ilike.*${x}*,phone.ilike.*${x}*,name.ilike.*${x}*,landing_path.ilike.*${x}*)`);
  }
  if (s(q.utm_source)) parts.push(`utm_source=eq.${encodeURIComponent(s(q.utm_source))}`);
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(last_seen_at.gte.${encodeURIComponent(df)},last_seen_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`last_seen_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`last_seen_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildAbandonedFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  if (s(q.stage)) parts.push(`stage=eq.${encodeURIComponent(s(q.stage))}`);
  if (s(q.utm_campaign)) parts.push(`utm_campaign=ilike.*${encodeURIComponent(s(q.utm_campaign))}*`);
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(email.ilike.*${x}*,checkout_session_id.ilike.*${x}*,phone.ilike.*${x}*)`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(last_event_at.gte.${encodeURIComponent(df)},last_event_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`last_event_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`last_event_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildProfilesFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  // Admin Profiles should only show contact-known people.
  parts.push('or=(canonical_email.not.is.null,canonical_phone.not.is.null)');
  if (s(q.search)) {
    const raw = s(q.search);
    const x = encodeURIComponent(raw);
    parts.push(
      `or=(canonical_email.ilike.*${x}*,canonical_phone.ilike.*${x}*,canonical_name.ilike.*${x}*,id_text.ilike.*${x}*,merged_session_ids_text.ilike.*${x}*)`
    );
  }
  if (s(q.lifecycle_stage)) parts.push(`lifecycle_stage=eq.${encodeURIComponent(s(q.lifecycle_stage))}`);
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(last_seen_at.gte.${encodeURIComponent(df)},last_seen_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`last_seen_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`last_seen_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

export async function adminListOrders(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildOrdersFilterQs(q);
  const sel = encodeURIComponent(ADMIN_ORDERS_SELECT);
  const path = `orders?select=${sel}&order=created_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

/** Same filters as list; sums revenue for current filter (cap rows for safety). */
export async function adminOrdersSummary(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const filter = buildOrdersFilterQs(q);
  const cap = 8000;
  const sel = encodeURIComponent('amount_paise,currency,lead_id');
  const path = `orders?select=${sel}&order=created_at.desc${filter}&limit=${cap}`;
  const r = await supabaseRequest(cfg, 'GET', path);
  if (r.code < 200 || r.code >= 300) {
    return { ok: false, error: (r.body || '').slice(0, 400) };
  }
  const rows = JSON.parse(r.body || '[]');
  const list = Array.isArray(rows) ? rows : [];
  let rev = 0;
  let fromLead = 0;
  let direct = 0;
  for (const o of list) {
    rev += parseInt(o.amount_paise ?? 0, 10);
    if (o.lead_id) fromLead++;
    else direct++;
  }
  return {
    ok: true,
    orderCount: list.length,
    revenueInr: Math.round(rev) / 100,
    revenuePaise: rev,
    truncated: list.length >= cap,
    ordersAttributedToLead: fromLead,
    ordersDirectPurchase: direct,
  };
}

export async function adminListCustomers(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildCustomersFilterQs(q);
  const sel =
    'id,email,name,phone,person_profile_id,is_paying_customer,first_paid_at,total_spent_paise,notes,meta,created_at,updated_at';
  const path = `customers?select=${encodeURIComponent(sel)}&order=created_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListProfiles(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildProfilesFilterQs(q);
  const path = `person_profiles?select=${encodeURIComponent(ADMIN_PROFILES_SELECT)}&order=last_seen_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminProfileDetail(cfg, profileId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(profileId || '').trim();
  if (!id) return { ok: false, error: 'profile id required' };
  const pr = await supabaseRequest(
    cfg,
    'GET',
    `person_profiles?id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_PROFILES_SELECT)}&limit=1`
  );
  if (pr.code < 200 || pr.code >= 300) return { ok: false, error: (pr.body || '').slice(0, 300) };
  const rows = JSON.parse(pr.body || '[]');
  const profile = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!profile) return { ok: false, error: 'Profile not found' };

  const [visitors, leads, abandoned, orders, bookings, customers] = await Promise.all([
    supabaseRequest(cfg, 'GET', `visitors?person_profile_id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_VISITORS_SELECT)}&order=last_seen_at.desc&limit=200`),
    supabaseRequest(cfg, 'GET', `leads?person_profile_id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_LEADS_SELECT)}&order=last_seen_at.desc&limit=200`),
    supabaseRequest(cfg, 'GET', `abandoned_checkouts?person_profile_id=eq.${encodeURIComponent(id)}&select=id,checkout_session_id,lead_id,email,name,phone,product_slug,stage,amount_paise,currency,last_event_at,abandoned_at,converted_order_id,converted_at,landing_path,referrer,utm_source,utm_medium,utm_campaign,created_at,updated_at&order=last_event_at.desc&limit=200`),
    supabaseRequest(cfg, 'GET', `orders?person_profile_id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_ORDERS_SELECT)}&order=paid_at.desc&limit=200`),
    supabaseRequest(cfg, 'GET', `consultancy_bookings?person_profile_id=eq.${encodeURIComponent(id)}&select=id,lead_id,session_id,name,email,phone,topic,slot_start,slot_end,status,payment_status,plan_code,plan_name,duration_minutes,amount_paise,currency,razorpay_order_id,razorpay_payment_id,created_at,updated_at&order=created_at.desc&limit=200`),
    supabaseRequest(cfg, 'GET', `customers?person_profile_id=eq.${encodeURIComponent(id)}&select=id,email,name,phone,is_paying_customer,first_paid_at,total_spent_paise,notes,meta,created_at,updated_at&limit=50`),
  ]);

  const events = await profileEventsTimeline(cfg, id);
  const touches = timelineFirstAndLastTouch(events);
  const drops = deriveDropOffs(events);

  return {
    ok: true,
    profile: { ...profile, ...touches, latest_drop: drops[drops.length - 1] || null },
    visitors: JSON.parse(visitors.body || '[]') || [],
    leads: JSON.parse(leads.body || '[]') || [],
    abandonedCheckouts: JSON.parse(abandoned.body || '[]') || [],
    orders: JSON.parse(orders.body || '[]') || [],
    consultancyBookings: JSON.parse(bookings.body || '[]') || [],
    customers: JSON.parse(customers.body || '[]') || [],
    events,
    drops,
  };
}

export async function adminListLeads(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const groupByPerson = queryFlagOn(q, 'group_by_person');
  if (groupByPerson) {
    const cap = 5000;
    const filter = buildLeadsFilterQs(q);
    const path = `leads?select=${encodeURIComponent(ADMIN_LEADS_ROLLUP_SELECT)}&order=last_seen_at.desc${filter}&limit=${cap}`;
    const r = await supabaseRequest(cfg, 'GET', path);
    if (r.code < 200 || r.code >= 300) {
      return { ok: false, error: (r.body || '').slice(0, 400) };
    }
    const list = JSON.parse(r.body || '[]');
    const raw = Array.isArray(list) ? list : [];
    const groups = groupLeadsByPerson(raw);
    const start = (pg.page - 1) * pg.perPage;
    const pageRows = groups.slice(start, start + pg.perPage);
    return {
      ok: true,
      rows: pageRows,
      total: groups.length,
      page: pg.page,
      perPage: pg.perPage,
      grouped: true,
      truncated: raw.length >= cap,
    };
  }

  const filter = buildLeadsFilterQs(q);
  const path = `leads?select=${encodeURIComponent(ADMIN_LEADS_SELECT)}&order=last_seen_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
      grouped: false,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListAbandoned(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const groupByPerson = queryFlagOn(q, 'group_by_person');
  if (groupByPerson) {
    const cap = 5000;
    const filter = buildAbandonedFilterQs(q);
    const path = `abandoned_checkouts?select=${encodeURIComponent(ADMIN_ABANDONED_ROLLUP_SELECT)}&order=last_event_at.desc${filter}&limit=${cap}`;
    const r = await supabaseRequest(cfg, 'GET', path);
    if (r.code < 200 || r.code >= 300) {
      return { ok: false, error: (r.body || '').slice(0, 400) };
    }
    const list = JSON.parse(r.body || '[]');
    const raw = Array.isArray(list) ? list : [];
    const groups = groupAbandonedByPerson(raw);
    const start = (pg.page - 1) * pg.perPage;
    const pageRows = groups.slice(start, start + pg.perPage);
    return {
      ok: true,
      rows: pageRows,
      total: groups.length,
      page: pg.page,
      perPage: pg.perPage,
      grouped: true,
      truncated: raw.length >= cap,
    };
  }

  const filter = buildAbandonedFilterQs(q);
  const sel =
    'id,checkout_session_id,lead_id,email,name,phone,product_slug,stage,razorpay_order_id,amount_paise,currency,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,landing_path,last_event_at,abandoned_at,converted_order_id,converted_at,meta,created_at,updated_at,person_profile_id,leads(session_id,utm_campaign,utm_source,utm_medium,landing_path)';
  const path = `abandoned_checkouts?select=${encodeURIComponent(sel)}&order=last_event_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
      grouped: false,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminUpdateOrderStatus(cfg, orderId, orderStatus) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(orderId).trim();
  const st = strTrim(orderStatus, 64) || '';
  if (!id || !st) return { ok: false, error: 'order id and order_status required' };
  const patch = JSON.stringify({ order_status: st, updated_at: new Date().toISOString() });
  const r = await supabaseRequest(cfg, 'PATCH', `orders?id=eq.${encodeURIComponent(id)}`, patch, 'return=minimal');
  if (r.code >= 200 && r.code < 300) return { ok: true };
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminDeleteLead(cfg, leadId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(leadId).trim();
  if (!id) return { ok: false, error: 'Missing lead id' };
  const r = await supabaseRequest(cfg, 'DELETE', `leads?id=eq.${encodeURIComponent(id)}`, null, 'return=minimal');
  if (r.code >= 200 && r.code < 300) return { ok: true };
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminOrderPrePurchaseTimeline(cfg, orderId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(orderId).trim();
  if (!id) return { ok: false, error: 'order id required' };
  const oq = `orders?id=eq.${encodeURIComponent(id)}&select=id,lead_id,person_profile_id,paid_at,created_at,product_slug&limit=1`;
  const or = await supabaseRequest(cfg, 'GET', oq);
  if (or.code < 200 || or.code >= 300) {
    return { ok: false, error: (or.body || '').slice(0, 300) };
  }
  const rows = JSON.parse(or.body || '[]');
  const order = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!order) return { ok: false, error: 'Order not found' };
  const paidAt = order.paid_at ?? order.created_at;
  const leadId = order.lead_id ?? null;
  const personProfileId = order.person_profile_id ?? null;
  const out = {
    ok: true,
    orderId: order.id,
    leadId,
    personProfileId,
    paidAt,
    acquisition: leadId ? 'lead_attributed' : 'direct_purchase',
    events: [],
  };
  if (personProfileId) {
    out.events = await profileEventsTimeline(cfg, personProfileId, paidAt);
    return out;
  }
  if (!leadId) return out;

  let visitorId = null;
  const lq = `leads?id=eq.${encodeURIComponent(String(leadId))}&select=visitor_id&limit=1`;
  const lres = await supabaseRequest(cfg, 'GET', lq);
  if (lres.code >= 200 && lres.code < 300) {
    const lr = JSON.parse(lres.body || '[]');
    if (lr[0]?.visitor_id) visitorId = String(lr[0].visitor_id);
  }

  const visitorBefore = [];
  if (visitorId) {
    const vUrl =
      `visitor_events?visitor_id=eq.${encodeURIComponent(visitorId)}` +
      '&select=id,session_id,event_type,event_name,path,referrer,meta,created_at' +
      `&created_at=lte.${encodeURIComponent(String(paidAt))}` +
      '&order=created_at.asc&limit=500';
    const vr = await supabaseRequest(cfg, 'GET', vUrl);
    if (vr.code >= 200 && vr.code < 300) {
      const evs = JSON.parse(vr.body || '[]');
      visitorBefore.push(...(Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'visitor' })) : []));
    }
  }

  const evUrl =
    `lead_events?lead_id=eq.${encodeURIComponent(String(leadId))}` +
    '&select=id,session_id,event_type,event_name,stage,path,referrer,utm_source,utm_medium,utm_campaign,meta,created_at' +
    `&created_at=lte.${encodeURIComponent(String(paidAt))}` +
    '&order=created_at.asc&limit=500';
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let leadEvs = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    leadEvs = Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'lead' })) : [];
  }

  const merged = dedupeMergedTimelineEvents(
    [...visitorBefore, ...leadEvs].sort(
      (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
    )
  );
  out.events = merged;
  out.visitorEvents = visitorBefore;
  out.leadOnlyEvents = leadEvs;
  return out;
}

export async function adminGetAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const label = w.label;

  const leadsPath = `leads?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;
  const leadsConvPath = `leads?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})&converted_order_id=not.is.null`;

  const leadsIn = await supabaseCountRows(cfg, leadsPath);
  const leadsConv = await supabaseCountRows(cfg, leadsConvPath);
  const leadPct = leadsIn > 0 ? Math.round((leadsConv / leadsIn) * 1000) / 10 : 0;

  const ordersPath = `orders?select=amount_paise,lead_id,currency&and=(paid_at.gte.${encodeURIComponent(startIso)},paid_at.lt.${encodeURIComponent(endIso)},payment_status.eq.paid)&limit=5000`;
  const or = await supabaseRequest(cfg, 'GET', ordersPath);
  let ordersRows = [];
  if (or.code >= 200 && or.code < 300) {
    const rows = JSON.parse(or.body || '[]');
    ordersRows = Array.isArray(rows) ? rows : [];
  }
  let rev = 0;
  let fromLead = 0;
  let direct = 0;
  for (const o of ordersRows) {
    rev += parseInt(o.amount_paise ?? 0, 10);
    if (o.lead_id) fromLead++;
    else direct++;
  }
  const ordersPaid = ordersRows.length;

  const allOrders = await supabaseCountRows(cfg, 'orders?select=id');
  const allLeads = await supabaseCountRows(cfg, 'leads?select=id');
  const allCust = await supabaseCountRows(cfg, 'customers?select=id');
  const allVisitors = await supabaseCountRows(cfg, 'visitors?select=id');
  const allProfiles = await supabaseCountRows(cfg, 'person_profiles?select=id');

  const visitorsPath = `visitors?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;
  const visitorsNew = await supabaseCountRows(cfg, visitorsPath);
  const visitorToLeadPath = `visitors?select=id&and=(conversion_at.gte.${encodeURIComponent(startIso)},conversion_at.lt.${encodeURIComponent(endIso)})&converted_lead_id=not.is.null`;
  const visitorsConvertedToLead = await supabaseCountRows(cfg, visitorToLeadPath);

  const lsWin = `and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`;
  const intentHigh = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.high`);
  const intentMedium = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.medium`);
  const intentLow = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.low`);

  const pageViewsQ = `lead_events?select=id&and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},event_name.eq.page_view)`;
  const pageViewsTotal = await supabaseCountRows(cfg, pageViewsQ);

  const visitorsActiveQ = `visitors?select=id&and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`;
  const visitorsActiveInPeriod = await supabaseCountRows(cfg, visitorsActiveQ);

  const abWin = `and=(last_event_at.gte.${encodeURIComponent(startIso)},last_event_at.lt.${encodeURIComponent(endIso)})`;
  const abandonedCheckoutSessions = await supabaseCountRows(cfg, `abandoned_checkouts?select=id&${abWin}`);
  const abandonedConvertedLater = await supabaseCountRows(
    cfg,
    `abandoned_checkouts?select=id&${abWin}&converted_order_id=not.is.null`
  );

  const profilesActive = await supabaseCountRows(
    cfg,
    `person_profiles?select=id&and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`
  );
  const profilesCustomers = await supabaseCountRows(
    cfg,
    `person_profiles?select=id&lifecycle_stage=eq.customer&and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`
  );
  const [leadDropSeed, visitorDropSeed] = await Promise.all([
    supabaseRequest(
      cfg,
      'GET',
      `lead_events?select=session_id,event_type,event_name,stage,path,meta,created_at&and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)})&limit=5000`
    ),
    supabaseRequest(
      cfg,
      'GET',
      `visitor_events?select=session_id,event_type,event_name,path,meta,created_at&and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)})&limit=5000`
    ),
  ]);
  const derivedDrops = deriveDropOffs(
    []
      .concat((JSON.parse(leadDropSeed.body || '[]') || []).map((e) => ({ ...e, _source: 'lead' })))
      .concat((JSON.parse(visitorDropSeed.body || '[]') || []).map((e) => ({ ...e, _source: 'visitor' })))
  );

  const period = {
    leadsCollected: leadsIn,
    leadsConverted: leadsConv,
    leadToOrderConversionPercent: leadPct,
    ordersPaid,
    revenuePaise: rev,
    revenueInr: Math.round(rev) / 100,
    ordersAttributedToLead: fromLead,
    ordersDirectPurchase: direct,
    visitorsNew,
    visitorsActiveInPeriod,
    pageViewsTotal,
    abandonedCheckoutSessions,
    abandonedLaterPaid: abandonedConvertedLater,
    profilesActive,
    customerProfilesActive: profilesCustomers,
    funnelDropOffs: derivedDrops.length,
    visitorsConvertedToLead,
    visitorToLeadRatePercent:
      visitorsNew > 0 ? Math.round((visitorsConvertedToLead / visitorsNew) * 1000) / 10 : 0,
    intentLeadsHigh: intentHigh,
    intentLeadsMedium: intentMedium,
    intentLeadsLow: intentLow,
  };

  return {
    ok: true,
    timezone: ADMIN_TZ,
    preset: label,
    periodStart: startIso,
    periodEnd: endIso,
    dayStart: startIso,
    dayEnd: endIso,
    today: period,
    period,
    allTime: {
      orders: allOrders,
      leads: allLeads,
      customers: allCust,
      visitors: allVisitors,
      profiles: allProfiles,
    },
  };
}

export async function adminSaveAnalyticsSnapshot(cfg, body) {
  const metrics = await adminGetAnalytics(cfg, body);
  if (!metrics.ok) {
    return { ok: false, error: String(metrics.error ?? 'Analytics failed') };
  }
  const w = resolveAnalyticsWindow(body);
  const row = {
    period_start: w.startIso,
    period_end: w.endIso,
    preset: metrics.preset ?? null,
    payload: metrics,
  };
  const ins = await supabaseRequest(cfg, 'POST', 'analytics_snapshots', JSON.stringify(row), 'return=minimal');
  if (ins.code >= 200 && ins.code < 300) return { ok: true };
  return { ok: false, error: (ins.body || '').slice(0, 400) };
}

export async function adminListAnalyticsSnapshots(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const limit = Math.min(100, Math.max(1, parseInt(String(q.limit ?? '20'), 10) || 20));
  const path = `analytics_snapshots?select=id,created_at,period_start,period_end,preset,payload&order=created_at.desc&limit=${limit}`;
  const r = await supabaseRequest(cfg, 'GET', path);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return { ok: true, rows: Array.isArray(rows) ? rows : [] };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListVisitors(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const groupByPerson = queryFlagOn(q, 'group_by_person');
  if (groupByPerson) {
    const cap = 5000;
    const filter = buildVisitorsFilterQs(q);
    const path = `visitors?select=${encodeURIComponent(ADMIN_VISITORS_ROLLUP_SELECT)}&order=last_seen_at.desc${filter}&limit=${cap}`;
    const r = await supabaseRequest(cfg, 'GET', path);
    if (r.code < 200 || r.code >= 300) {
      return { ok: false, error: (r.body || '').slice(0, 400) };
    }
    const list = JSON.parse(r.body || '[]');
    const raw = Array.isArray(list) ? list : [];
    const groups = groupVisitorsByPerson(raw);
    const start = (pg.page - 1) * pg.perPage;
    const pageGroups = groups.slice(start, start + pg.perPage);
    const enriched = await Promise.all(
      pageGroups.map(async (g) => {
        const row = g.primary_visitor;
        if (!row) return g;
        try {
          const firstPath = await firstSitePathFromVisitorTimeline(cfg, row);
          if (firstPath) {
            return {
              ...g,
              primary_visitor: { ...row, landing_path: firstPath },
            };
          }
        } catch {
          /* keep */
        }
        return g;
      })
    );
    return {
      ok: true,
      rows: enriched,
      total: groups.length,
      page: pg.page,
      perPage: pg.perPage,
      grouped: true,
      truncated: raw.length >= cap,
    };
  }

  const filter = buildVisitorsFilterQs(q);
  const path = `visitors?select=${encodeURIComponent(ADMIN_VISITORS_SELECT)}&order=last_seen_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    const list = Array.isArray(rows) ? rows : [];
    const enriched = await Promise.all(
      list.map(async (row) => {
        try {
          const firstPath = await firstSitePathFromVisitorTimeline(cfg, row);
          if (firstPath) return { ...row, landing_path: firstPath };
        } catch {
          /* keep row.landing_path */
        }
        return row;
      })
    );
    return {
      ok: true,
      rows: enriched,
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
      grouped: false,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminVisitorTimeline(cfg, visitorId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(visitorId).trim();
  if (!id) return { ok: false, error: 'visitor id required' };
  const vq = `visitors?id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_VISITORS_SELECT)}&limit=1`;
  const vr = await supabaseRequest(cfg, 'GET', vq);
  if (vr.code < 200 || vr.code >= 300) {
    return { ok: false, error: (vr.body || '').slice(0, 300) };
  }
  const vrows = JSON.parse(vr.body || '[]');
  const visitor = Array.isArray(vrows) && vrows[0] ? vrows[0] : null;
  if (!visitor) return { ok: false, error: 'Visitor not found' };

  if (visitor.person_profile_id) {
    const events = await profileEventsTimeline(cfg, visitor.person_profile_id);
    return {
      ok: true,
      visitor,
      visitorEvents: events.filter((e) => e._source === 'visitor'),
      leadEvents: events.filter((e) => e._source === 'lead'),
      touches: timelineFirstAndLastTouch(events),
    };
  }

  const evUrl = `visitor_events?visitor_id=eq.${encodeURIComponent(id)}&select=id,event_type,event_name,path,referrer,meta,created_at&order=created_at.asc&limit=2000`;
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let visitorEvents = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    visitorEvents = Array.isArray(evs) ? evs : [];
  }

  let leadEvents = [];
  if (visitor.converted_lead_id) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `lead_events?lead_id=eq.${encodeURIComponent(String(visitor.converted_lead_id))}&select=id,event_type,event_name,stage,path,referrer,meta,created_at&order=created_at.asc&limit=2000`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const evs = JSON.parse(lr.body || '[]');
      leadEvents = Array.isArray(evs) ? evs : [];
    }
  }

  return { ok: true, visitor, visitorEvents, leadEvents, touches: timelineFirstAndLastTouch(
    dedupeMergedTimelineEvents(
      visitorEvents
        .map((e) => ({ ...e, _source: 'visitor' }))
        .concat(leadEvents.map((e) => ({ ...e, _source: 'lead' })))
    )
  ) };
}

/** Prior abandon sessions + conversion label for admin abandoned-checkout expand. */
export async function adminAbandonedCheckoutContext(cfg, abandonedId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(abandonedId || '').trim();
  if (!id) return { ok: false, error: 'id required' };
  const gr = await supabaseRequest(
    cfg,
    'GET',
    `abandoned_checkouts?id=eq.${encodeURIComponent(id)}&select=id,email,name,phone,checkout_session_id,stage,last_event_at,abandoned_at,converted_order_id,converted_at,utm_source,utm_medium,utm_campaign,landing_path,referrer,product_slug,amount_paise,currency&limit=1`
  );
  if (gr.code < 200 || gr.code >= 300) {
    return { ok: false, error: (gr.body || '').slice(0, 300) };
  }
  const rows = JSON.parse(gr.body || '[]');
  const row = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!row) return { ok: false, error: 'Abandoned checkout not found' };

  const email = String(row.email || '')
    .trim()
    .toLowerCase();
  const phone = String(row.phone || '').replace(/\s+/g, '');

  let priorAbandonSessions = 0;
  if (email || phone) {
    const orParts = [];
    if (email) orParts.push(`email.eq.${encodeURIComponent(email)}`);
    if (phone) orParts.push(`phone.eq.${encodeURIComponent(phone)}`);
    const orq = orParts.join(',');
    const pq = `abandoned_checkouts?select=id&or=(${orq})&last_event_at=lt.${encodeURIComponent(String(row.last_event_at))}&id=neq.${encodeURIComponent(id)}`;
    priorAbandonSessions = await supabaseCountRows(cfg, pq);
  }

  let conversionSummary = 'Not paid yet';
  let paidOrderNumber = 0;
  if (row.converted_order_id) {
    const ordr = await supabaseRequest(
      cfg,
      'GET',
      `orders?id=eq.${encodeURIComponent(String(row.converted_order_id))}&select=id,customer_id,paid_at,payment_status&limit=1`
    );
    let ord = null;
    if (ordr.code >= 200 && ordr.code < 300) {
      const orows = JSON.parse(ordr.body || '[]');
      ord = Array.isArray(orows) && orows[0] ? orows[0] : null;
    }
    if (ord && ord.customer_id) {
      const paidAt = ord.paid_at || row.converted_at;
      const cntPath = `orders?select=id&customer_id=eq.${encodeURIComponent(String(ord.customer_id))}&payment_status=eq.paid&paid_at=lte.${encodeURIComponent(String(paidAt))}`;
      paidOrderNumber = await supabaseCountRows(cfg, cntPath);
      if (paidOrderNumber <= 1) {
        conversionSummary = 'First payment from this customer (1st time converted)';
      } else {
        conversionSummary = `Return buyer — this is payment #${paidOrderNumber} for this customer`;
      }
    } else {
      conversionSummary = 'Marked converted — order or customer link missing';
    }
  }

  return {
    ok: true,
    priorAbandonSessions,
    conversionSummary,
    paidOrderNumber,
    repeatAbandoner: priorAbandonSessions > 0,
  };
}

function phoneMatchOrParts(phoneRaw) {
  const raw = String(phoneRaw || '').trim();
  const digits = raw.replace(/\D/g, '');
  const parts = [];
  if (raw) parts.push(`phone.eq.${encodeURIComponent(raw)}`);
  if (digits && digits.length >= 8) {
    parts.push(`phone.eq.${encodeURIComponent(digits)}`);
    if (digits.length === 10) parts.push(`phone.eq.${encodeURIComponent('91' + digits)}`);
    if (digits.length === 12 && digits.startsWith('91'))
      parts.push(`phone.eq.${encodeURIComponent(digits.slice(2))}`);
  }
  return parts;
}

/** Full visitor + lead event history: prefer lead linked from a paid order, else email/phone on leads. */
export async function adminCustomerActivityTimeline(cfg, customerId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const cid = String(customerId || '').trim();
  if (!cid) return { ok: false, error: 'customer id required' };
  const cr = await supabaseRequest(
    cfg,
    'GET',
    `customers?id=eq.${encodeURIComponent(cid)}&select=id,email,name,phone,created_at&limit=1`
  );
  if (cr.code < 200 || cr.code >= 300) {
    return { ok: false, error: (cr.body || '').slice(0, 300) };
  }
  const crows = JSON.parse(cr.body || '[]');
  const customer = Array.isArray(crows) && crows[0] ? crows[0] : null;
  if (!customer) return { ok: false, error: 'Customer not found' };

  if (customer.person_profile_id) {
    const events = await profileEventsTimeline(cfg, customer.person_profile_id);
    return { ok: true, customer, lead: null, events };
  }

  const em = String(customer.email || '').trim().toLowerCase();
  const ph = String(customer.phone || '').replace(/\s+/g, '');

  let lead = null;
  const oPath =
    `orders?customer_id=eq.${encodeURIComponent(cid)}` +
    '&lead_id=not.is.null&select=lead_id&order=paid_at.desc&limit=1';
  const orderLeadRes = await supabaseRequest(cfg, 'GET', oPath);
  let leadIdFromOrder = null;
  if (orderLeadRes.code >= 200 && orderLeadRes.code < 300) {
    const orows = JSON.parse(orderLeadRes.body || '[]');
    if (Array.isArray(orows) && orows[0] && orows[0].lead_id) {
      leadIdFromOrder = String(orows[0].lead_id);
    }
  }
  if (leadIdFromOrder) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `leads?id=eq.${encodeURIComponent(leadIdFromOrder)}&select=id,visitor_id,session_id,first_seen_at,last_seen_at&limit=1`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const lrows = JSON.parse(lr.body || '[]');
      lead = Array.isArray(lrows) && lrows[0] ? lrows[0] : null;
    }
  }

  if (!lead) {
    const orParts = [];
    if (em) {
      orParts.push(`email.eq.${encodeURIComponent(em)}`);
      orParts.push(`email.ilike.${encodeURIComponent('*' + em + '*')}`);
    }
    orParts.push(...phoneMatchOrParts(ph));
    const uniq = [...new Set(orParts)];
    if (!uniq.length) {
      return {
        ok: true,
        customer,
        lead: null,
        events: [],
        note: 'No email or phone on file — cannot match activity.',
      };
    }
    const lq = `leads?select=id,visitor_id,session_id,first_seen_at,last_seen_at&or=(${uniq.join(',')})&order=last_seen_at.desc&limit=1`;
    const lr = await supabaseRequest(cfg, 'GET', lq);
    if (lr.code >= 200 && lr.code < 300) {
      const lrows = JSON.parse(lr.body || '[]');
      lead = Array.isArray(lrows) && lrows[0] ? lrows[0] : null;
    }
  }

  if (!lead) {
    return {
      ok: true,
      customer,
      lead: null,
      events: [],
      note: 'No browsing history matched yet — it appears after they use the site with tracking, or after a linked order.',
    };
  }

  const visitorBefore = [];
  const vid = lead.visitor_id ? String(lead.visitor_id) : '';
  if (vid) {
    const vUrl =
      `visitor_events?visitor_id=eq.${encodeURIComponent(vid)}` +
      '&select=id,session_id,event_type,event_name,path,referrer,meta,created_at&order=created_at.asc&limit=2000';
    const vr = await supabaseRequest(cfg, 'GET', vUrl);
    if (vr.code >= 200 && vr.code < 300) {
      const evs = JSON.parse(vr.body || '[]');
      visitorBefore.push(
        ...(Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'visitor' })) : [])
      );
    }
  }

  const evUrl =
    `lead_events?lead_id=eq.${encodeURIComponent(String(lead.id))}` +
    '&select=id,session_id,event_type,event_name,stage,path,referrer,utm_source,utm_medium,utm_campaign,meta,created_at&order=created_at.asc&limit=2000';
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let leadEvs = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    leadEvs = Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'lead' })) : [];
  }

  const merged = dedupeMergedTimelineEvents(
    [...visitorBefore, ...leadEvs].sort(
      (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
    )
  );

  return { ok: true, customer, lead, events: merged };
}

export async function adminRebuildProfilesNow(cfg) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const r = await supabaseRequest(cfg, 'POST', 'rpc/rebuild_profiles_now');
  if (r.code >= 200 && r.code < 300) {
    return { ok: true };
  }
  return { ok: false, error: (r.body || '').slice(0, 500) };
}
