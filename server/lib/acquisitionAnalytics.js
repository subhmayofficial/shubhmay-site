import { supabaseRequest } from './supabase.js';
import { strTrim } from './strings.js';
import { resolveAnalyticsWindow } from './admin.js';

function normDim(v) {
  const s = String(v ?? '').trim();
  return s || '(not set)';
}

function utm3Key(r) {
  return `${normDim(r.utm_source)}\t${normDim(r.utm_medium)}\t${normDim(r.utm_campaign)}`;
}

function landingMatches(pathRaw, prefix) {
  if (!prefix) return true;
  const p = String(pathRaw ?? '').trim();
  return p.startsWith(prefix);
}

/**
 * First-touch style acquisition: visitors whose first_seen is in window, grouped by UTM dimensions.
 * Lead counts matched on utm_source + utm_medium + utm_campaign only.
 */
export async function adminAcquisitionAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const win = `and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;

  let vUrl = `visitors?select=session_id,landing_path,utm_source,utm_medium,utm_campaign,utm_content,utm_term&${win}&limit=10000`;
  const fSrc = strTrim(q.utm_source, 128);
  const fMed = strTrim(q.utm_medium, 128);
  const fCamp = strTrim(q.utm_campaign, 256);
  if (fSrc) vUrl += `&utm_source=eq.${encodeURIComponent(fSrc)}`;
  if (fMed) vUrl += `&utm_medium=eq.${encodeURIComponent(fMed)}`;
  if (fCamp) vUrl += `&utm_campaign=ilike.*${encodeURIComponent(fCamp)}*`;

  const pathPrefix = strTrim(q.path_prefix, 500);
  const vRes = await supabaseRequest(cfg, 'GET', vUrl);
  if (vRes.code < 200 || vRes.code >= 300) {
    return { ok: false, error: (vRes.body || '').slice(0, 400) };
  }
  let vRows = JSON.parse(vRes.body || '[]');
  if (!Array.isArray(vRows)) vRows = [];
  if (pathPrefix) {
    vRows = vRows.filter((r) => landingMatches(r.landing_path, pathPrefix));
  }

  const map = new Map();
  for (const r of vRows) {
    const key = [
      normDim(r.utm_source),
      normDim(r.utm_medium),
      normDim(r.utm_campaign),
      normDim(r.utm_content),
      normDim(r.utm_term),
    ].join('\t');
    let o = map.get(key);
    if (!o) {
      o = {
        utm_source: normDim(r.utm_source) === '(not set)' ? null : r.utm_source,
        utm_medium: normDim(r.utm_medium) === '(not set)' ? null : r.utm_medium,
        utm_campaign: normDim(r.utm_campaign) === '(not set)' ? null : r.utm_campaign,
        utm_content: normDim(r.utm_content) === '(not set)' ? null : r.utm_content,
        utm_term: normDim(r.utm_term) === '(not set)' ? null : r.utm_term,
        newSessions: 0,
        sampleLandingPaths: new Set(),
      };
      map.set(key, o);
    }
    o.newSessions += 1;
    const lp = strTrim(r.landing_path, 500);
    if (lp && o.sampleLandingPaths.size < 5) o.sampleLandingPaths.add(lp);
  }

  const lWin = `and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;
  let lUrl = `leads?select=id,session_id,converted_order_id,landing_path,utm_source,utm_medium,utm_campaign&${lWin}&limit=10000`;
  if (fSrc) lUrl += `&utm_source=eq.${encodeURIComponent(fSrc)}`;
  if (fMed) lUrl += `&utm_medium=eq.${encodeURIComponent(fMed)}`;
  if (fCamp) lUrl += `&utm_campaign=ilike.*${encodeURIComponent(fCamp)}*`;

  const lRes = await supabaseRequest(cfg, 'GET', lUrl);
  let lRows = [];
  if (lRes.code >= 200 && lRes.code < 300) {
    lRows = JSON.parse(lRes.body || '[]');
    if (!Array.isArray(lRows)) lRows = [];
  }
  if (pathPrefix) {
    lRows = lRows.filter((r) => landingMatches(r.landing_path, pathPrefix));
  }

  const leadBy3 = new Map();
  for (const r of lRows) {
    const k3 = utm3Key(r);
    let o = leadBy3.get(k3);
    if (!o) {
      o = { leads: 0, leadsConverted: 0 };
      leadBy3.set(k3, o);
    }
    o.leads += 1;
    if (r.converted_order_id) o.leadsConverted += 1;
  }

  const rows = [...map.values()].map((o) => {
    const k3 = [normDim(o.utm_source), normDim(o.utm_medium), normDim(o.utm_campaign)].join('\t');
    const lm = leadBy3.get(k3) || { leads: 0, leadsConverted: 0 };
    return {
      utm_source: o.utm_source,
      utm_medium: o.utm_medium,
      utm_campaign: o.utm_campaign,
      utm_content: o.utm_content,
      utm_term: o.utm_term,
      newVisitorSessions: o.newSessions,
      leadsFirstSeenInWindow: lm.leads,
      leadsConvertedInWindow: lm.leadsConverted,
      sampleLandingPaths: [...o.sampleLandingPaths],
    };
  });

  rows.sort((a, b) => b.newVisitorSessions - a.newVisitorSessions);

  return {
    ok: true,
    preset: w.label,
    periodStart: startIso,
    periodEnd: endIso,
    filters: {
      utm_source: fSrc || null,
      utm_medium: fMed || null,
      utm_campaign: fCamp || null,
      path_prefix: pathPrefix || null,
    },
    truncatedVisitors: vRows.length >= 10000,
    truncatedLeads: lRows.length >= 10000,
    rows,
    note: 'newVisitorSessions = visitors with first_seen in window. Leads matched on source+medium+campaign only.',
  };
}
