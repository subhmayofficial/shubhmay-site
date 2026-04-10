import { supabaseRequest } from './supabase.js';
import { resolveAnalyticsWindow } from './admin.js';

const MAX_ROWS_PER_TABLE = 12000;
const FETCH_LIMIT = 2000;

function normalizePath(p) {
  let s = String(p || '').trim();
  try {
    s = decodeURIComponent(s);
  } catch {
    /* ignore */
  }
  const q = s.indexOf('?');
  if (q >= 0) s = s.slice(0, q);
  const h = s.indexOf('#');
  if (h >= 0) s = s.slice(0, h);
  if (s.length > 1 && s.endsWith('/')) s = s.slice(0, -1);
  return s || '/';
}

function isRingDhatuPage(pathNorm) {
  const pl = pathNorm.toLowerCase();
  return pl === '/ringdhantu' || pl === '/ringdhantu.html';
}

function parseMeta(m) {
  if (m && typeof m === 'object') return m;
  if (typeof m === 'string') {
    try {
      return JSON.parse(m);
    } catch {
      return {};
    }
  }
  return {};
}

async function fetchRingDhatuEventRows(cfg, startIso, endIso) {
  const win = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},event_type.eq.ring_dhatu)`;
  const out = [];
  let truncated = false;
  for (const table of ['visitor_events', 'lead_events']) {
    let offset = 0;
    let got = 0;
    while (got < MAX_ROWS_PER_TABLE) {
      const url = `${table}?select=session_id,event_name,path,meta,created_at&${win}&order=created_at.asc&limit=${FETCH_LIMIT}&offset=${offset}`;
      const res = await supabaseRequest(cfg, 'GET', url);
      if (res.code < 200 || res.code >= 300) break;
      const batch = JSON.parse(res.body || '[]');
      if (!Array.isArray(batch) || batch.length === 0) break;
      for (const r of batch) out.push(r);
      got += batch.length;
      offset += batch.length;
      if (got >= MAX_ROWS_PER_TABLE) {
        truncated = true;
        break;
      }
      if (batch.length < FETCH_LIMIT) break;
    }
  }
  return { rows: out, truncated };
}

/**
 * Ring dhatu tool (/ringdhantu) — visits, metal checks, free kundli CTA clicks.
 * Uses event_type=ring_dhatu rows from visitor_events + lead_events (same session not duplicated across tables).
 */
export async function adminRingDhatuAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const { rows, truncated: fetchTruncated } = await fetchRingDhatuEventRows(cfg, w.startIso, w.endIso);

  const sessionsPageView = new Set();
  let eventsPageView = 0;
  const sessionsRashi = new Set();
  let eventsRashi = 0;
  const sessionsCta = new Set();
  let eventsCta = 0;
  const sessionsCtaMain = new Set();
  let eventsCtaMain = 0;
  const sessionsCtaSticky = new Set();
  let eventsCtaSticky = 0;

  for (const r of rows) {
    const sid = String(r.session_id || '').trim();
    const en = String(r.event_name || '').trim();
    const pathNorm = normalizePath(r.path);
    const meta = parseMeta(r.meta);

    if (en === 'page_view' && isRingDhatuPage(pathNorm)) {
      eventsPageView += 1;
      if (sid) sessionsPageView.add(sid);
      continue;
    }
    if (en === 'rashi_selected') {
      eventsRashi += 1;
      if (sid) sessionsRashi.add(sid);
      continue;
    }
    if (en === 'cta_click') {
      const cta = String(meta.cta || '').trim();
      if (cta === 'free_kundli_preview' || cta === 'sticky_free_kundli') {
        eventsCta += 1;
        if (sid) sessionsCta.add(sid);
        if (cta === 'free_kundli_preview') {
          eventsCtaMain += 1;
          if (sid) sessionsCtaMain.add(sid);
        } else {
          eventsCtaSticky += 1;
          if (sid) sessionsCtaSticky.add(sid);
        }
      }
    }
  }

  const uniqueSessionsPage = sessionsPageView.size;
  const uniqueSessionsCheck = sessionsRashi.size;
  const uniqueSessionsCta = sessionsCta.size;
  let ctaRatePercent = null;
  if (uniqueSessionsPage > 0) {
    ctaRatePercent = Math.round((uniqueSessionsCta / uniqueSessionsPage) * 1000) / 10;
  }

  return {
    ok: true,
    preset: w.label,
    periodStart: w.startIso,
    periodEnd: w.endIso,
    toolPath: '/ringdhantu',
    /** Unique sessions with page_view on /ringdhantu */
    uniqueSessionsPageViews: uniqueSessionsPage,
    pageViewEvents: eventsPageView,
    /** Unique sessions that completed a rashi → metal “check” (result screen) */
    uniqueSessionsMetalCheck: uniqueSessionsCheck,
    metalCheckEvents: eventsRashi,
    /** Unique sessions that clicked Free kundli (main or sticky) */
    uniqueSessionsFreeKundliCta: uniqueSessionsCta,
    freeKundliCtaEvents: eventsCta,
    uniqueSessionsCtaMain: sessionsCtaMain.size,
    ctaMainEvents: eventsCtaMain,
    uniqueSessionsCtaSticky: sessionsCtaSticky.size,
    ctaStickyEvents: eventsCtaSticky,
    /** % of visiting sessions that clicked a free kundli CTA */
    ctaClickRatePercentVsVisits: ctaRatePercent,
    truncated: fetchTruncated,
    note:
      'Counts use event_type=ring_dhatu only. Visits = page_view on /ringdhantu. Check = rashi_selected. CTA = cta_click with free_kundli_preview or sticky_free_kundli.',
  };
}
