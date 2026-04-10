import { DateTime } from 'luxon';
import { supabaseRequest } from './supabase.js';
import { resolveAnalyticsWindow } from './admin.js';
import { strTrim } from './strings.js';

const ADMIN_TZ = 'Asia/Kolkata';

/** Known static routes for friendly labels (extend as you add pages). */
export const SITE_PAGE_LABELS = {
  '/': 'Home',
  '/index.html': 'Home',
  '/consultancy.html': 'Consultancy',
  '/consultancy-checkout.html': 'Consultancy checkout',
  '/kundli': 'Kundli',
  '/kundli.html': 'Kundli',
  '/lp/kundli': 'Kundli LP',
  '/kundli-preview': 'Kundli preview',
  '/kundli-preview.html': 'Kundli preview',
  '/contact-us.html': 'Contact',
  '/contact': 'Contact',
  '/metal/index.html': 'Metal finder',
  '/tools/metal': 'Metal finder',
  '/free-metal-finder-tool.html': 'Metal finder',
  '/metal-finder': 'Metal finder',
  '/ringdhantu': 'Ring dhatu tool',
  '/ringdhantu.html': 'Ring dhatu tool',
  '/freevedictools.html': 'Free tools',
  '/mahamantra.html': 'Mahamantra',
  '/tracking': 'Tracking test',
  '/tracking.html': 'Tracking test',
  '/lp/kundli-checkout/': 'LP Kundli checkout',
};

function dayKeyIst(iso) {
  try {
    return DateTime.fromISO(iso, { setZone: true }).setZone(ADMIN_TZ).toFormat('yyyy-MM-dd');
  } catch {
    return String(iso || '').slice(0, 10);
  }
}

function normalizePath(p) {
  const s = strTrim(p, 2000) || '';
  if (!s) return null;
  try {
    const u = s.startsWith('http') ? new URL(s) : s;
    if (typeof u === 'string') {
      const path = u.split('?')[0].split('#')[0];
      return path || '/';
    }
    return u.pathname || '/';
  } catch {
    return s.split('?')[0] || '/';
  }
}

function stripTrailingSlash(p) {
  if (!p || p === '/') return p;
  return p.replace(/\/$/, '') || '/';
}

/**
 * Map alternate URLs to one row (matches how GA4 groups "same" page: /x vs /x.html).
 */
const PATH_ALIAS_TO_CANONICAL = new Map([
  ['/free-metal-finder-tool', '/free-metal-finder-tool.html'],
  ['/metal-finder', '/free-metal-finder-tool.html'],
  ['/tools/metal', '/free-metal-finder-tool.html'],
  ['/kundli', '/kundli.html'],
  ['/kundli-preview', '/kundli-preview.html'],
  ['/consultancy', '/consultancy.html'],
  ['/contact', '/contact-us.html'],
  ['/tracking', '/tracking.html'],
  ['/lp/kundli', '/lp/kundli.html'],
]);

export function canonicalPathForAnalytics(pathNorm) {
  if (!pathNorm || pathNorm === '(unknown)') return pathNorm;
  const p = stripTrailingSlash(pathNorm);
  const pl = p.toLowerCase();
  if (PATH_ALIAS_TO_CANONICAL.has(p)) return PATH_ALIAS_TO_CANONICAL.get(p);
  if (PATH_ALIAS_TO_CANONICAL.has(pl)) return PATH_ALIAS_TO_CANONICAL.get(pl);
  for (const [alias, canon] of PATH_ALIAS_TO_CANONICAL) {
    if (p === canon || pl === canon.toLowerCase()) return canon;
    if (p === alias || pl === alias.toLowerCase()) return canon;
  }
  return pathNorm;
}

/** Paths to match in DB when user opens detail for one canonical URL. */
export function pathVariantsForDetail(canonicalPath) {
  const out = new Set([canonicalPath]);
  for (const [alias, canon] of PATH_ALIAS_TO_CANONICAL) {
    if (canon === canonicalPath || alias === canonicalPath) {
      out.add(alias);
      out.add(canon);
    }
  }
  return [...out];
}

const PAGE_VIEW_FILTER = 'event_name.eq.page_view';
const PAGE_ROWS_CAP = 15000;

export async function adminPageAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const pathPrefixFilter = strTrim(q.path_prefix, 500);

  const andRange = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},${PAGE_VIEW_FILTER})`;
  const vePath = `visitor_events?select=path,session_id,created_at,event_name&${andRange}&limit=${PAGE_ROWS_CAP}`;
  const lePath = `lead_events?select=path,session_id,created_at,event_name&${andRange}&limit=${PAGE_ROWS_CAP}`;

  const ve = await supabaseRequest(cfg, 'GET', vePath);
  const le = await supabaseRequest(cfg, 'GET', lePath);

  const byPath = [];
  const map = new Map();

  function addRow(pathRaw, sessionId, createdAt) {
    const raw = normalizePath(pathRaw) || '(unknown)';
    const path = canonicalPathForAnalytics(raw);
    if (pathPrefixFilter && !String(path).startsWith(pathPrefixFilter)) return;
    const day = dayKeyIst(createdAt);
    const key = path;
    if (!map.has(key)) {
      map.set(key, {
        path: key,
        label: SITE_PAGE_LABELS[key] || SITE_PAGE_LABELS[key.replace(/\/$/, '')] || key,
        events: 0,
        sessions: new Set(),
        byDay: new Map(),
      });
    }
    const o = map.get(key);
    o.events += 1;
    if (sessionId) o.sessions.add(sessionId);
    o.byDay.set(day, (o.byDay.get(day) || 0) + 1);
  }

  if (ve.code >= 200 && ve.code < 300) {
    const rows = JSON.parse(ve.body || '[]');
    if (Array.isArray(rows)) {
      for (const r of rows) {
        if (r.path && String(r.event_name || '').trim() === 'page_view') addRow(r.path, r.session_id, r.created_at);
      }
    }
  }
  if (le.code >= 200 && le.code < 300) {
    const rows = JSON.parse(le.body || '[]');
    if (Array.isArray(rows)) {
      for (const r of rows) {
        if (r.path && String(r.event_name || '').trim() === 'page_view') addRow(r.path, r.session_id, r.created_at);
      }
    }
  }

  for (const v of map.values()) {
    const byDayArr = [...v.byDay.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, count]) => ({ date, count }));
    byPath.push({
      path: v.path,
      label: v.label,
      events: v.events,
      uniqueSessions: v.sessions.size,
      byDay: byDayArr,
    });
  }

  byPath.sort((a, b) => b.events - a.events);

  let truncated = false;
  if (ve.code >= 200 && ve.code < 300) {
    const rows = JSON.parse(ve.body || '[]');
    if (Array.isArray(rows) && rows.length >= PAGE_ROWS_CAP) truncated = true;
  }
  if (le.code >= 200 && le.code < 300) {
    const rows = JSON.parse(le.body || '[]');
    if (Array.isArray(rows) && rows.length >= PAGE_ROWS_CAP) truncated = true;
  }

  return {
    ok: true,
    preset: w.label,
    periodStart: startIso,
    periodEnd: endIso,
    timezone: ADMIN_TZ,
    pages: byPath,
    totalEvents: byPath.reduce((s, x) => s + x.events, 0),
    truncated,
    metric: 'page_view',
    metricNote: 'Views = page_view events only (aligned with GA4 “Views”), not every tracked event.',
  };
}

export async function adminPageAnalyticsDetail(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const rawPath = strTrim(q.path, 2000);
  if (!rawPath) return { ok: false, error: 'path query required' };
  const normalized = canonicalPathForAnalytics(normalizePath(rawPath) || rawPath);
  const variants = pathVariantsForDetail(normalized);
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const andRange = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},${PAGE_VIEW_FILTER})`;

  const byDay = new Map();
  const sessions = new Set();
  let total = 0;

  function ingest(rows) {
    if (!Array.isArray(rows)) return;
    for (const r of rows) {
      if (String(r.event_name || '').trim() !== 'page_view') continue;
      total += 1;
      if (r.session_id) sessions.add(r.session_id);
      const d = dayKeyIst(r.created_at);
      byDay.set(d, (byDay.get(d) || 0) + 1);
    }
  }

  for (const pv of variants) {
    const peq = `path=eq.${encodeURIComponent(pv)}`;
    const vePath = `visitor_events?select=path,session_id,created_at,event_name&${andRange}&${peq}&limit=5000`;
    const lePath = `lead_events?select=path,session_id,created_at,event_name&${andRange}&${peq}&limit=5000`;
    const ve = await supabaseRequest(cfg, 'GET', vePath);
    const le = await supabaseRequest(cfg, 'GET', lePath);
    if (ve.code >= 200 && ve.code < 300) ingest(JSON.parse(ve.body || '[]'));
    if (le.code >= 200 && le.code < 300) ingest(JSON.parse(le.body || '[]'));
  }

  const byDayArr = [...byDay.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, count]) => ({ date, count }));

  return {
    ok: true,
    path: normalized,
    label: SITE_PAGE_LABELS[normalized] || normalized,
    periodStart: startIso,
    periodEnd: endIso,
    totalEvents: total,
    uniqueSessions: sessions.size,
    byDay: byDayArr,
    metric: 'page_view',
    pathVariants: variants,
  };
}

const SERIES_ROW_CAP = 15000;

/**
 * Daily page_view counts (visitor_events + lead_events), IST buckets.
 * For dashboard / traffic charts; respects path_prefix like adminPageAnalytics.
 */
export async function adminTrafficDailySeries(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const pathPrefixFilter = strTrim(q.path_prefix, 500);
  const andRange = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},event_name.eq.page_view)`;

  const byDay = new Map();
  let truncated = false;

  for (const table of ['visitor_events', 'lead_events']) {
    const url = `${table}?select=path,created_at&${andRange}&limit=${SERIES_ROW_CAP}&order=created_at.asc`;
    const res = await supabaseRequest(cfg, 'GET', url);
    if (res.code < 200 || res.code >= 300) continue;
    const rows = JSON.parse(res.body || '[]');
    if (!Array.isArray(rows)) continue;
    if (rows.length >= SERIES_ROW_CAP) truncated = true;
    for (const r of rows) {
      const raw = normalizePath(r.path) || '(unknown)';
      const path = canonicalPathForAnalytics(raw);
      if (pathPrefixFilter && !String(path).startsWith(pathPrefixFilter)) continue;
      const d = dayKeyIst(r.created_at);
      byDay.set(d, (byDay.get(d) || 0) + 1);
    }
  }

  const series = [...byDay.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, pageViews]) => ({ date, pageViews }));

  return {
    ok: true,
    preset: w.label,
    periodStart: startIso,
    periodEnd: endIso,
    timezone: ADMIN_TZ,
    series,
    truncated,
    metric: 'page_view',
  };
}
