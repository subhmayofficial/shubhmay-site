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

export async function adminPageAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;

  const andRange = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)})`;
  const vePath = `visitor_events?select=path,session_id,created_at,event_type&${andRange}&limit=10000`;
  const lePath = `lead_events?select=path,session_id,created_at,event_type&${andRange}&limit=10000`;

  const ve = await supabaseRequest(cfg, 'GET', vePath);
  const le = await supabaseRequest(cfg, 'GET', lePath);

  const byPath = [];
  const map = new Map();

  function addRow(pathRaw, sessionId, createdAt) {
    const path = normalizePath(pathRaw) || '(unknown)';
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
        if (r.path) addRow(r.path, r.session_id, r.created_at);
      }
    }
  }
  if (le.code >= 200 && le.code < 300) {
    const rows = JSON.parse(le.body || '[]');
    if (Array.isArray(rows)) {
      for (const r of rows) {
        if (r.path) addRow(r.path, r.session_id, r.created_at);
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

  const truncated = (ve.code >= 200 && ve.body?.length > 100000) || (le.code >= 200 && le.body?.length > 100000);

  return {
    ok: true,
    preset: w.label,
    periodStart: startIso,
    periodEnd: endIso,
    timezone: ADMIN_TZ,
    pages: byPath,
    totalEvents: byPath.reduce((s, x) => s + x.events, 0),
    truncated: Boolean(truncated),
  };
}

export async function adminPageAnalyticsDetail(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const rawPath = strTrim(q.path, 2000);
  if (!rawPath) return { ok: false, error: 'path query required' };
  const normalized = normalizePath(rawPath) || rawPath;
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const andRange = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)})`;

  const vePath = `visitor_events?select=path,session_id,created_at,event_type,event_name,meta&${andRange}&path=eq.${encodeURIComponent(normalized)}&limit=5000`;
  const lePath = `lead_events?select=path,session_id,created_at,event_type,event_name,meta&${andRange}&path=eq.${encodeURIComponent(normalized)}&limit=5000`;

  const ve = await supabaseRequest(cfg, 'GET', vePath);
  const le = await supabaseRequest(cfg, 'GET', lePath);

  const byDay = new Map();
  const sessions = new Set();
  let total = 0;

  function ingest(rows) {
    if (!Array.isArray(rows)) return;
    for (const r of rows) {
      total += 1;
      if (r.session_id) sessions.add(r.session_id);
      const d = dayKeyIst(r.created_at);
      byDay.set(d, (byDay.get(d) || 0) + 1);
    }
  }

  if (ve.code >= 200 && ve.code < 300) ingest(JSON.parse(ve.body || '[]'));
  if (le.code >= 200 && le.code < 300) ingest(JSON.parse(le.body || '[]'));

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
  };
}
