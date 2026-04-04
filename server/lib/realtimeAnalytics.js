import { supabaseRequest } from './supabase.js';

const MAX_EVENTS = 80;
const MAX_FETCH = 4000;

/**
 * Active sessions + recent events in the last N minutes (both visitor_events and lead_events).
 */
export async function adminRealtimeAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const minutes = Math.min(120, Math.max(5, parseInt(String(q.window_minutes ?? '30'), 10) || 30));
  const since = new Date(Date.now() - minutes * 60 * 1000).toISOString();
  const win = `and=(created_at.gte.${encodeURIComponent(since)})`;

  const sessions = new Set();
  const events = [];

  async function pull(table, label) {
    const url = `${table}?select=session_id,event_name,event_type,path,created_at&${win}&order=created_at.desc&limit=${MAX_FETCH}`;
    const r = await supabaseRequest(cfg, 'GET', url);
    if (r.code < 200 || r.code >= 300) return;
    const rows = JSON.parse(r.body || '[]');
    if (!Array.isArray(rows)) return;
    for (const row of rows) {
      const sid = String(row.session_id || '').trim();
      if (sid) sessions.add(sid);
      events.push({
        _source: label,
        session_id: sid || null,
        event_name: row.event_name || null,
        event_type: row.event_type || null,
        path: row.path || null,
        created_at: row.created_at || null,
      });
    }
  }

  await Promise.all([pull('visitor_events', 'visitor'), pull('lead_events', 'lead')]);

  events.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
  const recentEvents = events.slice(0, MAX_EVENTS);

  return {
    ok: true,
    windowMinutes: minutes,
    sinceIso: since,
    activeSessions: sessions.size,
    recentEvents,
    truncated: events.length >= MAX_FETCH * 2,
  };
}
