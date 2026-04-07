import { supabaseRequest } from './supabase.js';
import { resolveAnalyticsWindow } from './admin.js';
import { loadFunnelDefinitions } from './funnelsStore.js';

function normalizePath(p) {
  let s = String(p || '').trim();
  if (!s) return '';
  try {
    s = decodeURIComponent(s);
  } catch {
    /* ignore */
  }
  const q = s.indexOf('?');
  if (q >= 0) s = s.slice(0, q);
  if (s.length > 1 && s.endsWith('/')) s = s.slice(0, -1);
  return s || '/';
}

function pathMatches(kind, pattern, pathNorm) {
  if (!pattern) return false;
  if (kind === 'page_exact') return pathNorm === normalizePath(pattern);
  if (kind === 'page_prefix') {
    const pref = normalizePath(pattern);
    return pathNorm === pref || pathNorm.startsWith(pref.endsWith('/') ? pref.slice(0, -1) : pref) || pathNorm.startsWith(pref + '/');
  }
  return false;
}

function eventMatches(kind, pattern, eventName) {
  if (kind !== 'event_name') return false;
  return String(eventName || '').trim() === String(pattern || '').trim();
}

function stepSatisfied(step, pathNorm, eventName) {
  if (step.kind === 'event_name') return eventMatches('event_name', step.value, eventName);
  return pathMatches(step.kind, step.value, pathNorm);
}

/**
 * Fetch merged timeline per session_id from both event tables in date range.
 */
async function fetchSessionEvents(cfg, startIso, endIso, maxRowsPerTable = 12000) {
  const win = `and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)})`;
  const bySession = new Map();

  function ingest(rows, source) {
    if (!Array.isArray(rows)) return;
    for (const r of rows) {
      const sid = String(r.session_id || '').trim();
      if (!sid) continue;
      let arr = bySession.get(sid);
      if (!arr) {
        arr = [];
        bySession.set(sid, arr);
      }
      arr.push({
        t: new Date(r.created_at || 0).getTime(),
        path: r.path,
        event_name: r.event_name,
        event_type: r.event_type,
        _source: source,
      });
    }
  }

  for (const table of ['visitor_events', 'lead_events']) {
    let offset = 0;
    const limit = 2000;
    let got = 0;
    while (got < maxRowsPerTable) {
      const url = `${table}?select=session_id,path,event_name,event_type,created_at&${win}&order=created_at.asc&limit=${limit}&offset=${offset}`;
      const res = await supabaseRequest(cfg, 'GET', url);
      if (res.code < 200 || res.code >= 300) break;
      const rows = JSON.parse(res.body || '[]');
      if (!Array.isArray(rows) || rows.length === 0) break;
      ingest(rows, table === 'visitor_events' ? 'visitor' : 'lead');
      got += rows.length;
      offset += limit;
      if (rows.length < limit) break;
    }
  }

  for (const arr of bySession.values()) {
    arr.sort((a, b) => a.t - b.t);
  }
  return bySession;
}

function funnelPassForSession(events, steps) {
  if (!steps.length) return { reached: 0, completed: false };
  let stepIdx = 0;
  for (const ev of events) {
    const pathNorm = normalizePath(ev.path);
    const en = ev.event_name;
    const st = steps[stepIdx];
    if (stepSatisfied(st, pathNorm, en)) {
      stepIdx += 1;
      if (stepIdx >= steps.length) return { reached: steps.length, completed: true };
    }
  }
  return { reached: stepIdx, completed: false };
}

export async function adminFunnelAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const funnelId = String(q.funnel_id || q.id || '').trim();
  const { funnels } = loadFunnelDefinitions();
  const funnel = funnels.find((f) => f.id === funnelId && f.enabled !== false);
  if (!funnel || !funnel.steps?.length) {
    return { ok: false, error: 'Unknown or disabled funnel (use funnel_id)' };
  }

  const bySession = await fetchSessionEvents(cfg, w.startIso, w.endIso);
  const stepCounts = funnel.steps.map(() => 0);
  let completed = 0;

  for (const events of bySession.values()) {
    const { reached, completed: done } = funnelPassForSession(events, funnel.steps);
    for (let i = 0; i < reached; i += 1) {
      stepCounts[i] += 1;
    }
    if (done) completed += 1;
  }

  const stepsOut = funnel.steps.map((s, i) => ({
    index: i,
    kind: s.kind,
    value: s.value,
    sessionsReached: stepCounts[i],
    dropOff:
      i === 0
        ? null
        : stepCounts[i - 1] > 0
          ? Math.round((1 - stepCounts[i] / stepCounts[i - 1]) * 1000) / 10
          : null,
  }));

  return {
    ok: true,
    funnelId: funnel.id,
    funnelName: funnel.name,
    preset: w.label,
    periodStart: w.startIso,
    periodEnd: w.endIso,
    sessionsInSample: bySession.size,
    sessionsCompletedFunnel: completed,
    completionRatePercent:
      bySession.size > 0 ? Math.round((completed / bySession.size) * 1000) / 10 : 0,
    steps: stepsOut,
    note: 'Sessions counted if any event in range; step order is chronological per session.',
  };
}

export function adminListFunnels() {
  const { funnels } = loadFunnelDefinitions();
  return {
    ok: true,
    funnels: funnels.map((f) => ({
      id: f.id,
      name: f.name,
      enabled: f.enabled !== false,
      stepCount: Array.isArray(f.steps) ? f.steps.length : 0,
    })),
  };
}
