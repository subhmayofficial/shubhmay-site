/**
 * PostgREST (Supabase) client — mirrors api/inc/Supabase.php
 */

export function supabaseHeaders(cfg, withContentProfile = true) {
  const key = cfg.serviceRoleKey;
  const schema = cfg.schema || 'public';
  const h = {
    apikey: key,
    Authorization: `Bearer ${key}`,
    'Content-Type': 'application/json',
  };
  if (withContentProfile && schema && schema !== 'public') {
    h['Accept-Profile'] = schema;
    h['Content-Profile'] = schema;
  }
  return h;
}

export async function supabaseRequest(cfg, method, relativePath, jsonBody = null, prefer = null) {
  const base = cfg.supabaseUrl.replace(/\/$/, '');
  const url = `${base}/rest/v1/${relativePath.replace(/^\//, '')}`;
  const headers = { ...supabaseHeaders(cfg) };
  if (prefer) headers.Prefer = prefer;
  const opts = {
    method,
    headers,
    signal: AbortSignal.timeout(25_000),
  };
  if (jsonBody != null && method !== 'GET' && method !== 'HEAD') {
    opts.body = jsonBody;
  }
  const res = await fetch(url, opts);
  const body = await res.text();
  return { code: res.status, body };
}

export function parseContentRangeTotal(contentRange) {
  if (!contentRange) return null;
  const m = String(contentRange).trim().match(/\/(\d+)$/);
  return m ? parseInt(m[1], 10) : null;
}

export async function supabaseGetRange(cfg, pathWithQuery, rangeSpec) {
  const base = cfg.supabaseUrl.replace(/\/$/, '');
  const url = `${base}/rest/v1/${pathWithQuery.replace(/^\//, '')}`;
  const headers = {
    ...supabaseHeaders(cfg),
    Range: rangeSpec,
    Prefer: 'count=exact',
  };
  const res = await fetch(url, {
    method: 'GET',
    headers,
    signal: AbortSignal.timeout(45_000),
  });
  const contentRange = res.headers.get('content-range');
  const body = await res.text();
  return { code: res.status, body, contentRange };
}

export async function supabaseCountRows(cfg, pathWithQuery) {
  const r = await supabaseGetRange(cfg, pathWithQuery, '0-0');
  if (r.code < 200 || r.code >= 300) return 0;
  return parseContentRangeTotal(r.contentRange) ?? 0;
}
