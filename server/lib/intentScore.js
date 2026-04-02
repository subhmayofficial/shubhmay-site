/** Lead intent scoring — server-side only. */

export const INTENT_SCORE_FIRST_VISIT = 10;
export const INTENT_SCORE_CONTACT = 20;
export const INTENT_SCORE_NEW_PAGE = 5;

export function intentTierFromScore(score) {
  const n = Number(score) || 0;
  if (n >= 50) return 'high';
  if (n >= 25) return 'medium';
  return 'low';
}

export function normalizePathForIntent(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  try {
    const u = s.startsWith('http') ? new URL(s) : new URL(s, 'https://placeholder.local');
    let p = u.pathname || '/';
    if (p.length > 1 && p.endsWith('/')) p = p.slice(0, -1);
    return p || '/';
  } catch {
    let p = s.split('?')[0].split('#')[0] || '';
    if (p.length > 1 && p.endsWith('/')) p = p.slice(0, -1);
    return p || '/';
  }
}

function ensureIntent(meta) {
  const m = meta && typeof meta === 'object' ? { ...meta } : {};
  const intent = m.intent && typeof m.intent === 'object' ? { ...m.intent } : {};
  if (!Array.isArray(intent.paths)) intent.paths = [];
  m.intent = intent;
  return m;
}

export function applyIntentFirstVisit(existingScore, meta) {
  const m = ensureIntent(meta);
  if (m.intent.first_visit_scored) {
    return { score: existingScore, meta: m, changed: false, reasons: [] };
  }
  m.intent.first_visit_scored = true;
  return {
    score: existingScore + INTENT_SCORE_FIRST_VISIT,
    meta: m,
    changed: true,
    reasons: ['first_visit'],
  };
}

export function applyIntentContact(existingScore, meta) {
  const m = ensureIntent(meta);
  if (m.intent.contact_scored) {
    return { score: existingScore, meta: m, changed: false, reasons: [] };
  }
  m.intent.contact_scored = true;
  return {
    score: existingScore + INTENT_SCORE_CONTACT,
    meta: m,
    changed: true,
    reasons: ['contact'],
  };
}

export function applyIntentNewPage(existingScore, meta, rawPath) {
  const norm = normalizePathForIntent(rawPath);
  if (!norm) {
    return { score: existingScore, meta: ensureIntent(meta), changed: false, reasons: [] };
  }
  const m = ensureIntent(meta);
  if (m.intent.paths.includes(norm)) {
    return { score: existingScore, meta: m, changed: false, reasons: [] };
  }
  m.intent.paths = [...m.intent.paths, norm];
  return {
    score: existingScore + INTENT_SCORE_NEW_PAGE,
    meta: m,
    changed: true,
    reasons: ['new_page:' + norm],
  };
}
