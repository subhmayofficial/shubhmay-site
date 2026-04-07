export function strTrim(v, max) {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;
  return [...s].slice(0, max).join('');
}

export function normalizeEmail(v) {
  const s = strTrim(v, 320);
  return s ? s.toLowerCase() : null;
}

export function normalizePhone(v) {
  const s = strTrim(v, 32);
  if (!s) return null;
  let digits = s.replace(/\D/g, '');
  if (!digits) return null;
  if (digits.length === 12 && digits.startsWith('91')) digits = digits.slice(2);
  if (digits.length === 11 && digits.startsWith('0')) digits = digits.slice(1);
  return digits || null;
}

export function uuidOk(s) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(s || '')
  );
}
