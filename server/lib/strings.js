export function strTrim(v, max) {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  if (!s) return null;
  return [...s].slice(0, max).join('');
}

export function uuidOk(s) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(s || '')
  );
}
