import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FUNNELS_FILE = path.join(__dirname, '..', 'data', 'funnels.json');

const DEFAULT_FUNNELS = [
  {
    id: 'kundli_checkout',
    name: 'Kundli LP → checkout',
    enabled: true,
    steps: [
      { kind: 'page_prefix', value: '/lp/kundli' },
      { kind: 'page_prefix', value: '/lp/kundli-checkout' },
    ],
  },
  {
    id: 'site_to_kundli',
    name: 'Home → Kundli',
    enabled: true,
    steps: [
      { kind: 'page_exact', value: '/' },
      { kind: 'page_prefix', value: '/kundli' },
    ],
  },
];

function ensureDataDir() {
  const dir = path.dirname(FUNNELS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { mode: 0o700 });
}

export function loadFunnelDefinitions() {
  try {
    if (!fs.existsSync(FUNNELS_FILE)) {
      return { funnels: JSON.parse(JSON.stringify(DEFAULT_FUNNELS)) };
    }
    const raw = fs.readFileSync(FUNNELS_FILE, 'utf8');
    const j = JSON.parse(raw);
    const list = Array.isArray(j.funnels) ? j.funnels : [];
    if (list.length === 0) return { funnels: JSON.parse(JSON.stringify(DEFAULT_FUNNELS)) };
    return { funnels: list };
  } catch {
    return { funnels: JSON.parse(JSON.stringify(DEFAULT_FUNNELS)) };
  }
}

export function saveFunnelDefinitions(body) {
  ensureDataDir();
  const funnels = Array.isArray(body?.funnels) ? body.funnels : [];
  const cleaned = funnels.map((f, i) => ({
    id: String(f.id || `funnel_${i}`).replace(/[^a-z0-9_-]/gi, '_').slice(0, 64),
    name: String(f.name || 'Untitled').slice(0, 200),
    enabled: Boolean(f.enabled),
    steps: Array.isArray(f.steps)
      ? f.steps
          .map((s) => ({
            kind: ['page_prefix', 'page_exact', 'event_name'].includes(s.kind) ? s.kind : 'page_prefix',
            value: String(s.value || '').slice(0, 500),
          }))
          .filter((s) => s.value)
      : [],
  }));
  fs.writeFileSync(FUNNELS_FILE, JSON.stringify({ funnels: cleaned }, null, 2), { mode: 0o600 });
  return { ok: true, funnels: cleaned };
}
