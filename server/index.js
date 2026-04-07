import path from 'path';
import { fileURLToPath } from 'url';
import express from 'express';
import cors from 'cors';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
import { randomBytes, timingSafeEqual } from 'crypto';
import { getConfig, invalidateConfigCache } from './lib/config.js';
import {
  hasPasswordFile,
  hashPassword,
  verifyPassword,
  readPasswordHash,
  savePasswordHash,
  createSessionToken,
  parseSessionFromRequest,
  rateLimitLogin,
  clientIp,
  buildSessionCookie,
  buildClearCookie,
} from './lib/adminAuth.js';
import { saveRuntimeSettings, loadRuntimeSettings, hasRuntimeSettingsFile } from './lib/runtimeSettings.js';
import { razorpayCreateOrder, verifyPaymentSignature, verifyWebhookSignature } from './lib/razorpay.js';
import { upsertPaidOrder } from './lib/orders.js';
import { strTrim } from './lib/strings.js';
import {
  handleTrackLead,
  handleTrackLeadEvent,
  handleTrackCheckoutEvent,
} from './lib/track.js';
import {
  handleConsultancyConfig,
  handleConsultancySlots,
  handleConsultancyOrder,
  handleConsultancyBook,
} from './lib/consultancy.js';
import {
  resolveAnalyticsWindow,
  adminListOrders,
  adminOrdersSummary,
  adminListCustomers,
  adminListProfiles,
  adminProfileDetail,
  adminListLeads,
  adminListAbandoned,
  adminUpdateOrderStatus,
  adminDeleteLead,
  adminOrderPrePurchaseTimeline,
  adminGetAnalytics,
  adminSaveAnalyticsSnapshot,
  adminListAnalyticsSnapshots,
  adminListVisitors,
  adminVisitorTimeline,
  adminAbandonedCheckoutContext,
  adminCustomerActivityTimeline,
  adminRebuildProfilesNow,
} from './lib/admin.js';
import { adminPageAnalytics, adminPageAnalyticsDetail, adminTrafficDailySeries } from './lib/pageAnalytics.js';
import { getConnectionsSnapshot, runConnectionTests } from './lib/connectionsHealth.js';
import { adminAcquisitionAnalytics } from './lib/acquisitionAnalytics.js';
import { adminRealtimeAnalytics } from './lib/realtimeAnalytics.js';
import { adminFunnelAnalytics, adminListFunnels } from './lib/funnelAnalytics.js';
import { saveFunnelDefinitions } from './lib/funnelsStore.js';

const app = express();
app.set('trust proxy', 1);
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Powered-By', 'VedGuide-Node-API');
  next();
});

app.use(
  cors({
    origin: '*',
    methods: ['GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'X-Admin-Secret', 'Authorization', 'X-Razorpay-Signature'],
  })
);

function adminSecretFromRequest(req) {
  const h = req.headers['x-admin-secret'];
  if (h) return String(h).trim();
  const auth = req.headers.authorization || '';
  const m = /^Bearer\s+(.+)/i.exec(auth);
  if (m) return m[1].trim();
  return '';
}

function secretsEqual(a, b) {
  try {
    const ba = Buffer.from(a, 'utf8');
    const bb = Buffer.from(b, 'utf8');
    return ba.length === bb.length && timingSafeEqual(ba, bb);
  } catch {
    return false;
  }
}

function requireAdmin(req, res) {
  if (parseSessionFromRequest(req)) {
    return true;
  }
  const c = getConfig();
  const secret = c.adminSecret;
  if (secret) {
    const sent = adminSecretFromRequest(req);
    if (secretsEqual(secret, sent)) {
      return true;
    }
  }
  if (!hasPasswordFile() && !secret) {
    res.status(503).json({ ok: false, error: 'Complete first-time setup in admin (Settings) or set ADMIN_SECRET' });
    return false;
  }
  res.status(401).json({ ok: false, error: 'Unauthorized' });
  return false;
}

/** Razorpay webhook — raw body for HMAC */
app.post('/api/webhooks/razorpay', express.raw({ type: 'application/json' }), async (req, res) => {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  const whSecret = getConfig().razorpayWebhookSecret;
  if (!whSecret) {
    res.status(503).json({ ok: false, error: 'Set RAZORPAY_WEBHOOK_SECRET in environment' });
    return;
  }
  const raw = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body || '');
  const sig = req.headers['x-razorpay-signature'] || '';
  if (!verifyWebhookSignature(raw, String(sig), whSecret)) {
    res.status(400).json({ ok: false, error: 'Invalid webhook signature' });
    return;
  }
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    res.status(400).json({ ok: false, error: 'Invalid JSON' });
    return;
  }
  const event = payload.event || '';
  if (event === 'payment.captured') {
    const payment = payload.payload?.payment?.entity;
    if (payment && typeof payment === 'object') {
      const oid = String(payment.order_id || '').trim();
      const pid = String(payment.id || '').trim();
      if (oid && pid) {
        const db = await upsertPaidOrder(getConfig(), oid, pid);
        if (!db.ok && !db.skipped) {
          console.error('[razorpay webhook] order upsert failed:', db.error);
        }
      }
    }
  }
  res.status(200).json({ ok: true, received: event });
});

app.use(express.json({ limit: '1mb' }));

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    supabase: {
      url: Boolean(getConfig().supabaseUrl),
      serviceRole: Boolean(getConfig().serviceRoleKey),
      anon: Boolean(getConfig().anonKey),
      schema: getConfig().schema || 'v2',
    },
    ordersDb: Boolean(getConfig().supabaseUrl && getConfig().serviceRoleKey),
    razorpay: Boolean(getConfig().razorpayKeyId && getConfig().razorpayKeySecret),
    adminApi: Boolean(getConfig().adminSecret || hasPasswordFile()),
    adminNeedsBootstrap: !hasPasswordFile(),
    runtimeSettingsFile: hasRuntimeSettingsFile(),
    runtime: 'node',
  });
});

app.get('/api/supabase-public.json', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (!getConfig().supabaseUrl || !getConfig().anonKey) {
    res.status(503).json({
      ok: false,
      error: 'Set SUPABASE_URL and SUPABASE_ANON_KEY in environment',
    });
    return;
  }
  res.json({ supabaseUrl: getConfig().supabaseUrl, supabaseAnonKey: getConfig().anonKey });
});

app.get('/api/checkout/kundli/config', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const amount = getConfig().kundliAmountPaise;
  const currency = getConfig().currency;
  const keyId = getConfig().razorpayKeyId;
  const maps = getConfig().googleMapsBrowserKey;
  const out = {
    productName: 'Personalized Premium Kundli Report',
    productSlug: 'premium_kundli_report',
    amountPaise: amount,
    amountRupees: amount / 100,
    currency,
    razorpayReady: Boolean(keyId && getConfig().razorpayKeySecret),
  };
  if (maps) out.googleMapsBrowserKey = maps;
  res.json(out);
});

app.post('/api/checkout/kundli/order', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (!getConfig().razorpayKeyId || !getConfig().razorpayKeySecret) {
    res.status(503).json({ ok: false, error: 'Razorpay not configured' });
    return;
  }
  const b = req.body || {};
  const name = String(b.name ?? '').trim();
  const email = String(b.email ?? '').trim();
  const phone = String(b.phone ?? '').replace(/\s+/g, '');
  const dob = String(b.dob ?? '').trim();
  const tob = String(b.tob ?? '').trim();
  const birthPlace = String(b.birth_place ?? '').trim();
  const language = String(b.language ?? '').trim();
  const coupon = String(b.coupon ?? '').trim();
  const leadId = String(b.lead_id ?? '').trim();
  const checkoutSessionId = String(b.checkout_session_id ?? '').trim();

  if (!name || !email || !phone) {
    res.status(400).json({ ok: false, error: 'name, email, and phone are required' });
    return;
  }
  if (!dob || !tob || !birthPlace) {
    res.status(400).json({
      ok: false,
      error: 'date of birth, time of birth, and birth place are required',
    });
    return;
  }

  const amount = getConfig().kundliAmountPaise;
  const currency = getConfig().currency;
  const receipt =
    'knd_' + Math.floor(Date.now() / 1000).toString(36) + '_' + randomBytes(3).toString('hex');

  const notes = {
    product: 'premium_kundli_report',
    customer_name: strTrim(name, 200),
    customer_email: strTrim(email, 200),
    customer_phone: strTrim(phone, 20),
    dob: strTrim(dob, 32),
    tob: strTrim(tob, 16),
    birth_place: strTrim(birthPlace, 200),
    language: strTrim(language, 32),
    coupon: strTrim(coupon, 64),
  };
  if (leadId) notes.lead_id = strTrim(leadId, 64);
  if (checkoutSessionId) notes.checkout_session_id = strTrim(checkoutSessionId, 128);

  try {
    const order = await razorpayCreateOrder(getConfig(), {
      amount,
      currency,
      receipt,
      notes,
    });
    res.json({
      ok: true,
      keyId: getConfig().razorpayKeyId,
      orderId: order.id,
      amount: order.amount,
      currency: order.currency,
      receipt: order.receipt,
      prefill: { name, email, contact: phone },
    });
  } catch (e) {
    res.status(502).json({ ok: false, error: e.message });
  }
});

app.post('/api/checkout/kundli/verify', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const secret = getConfig().razorpayKeySecret;
  if (!secret) {
    res.status(503).json({ ok: false, error: 'Razorpay not configured' });
    return;
  }
  const b = req.body || {};
  const orderId = String(b.razorpay_order_id ?? '').trim();
  const paymentId = String(b.razorpay_payment_id ?? '').trim();
  const signature = String(b.razorpay_signature ?? '').trim();
  if (!orderId || !paymentId || !signature) {
    res.status(400).json({ ok: false, error: 'Missing payment verification fields' });
    return;
  }
  if (!verifyPaymentSignature(orderId, paymentId, signature, secret)) {
    res.status(400).json({ ok: false, error: 'Invalid signature' });
    return;
  }
  const db = await upsertPaidOrder(getConfig(), orderId, paymentId);
  const out = {
    ok: true,
    razorpay_order_id: orderId,
    razorpay_payment_id: paymentId,
    message: 'Payment verified.',
    saved: db.ok,
    dbSkipped: Boolean(db.skipped),
  };
  if (!db.ok && !db.skipped && db.error) out.dbError = db.error;
  res.json(out);
});

app.post('/api/track/lead', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleTrackLead(getConfig(), req.body || {});
  res.status(r.status).json(r.json);
});

app.post('/api/track/lead-event', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleTrackLeadEvent(getConfig(), req.body || {});
  res.status(r.status).json(r.json);
});

app.post('/api/track/checkout-event', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleTrackCheckoutEvent(getConfig(), req.body || {});
  res.status(r.status).json(r.json);
});

app.get('/api/consultancy/config', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleConsultancyConfig(getConfig());
  res.status(r.status).json(r.json);
});

app.get('/api/consultancy/slots', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleConsultancySlots(getConfig());
  res.status(r.status).json(r.json);
});

app.post('/api/consultancy/order', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleConsultancyOrder(getConfig(), req.body || {});
  res.status(r.status).json(r.json);
});

app.post('/api/consultancy/book', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const r = await handleConsultancyBook(getConfig(), req.body || {});
  res.status(r.status).json(r.json);
});

/* —— Admin auth & encrypted settings —— */
app.get('/api/admin/auth/status', (req, res) => {
  const c = getConfig();
  res.json({
    ok: true,
    needsBootstrap: !hasPasswordFile(),
    authenticated: Boolean(parseSessionFromRequest(req)),
    legacySecretConfigured: Boolean(c.adminSecret),
    hasRuntimeSettings: hasRuntimeSettingsFile(),
  });
});

app.post('/api/admin/auth/bootstrap', (req, res) => {
  if (hasPasswordFile()) {
    res.status(400).json({ ok: false, error: 'Already initialized' });
    return;
  }
  const ip = clientIp(req);
  if (!rateLimitLogin(ip)) {
    res.status(429).json({ ok: false, error: 'Too many attempts — try later' });
    return;
  }
  const b = req.body || {};
  const p1 = String(b.password || '');
  const p2 = String(b.passwordConfirm || '');
  if (p1.length < 12 || p1 !== p2) {
    res.status(400).json({
      ok: false,
      error: 'Password must be at least 12 characters and match confirmation',
    });
    return;
  }
  const settings = {
    supabaseUrl: strTrim(b.supabaseUrl, 500),
    serviceRoleKey: strTrim(b.serviceRoleKey, 2000),
    anonKey: strTrim(b.anonKey, 2000),
    schema: strTrim(b.schema || 'v2', 64),
    razorpayKeyId: strTrim(b.razorpayKeyId, 128),
    razorpayKeySecret: strTrim(b.razorpayKeySecret, 2000),
    razorpayWebhookSecret: strTrim(b.razorpayWebhookSecret, 2000),
    kundliAmountPaise: parseInt(String(b.kundliAmountPaise || '49900'), 10),
    currency: strTrim(b.currency || 'INR', 8),
    googleMapsBrowserKey: strTrim(b.googleMapsBrowserKey, 256),
  };
  if (!settings.supabaseUrl || !settings.serviceRoleKey) {
    res.status(400).json({ ok: false, error: 'Supabase URL and service role key are required' });
    return;
  }
  try {
    savePasswordHash(hashPassword(p1));
    saveRuntimeSettings(settings);
    invalidateConfigCache();
    const token = createSessionToken();
    res.setHeader('Set-Cookie', buildSessionCookie(token));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'Save failed' });
  }
});

app.post('/api/admin/auth/login', (req, res) => {
  const ip = clientIp(req);
  if (!rateLimitLogin(ip)) {
    res.status(429).json({ ok: false, error: 'Too many attempts — try later' });
    return;
  }
  if (!hasPasswordFile()) {
    res.status(400).json({ ok: false, error: 'Complete bootstrap first' });
    return;
  }
  const pwd = String((req.body || {}).password || '');
  if (!verifyPassword(pwd, readPasswordHash())) {
    res.status(401).json({ ok: false, error: 'Invalid password' });
    return;
  }
  const token = createSessionToken();
  res.setHeader('Set-Cookie', buildSessionCookie(token));
  res.json({ ok: true });
});

app.post('/api/admin/auth/logout', (req, res) => {
  res.setHeader('Set-Cookie', buildClearCookie());
  res.json({ ok: true });
});

app.get('/api/admin/settings', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const c = getConfig();
  const mask = (s) => {
    if (!s || typeof s !== 'string') return '';
    if (s.length <= 8) return '••••••••';
    return s.slice(0, 4) + '…' + s.slice(-4);
  };
  res.json({
    ok: true,
    supabaseUrl: c.supabaseUrl,
    supabaseUrlMasked: mask(c.supabaseUrl),
    serviceRoleKeySet: Boolean(c.serviceRoleKey),
    anonKeySet: Boolean(c.anonKey),
    schema: c.schema || 'v2',
    razorpayKeyId: c.razorpayKeyId,
    razorpayKeyIdMasked: mask(c.razorpayKeyId || ''),
    razorpayKeySecretSet: Boolean(c.razorpayKeySecret),
    razorpayWebhookSecretSet: Boolean(c.razorpayWebhookSecret),
    kundliAmountPaise: c.kundliAmountPaise,
    currency: c.currency,
    googleMapsBrowserKeySet: Boolean(c.googleMapsBrowserKey),
    legacyAdminSecretSet: Boolean(c.adminSecret),
  });
});

app.get('/api/admin/connections', (req, res) => {
  if (!requireAdmin(req, res)) return;
  res.json({ ok: true, ...getConnectionsSnapshot(getConfig()) });
});

app.post('/api/admin/connections/test', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const which = (req.body && req.body.which) || 'all';
    const live = await runConnectionTests(getConfig(), which);
    res.json({ ok: true, ...getConnectionsSnapshot(getConfig()), live });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'Test failed' });
  }
});

app.post('/api/admin/profiles/rebuild-now', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminRebuildProfilesNow(getConfig());
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Rebuild failed' });
    return;
  }
  res.json({ ok: true });
});

app.post('/api/admin/settings', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const b = req.body || {};
  const current = loadRuntimeSettings() || {};
  const merged = { ...current };
  const fields = [
    'supabaseUrl',
    'serviceRoleKey',
    'anonKey',
    'schema',
    'razorpayKeyId',
    'razorpayKeySecret',
    'razorpayWebhookSecret',
    'kundliAmountPaise',
    'currency',
    'googleMapsBrowserKey',
    'adminSecret',
  ];
  for (const k of fields) {
    if (b[k] === undefined || b[k] === null) continue;
    const s = String(b[k]).trim();
    if (s === '') continue;
    if (k === 'kundliAmountPaise') {
      const n = parseInt(s, 10);
      if (!Number.isNaN(n)) merged[k] = n;
    } else {
      merged[k] = strTrim(s, 4000);
    }
  }
  try {
    saveRuntimeSettings(merged);
    invalidateConfigCache();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'Save failed' });
  }
});

/* —— Admin —— */
app.get('/api/admin/ping', (req, res) => {
  if (!requireAdmin(req, res)) return;
  res.json({ ok: true });
});

app.get('/api/admin/date-window', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const w = resolveAnalyticsWindow(req.query);
  res.json({
    ok: true,
    timezone: 'Asia/Kolkata',
    preset: w.label,
    startIso: w.startIso,
    endIso: w.endIso,
  });
});

app.get('/api/admin/analytics', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminGetAnalytics(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Analytics failed' });
    return;
  }
  res.json(out);
});

app.post('/api/admin/analytics/snapshot', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminSaveAnalyticsSnapshot(getConfig(), req.body || {});
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Snapshot failed' });
    return;
  }
  res.json({ ok: true });
});

app.get('/api/admin/analytics/snapshots', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListAnalyticsSnapshots(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({ ok: true, snapshots: out.rows });
});

app.get('/api/admin/analytics/pages/detail', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminPageAnalyticsDetail(getConfig(), req.query);
  if (!out.ok) {
    res.status(400).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/analytics/pages', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminPageAnalytics(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/analytics/traffic-series', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminTrafficDailySeries(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Series failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/analytics/acquisition', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminAcquisitionAnalytics(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Acquisition analytics failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/analytics/realtime', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminRealtimeAnalytics(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Realtime failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/analytics/funnel', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminFunnelAnalytics(getConfig(), req.query);
  if (!out.ok) {
    res.status(400).json({ ok: false, error: out.error ?? 'Funnel failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/funnels', (req, res) => {
  if (!requireAdmin(req, res)) return;
  res.json(adminListFunnels());
});

app.post('/api/admin/funnels', (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const out = saveFunnelDefinitions(req.body || {});
    res.json({ ok: true, ...out });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'Save failed' });
  }
});

app.get('/api/admin/visitors/:id/timeline', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminVisitorTimeline(getConfig(), req.params.id);
  if (!out.ok) {
    const code = out.error === 'Visitor not found' ? 404 : 502;
    res.status(code).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/visitors', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListVisitors(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    visitors: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
    grouped: out.grouped,
    truncated: out.truncated,
  });
});

app.get('/api/admin/orders', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListOrders(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    orders: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
  });
});

app.get('/api/admin/orders/summary', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminOrdersSummary(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Summary failed' });
    return;
  }
  res.json({
    ok: true,
    orderCount: out.orderCount,
    revenueInr: out.revenueInr,
    revenuePaise: out.revenuePaise,
    truncated: out.truncated,
    ordersAttributedToLead: out.ordersAttributedToLead,
    ordersDirectPurchase: out.ordersDirectPurchase,
  });
});

app.get('/api/admin/customers', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListCustomers(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    customers: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
  });
});

app.get('/api/admin/profiles', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListProfiles(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    profiles: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
  });
});

app.get('/api/admin/profiles/:id', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminProfileDetail(getConfig(), req.params.id);
  if (!out.ok) {
    const code = out.error === 'Profile not found' ? 404 : 502;
    res.status(code).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/customers/:id/activity-timeline', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminCustomerActivityTimeline(getConfig(), req.params.id);
  if (!out.ok) {
    const code = out.error === 'Customer not found' ? 404 : 502;
    res.status(code).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/leads', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListLeads(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    leads: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
    grouped: out.grouped,
    truncated: out.truncated,
  });
});

app.get('/api/admin/abandoned-checkouts', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminListAbandoned(getConfig(), req.query);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'List failed' });
    return;
  }
  res.json({
    ok: true,
    abandonedCheckouts: out.rows,
    total: out.total,
    page: out.page,
    perPage: out.perPage,
    grouped: out.grouped,
    truncated: out.truncated,
  });
});

app.get('/api/admin/abandoned-checkouts/:id/context', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminAbandonedCheckoutContext(getConfig(), req.params.id);
  if (!out.ok) {
    const code = out.error === 'Abandoned checkout not found' ? 404 : 502;
    res.status(code).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.get('/api/admin/orders/:id/pre-purchase-timeline', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminOrderPrePurchaseTimeline(getConfig(), req.params.id);
  if (!out.ok) {
    const code = out.error === 'Order not found' ? 404 : 502;
    res.status(code).json({ ok: false, error: out.error ?? 'Failed' });
    return;
  }
  res.json(out);
});

app.patch('/api/admin/orders/:id', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const st = req.body?.order_status != null ? String(req.body.order_status) : '';
  const out = await adminUpdateOrderStatus(getConfig(), req.params.id, st);
  if (!out.ok) {
    res.status(400).json({ ok: false, error: out.error ?? 'Update failed' });
    return;
  }
  res.json({ ok: true });
});

app.delete('/api/admin/leads/:id', async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const out = await adminDeleteLead(getConfig(), req.params.id);
  if (!out.ok) {
    res.status(502).json({ ok: false, error: out.error ?? 'Delete failed' });
    return;
  }
  res.json({ ok: true });
});

app.use('/api', (req, res) => {
  res.status(404).json({ ok: false, error: 'Unknown API route' });
});

/** Same-origin test: serve public_html like nginx does on VPS (site + /api on one port). */
const serveStatic =
  process.env.SERVE_STATIC === '1' ||
  process.env.SERVE_STATIC === 'true' ||
  process.env.SERVE_STATIC === 'yes';

/** Clean URLs → real files (add nginx mirrors on production). */
const URL_REWRITES = {
  '/kundli': '/kundli.html',
  '/lp/kundli': '/lp/kundli.html',
  '/tracking': '/tracking.html',
  '/consultancy': '/consultancy.html',
  '/contact': '/contact-us.html',
  '/metal-finder': '/free-metal-finder-tool.html',
  '/tools/metal': '/metal/index.html',
  '/kundli-preview': '/kundli-preview.html',
  /** Product URLs used in nav/CTAs — map to real files under site root */
  '/products/consultancy-checkout': '/consultancy-checkout.html',
  '/products/kundli-checkout': '/lp/kundli-checkout/index.html',
};

if (serveStatic) {
  const staticRoot = process.env.STATIC_ROOT
    ? path.resolve(process.env.STATIC_ROOT)
    : path.join(__dirname, '..');
  const adminPanelPath = process.env.ADMIN_PANEL_PATH || '/admindeoghar';
  const adminRoot = path.join(staticRoot, 'admin');
  app.use((req, res, next) => {
    if (req.method !== 'GET' && req.method !== 'HEAD') return next();
    const pathname = (req.path || '').replace(/\/$/, '') || '/';
    const q = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
    if (URL_REWRITES[pathname]) req.url = URL_REWRITES[pathname] + q;
    next();
  });

  // Harden admin UI URL: serve admin from /admindeoghar (or ADMIN_PANEL_PATH).
  // API stays at /api/admin/* (protected by ADMIN_SECRET).
  app.use((req, res, next) => {
    if (req.method !== 'GET' && req.method !== 'HEAD') return next();
    // Avoid exposing the default /admin path for the panel UI.
    if (req.path === '/admin' || req.path.startsWith('/admin/')) {
      res.status(404).send('Not found');
      return;
    }
    next();
  });
  app.use((req, res, next) => {
    if (req.method !== 'GET' && req.method !== 'HEAD') return next();
    const q = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
    if (req.path === adminPanelPath) req.url = adminPanelPath + '/' + q;
    next();
  });
  app.use(
    adminPanelPath,
    express.static(adminRoot, {
      index: ['index.html'],
      maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0,
    })
  );

  app.use(
    express.static(staticRoot, {
      extensions: ['html'],
      index: ['index.html'],
      maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0,
      // Opt-in: if Nginx does not set CSP, set STATIC_HTML_CSP=1 so HTML allows CDN images (https:).
      // If Nginx already sends a strict CSP, fix Nginx (img-src https:) or omit this to avoid duplicate policies.
      setHeaders: (res, filePath) => {
        if (process.env.STATIC_HTML_CSP !== '1') return;
        if (!filePath.endsWith('.html')) return;
        res.setHeader(
          'Content-Security-Policy',
          [
            "default-src 'self'",
            "img-src 'self' data: blob: https:",
            "font-src 'self' data: https:",
            "style-src 'self' 'unsafe-inline' https:",
            "script-src 'self' 'unsafe-inline' https:",
            "connect-src 'self' https: wss:",
            "frame-src https:",
            "media-src 'self' https:",
            "base-uri 'self'",
          ].join('; ')
        );
      },
    })
  );
  console.log(`[static] Serving site from ${staticRoot} (set SERVE_STATIC=0 to API-only)`);
}

const port = getConfig().port;
app.listen(port, () => {
  console.log(`VedGuide API listening on http://127.0.0.1:${port}`);
  if (serveStatic) {
    console.log(`Open http://127.0.0.1:${port}/ — static site + /api same origin (VPS-like)`);
  }
});
