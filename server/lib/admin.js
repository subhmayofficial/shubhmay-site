import { DateTime } from 'luxon';
import { supabaseRequest, supabaseGetRange, supabaseCountRows, parseContentRangeTotal } from './supabase.js';
import { strTrim } from './strings.js';

const ADMIN_TZ = 'Asia/Kolkata';

export const ADMIN_ORDERS_SELECT =
  'id,customer_id,lead_id,abandoned_checkout_id,product_slug,razorpay_order_id,razorpay_payment_id,receipt,amount_paise,currency,payment_status,order_status,dob,tob,birth_place,language,coupon,paid_at,created_at,updated_at,razorpay_notes,customers(name,email,phone,is_paying_customer,first_paid_at,total_spent_paise,created_at),leads!orders_lead_id_fkey(session_id,utm_source,utm_medium,utm_campaign,landing_path,referrer,source_page,lead_status,first_seen_at,last_seen_at),abandoned_checkouts!orders_abandoned_checkout_id_fkey(checkout_session_id,stage,abandoned_at,last_event_at,utm_source,utm_medium,utm_campaign)';

// visitors!leads_visitor_id_fkey — disambiguate from visitors.converted_lead_id → leads (PGRST201)
export const ADMIN_LEADS_SELECT =
  'id,session_id,email,name,phone,visitor_id,source_page,landing_path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,user_agent,client_language,screen_width,screen_height,lead_status,converted_order_id,first_seen_at,last_seen_at,intent_score,intent_tier,meta,created_at,updated_at,visitors!leads_visitor_id_fkey(id,session_id,converted_lead_id,conversion_at,conversion_source),lead_events(id,session_id,event_type,event_name,stage,path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,meta,created_at),orders!orders_lead_id_fkey(id,product_slug,amount_paise,currency,payment_status,order_status,paid_at,created_at,razorpay_order_id,razorpay_payment_id,receipt,coupon),abandoned_checkouts!abandoned_checkouts_lead_id_fkey(id,checkout_session_id,stage,product_slug,amount_paise,currency,last_event_at,abandoned_at,converted_order_id,converted_at,razorpay_order_id,referrer,landing_path),consultancy_bookings!consultancy_bookings_lead_id_fkey(id,plan_code,plan_name,duration_minutes,amount_paise,currency,status,payment_status,slot_start,slot_end,razorpay_order_id,created_at)';

export const ADMIN_VISITORS_SELECT =
  'id,session_id,email,name,phone,source_page,landing_path,referrer,document_referrer,utm_source,utm_medium,utm_campaign,utm_content,utm_term,user_agent,client_language,screen_width,screen_height,first_seen_at,last_seen_at,converted_lead_id,conversion_at,conversion_source,intent_score,intent_tier,meta,created_at,updated_at';

/** Drop duplicate rows when the same event was stored on visitor_events and lead_events (legacy data). */
function dedupeMergedTimelineEvents(merged) {
  const sorted = [...merged].sort(
    (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
  );
  const out = [];
  for (const ev of sorted) {
    const prev = out[out.length - 1];
    if (prev) {
      const dt = Math.abs(new Date(ev.created_at || 0) - new Date(prev.created_at || 0));
      const sameKind =
        String(ev.event_type || '') === String(prev.event_type || '') &&
        String(ev.event_name || ev.stage || '') === String(prev.event_name || prev.stage || '') &&
        String(ev.path || '') === String(prev.path || '');
      if (sameKind && dt < 4000) {
        if (ev._source === 'lead' && prev._source === 'visitor') {
          out[out.length - 1] = ev;
        }
        continue;
      }
    }
    out.push(ev);
  }
  return out;
}

function isoDayBoundsIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('day');
  const end = start.plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoYesterdayBoundsIst() {
  const today = isoDayBoundsIst();
  const end = DateTime.fromISO(today.startIso);
  const start = end.minus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoLastNDaysRollingIst(n) {
  n = Math.max(1, Math.min(366, n));
  const t = isoDayBoundsIst();
  const end = DateTime.fromISO(t.endIso);
  const start = DateTime.fromISO(t.startIso).minus({ days: n - 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoThisMonthIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('month');
  const end = now.startOf('day').plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

function isoThisWeekIst() {
  const now = DateTime.now().setZone(ADMIN_TZ);
  const start = now.startOf('week');
  const end = now.startOf('day').plus({ days: 1 });
  return { startIso: start.toISO(), endIso: end.toISO() };
}

export function resolveAnalyticsWindow(q) {
  const from = strTrim(q.date_from, 100) || '';
  const to = strTrim(q.date_to, 100) || '';
  if (from && to) {
    const a = Date.parse(from);
    const b = Date.parse(to);
    if (!Number.isNaN(a) && !Number.isNaN(b) && a < b) {
      return { startIso: from, endIso: to, label: 'custom' };
    }
  }
  const preset = String(q.preset ?? '')
    .trim()
    .toLowerCase();
  if (preset === 'yesterday') {
    const y = isoYesterdayBoundsIst();
    return { ...y, label: 'yesterday' };
  }
  if (preset === 'last3') {
    const r = isoLastNDaysRollingIst(3);
    return { ...r, label: 'last3' };
  }
  if (preset === 'last7') {
    const r = isoLastNDaysRollingIst(7);
    return { ...r, label: 'last7' };
  }
  if (preset === 'last30') {
    const r = isoLastNDaysRollingIst(30);
    return { ...r, label: 'last30' };
  }
  if (preset === 'this_month') {
    const r = isoThisMonthIst();
    return { ...r, label: 'this_month' };
  }
  if (preset === 'this_week') {
    const r = isoThisWeekIst();
    return { ...r, label: 'this_week' };
  }
  const t = isoDayBoundsIst();
  return { ...t, label: 'today' };
}

export function pageFromQuery(q) {
  const page = Math.max(1, parseInt(String(q.page ?? '1'), 10) || 1);
  const perPage = Math.min(100, Math.max(1, parseInt(String(q.per_page ?? '10'), 10) || 10));
  const offset = (page - 1) * perPage;
  const end = offset + perPage - 1;
  return { page, perPage, range: `${offset}-${end}` };
}

function buildOrdersFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  if (s(q.order_status)) parts.push(`order_status=eq.${encodeURIComponent(s(q.order_status))}`);
  if (s(q.payment_status)) parts.push(`payment_status=eq.${encodeURIComponent(s(q.payment_status))}`);
  if (s(q.product_slug)) parts.push(`product_slug=eq.${encodeURIComponent(s(q.product_slug))}`);
  const acq = s(q.acquisition).toLowerCase();
  if (acq === 'direct') parts.push('lead_id=is.null');
  if (acq === 'lead') parts.push('lead_id=not.is.null');
  const df = s(q.date_from);
  const dt = s(q.date_to);
  /* PostgREST ORs repeated column filters — must use and=(...) for a real range. */
  if (df && dt) {
    parts.push(`and=(paid_at.gte.${encodeURIComponent(df)},paid_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`paid_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`paid_at=lte.${encodeURIComponent(dt)}`);
  }
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(razorpay_order_id.ilike.*${x}*,razorpay_payment_id.ilike.*${x}*,receipt.ilike.*${x}*)`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildCustomersFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const paying = s(q.paying);
  if (paying === '1' || paying === 'true' || paying === 'yes') parts.push('is_paying_customer=eq.true');
  else if (paying === '0' || paying === 'false' || paying === 'no') parts.push('is_paying_customer=eq.false');
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(email.ilike.*${x}*,name.ilike.*${x}*,phone.ilike.*${x}*)`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(created_at.gte.${encodeURIComponent(df)},created_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`created_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`created_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildLeadsFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const contactsOnly = s(q.contacts_only).toLowerCase();
  if (contactsOnly === '1' || contactsOnly === 'true' || contactsOnly === 'yes') {
    parts.push('or=(phone.not.is.null,email.not.is.null)');
  }
  if (s(q.utm_source)) parts.push(`utm_source=eq.${encodeURIComponent(s(q.utm_source))}`);
  if (s(q.utm_medium)) parts.push(`utm_medium=eq.${encodeURIComponent(s(q.utm_medium))}`);
  if (s(q.utm_campaign)) parts.push(`utm_campaign=ilike.*${encodeURIComponent(s(q.utm_campaign))}*`);
  if (s(q.lead_status)) parts.push(`lead_status=eq.${encodeURIComponent(s(q.lead_status))}`);
  const conv = s(q.converted).toLowerCase();
  if (['yes', '1', 'true'].includes(conv)) parts.push('converted_order_id=not.is.null');
  else if (['no', '0', 'false'].includes(conv)) parts.push('converted_order_id=is.null');
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(email.ilike.*${x}*,session_id.ilike.*${x}*,phone.ilike.*${x}*,name.ilike.*${x}*)`);
  }
  const it = s(q.intent_tier).toLowerCase();
  if (it === 'high' || it === 'medium' || it === 'low') {
    parts.push(`intent_tier=eq.${encodeURIComponent(it)}`);
  }
  const imin = parseInt(s(q.intent_min), 10);
  const imax = parseInt(s(q.intent_max), 10);
  const hasMin = !Number.isNaN(imin);
  const hasMax = !Number.isNaN(imax);
  if (hasMin && hasMax) {
    parts.push(`and=(intent_score.gte.${imin},intent_score.lte.${imax})`);
  } else if (hasMin) {
    parts.push(`intent_score=gte.${imin}`);
  } else if (hasMax) {
    parts.push(`intent_score=lte.${imax}`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  const dfld = s(q.date_field);
  const field = dfld === 'created' ? 'created_at' : dfld === 'first_seen' ? 'first_seen_at' : 'last_seen_at';
  if (df && dt) {
    parts.push(`and=(${field}.gte.${encodeURIComponent(df)},${field}.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`${field}=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`${field}=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildVisitorsFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  const conv = s(q.converted).toLowerCase();
  if (conv === 'yes' || conv === '1' || conv === 'true') parts.push('converted_lead_id=not.is.null');
  else if (conv === 'no' || conv === '0' || conv === 'false') parts.push('converted_lead_id=is.null');
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(session_id.ilike.*${x}*,email.ilike.*${x}*,phone.ilike.*${x}*,name.ilike.*${x}*,landing_path.ilike.*${x}*)`);
  }
  if (s(q.utm_source)) parts.push(`utm_source=eq.${encodeURIComponent(s(q.utm_source))}`);
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(last_seen_at.gte.${encodeURIComponent(df)},last_seen_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`last_seen_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`last_seen_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

function buildAbandonedFilterQs(q) {
  const parts = [];
  const s = (v) => String(v ?? '').trim();
  if (s(q.stage)) parts.push(`stage=eq.${encodeURIComponent(s(q.stage))}`);
  if (s(q.utm_campaign)) parts.push(`utm_campaign=ilike.*${encodeURIComponent(s(q.utm_campaign))}*`);
  if (s(q.search)) {
    const x = encodeURIComponent(s(q.search));
    parts.push(`or=(email.ilike.*${x}*,checkout_session_id.ilike.*${x}*,phone.ilike.*${x}*)`);
  }
  const df = s(q.date_from);
  const dt = s(q.date_to);
  if (df && dt) {
    parts.push(`and=(last_event_at.gte.${encodeURIComponent(df)},last_event_at.lte.${encodeURIComponent(dt)})`);
  } else if (df) {
    parts.push(`last_event_at=gte.${encodeURIComponent(df)}`);
  } else if (dt) {
    parts.push(`last_event_at=lte.${encodeURIComponent(dt)}`);
  }
  return parts.length ? `&${parts.join('&')}` : '';
}

export async function adminListOrders(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildOrdersFilterQs(q);
  const sel = encodeURIComponent(ADMIN_ORDERS_SELECT);
  const path = `orders?select=${sel}&order=created_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

/** Same filters as list; sums revenue for current filter (cap rows for safety). */
export async function adminOrdersSummary(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const filter = buildOrdersFilterQs(q);
  const cap = 8000;
  const sel = encodeURIComponent('amount_paise,currency,lead_id');
  const path = `orders?select=${sel}&order=created_at.desc${filter}&limit=${cap}`;
  const r = await supabaseRequest(cfg, 'GET', path);
  if (r.code < 200 || r.code >= 300) {
    return { ok: false, error: (r.body || '').slice(0, 400) };
  }
  const rows = JSON.parse(r.body || '[]');
  const list = Array.isArray(rows) ? rows : [];
  let rev = 0;
  let fromLead = 0;
  let direct = 0;
  for (const o of list) {
    rev += parseInt(o.amount_paise ?? 0, 10);
    if (o.lead_id) fromLead++;
    else direct++;
  }
  return {
    ok: true,
    orderCount: list.length,
    revenueInr: Math.round(rev) / 100,
    revenuePaise: rev,
    truncated: list.length >= cap,
    ordersAttributedToLead: fromLead,
    ordersDirectPurchase: direct,
  };
}

export async function adminListCustomers(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildCustomersFilterQs(q);
  const sel =
    'id,email,name,phone,is_paying_customer,first_paid_at,total_spent_paise,notes,meta,created_at,updated_at';
  const path = `customers?select=${encodeURIComponent(sel)}&order=created_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListLeads(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildLeadsFilterQs(q);
  const path = `leads?select=${encodeURIComponent(ADMIN_LEADS_SELECT)}&order=last_seen_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListAbandoned(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildAbandonedFilterQs(q);
  const sel =
    'id,checkout_session_id,lead_id,email,name,phone,product_slug,stage,razorpay_order_id,amount_paise,currency,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,landing_path,last_event_at,abandoned_at,converted_order_id,converted_at,meta,created_at,updated_at,leads(session_id,utm_campaign,utm_source,utm_medium,landing_path)';
  const path = `abandoned_checkouts?select=${encodeURIComponent(sel)}&order=last_event_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminUpdateOrderStatus(cfg, orderId, orderStatus) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(orderId).trim();
  const st = strTrim(orderStatus, 64) || '';
  if (!id || !st) return { ok: false, error: 'order id and order_status required' };
  const patch = JSON.stringify({ order_status: st, updated_at: new Date().toISOString() });
  const r = await supabaseRequest(cfg, 'PATCH', `orders?id=eq.${encodeURIComponent(id)}`, patch, 'return=minimal');
  if (r.code >= 200 && r.code < 300) return { ok: true };
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminDeleteLead(cfg, leadId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(leadId).trim();
  if (!id) return { ok: false, error: 'Missing lead id' };
  const r = await supabaseRequest(cfg, 'DELETE', `leads?id=eq.${encodeURIComponent(id)}`, null, 'return=minimal');
  if (r.code >= 200 && r.code < 300) return { ok: true };
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminOrderPrePurchaseTimeline(cfg, orderId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(orderId).trim();
  if (!id) return { ok: false, error: 'order id required' };
  const oq = `orders?id=eq.${encodeURIComponent(id)}&select=id,lead_id,paid_at,created_at,product_slug&limit=1`;
  const or = await supabaseRequest(cfg, 'GET', oq);
  if (or.code < 200 || or.code >= 300) {
    return { ok: false, error: (or.body || '').slice(0, 300) };
  }
  const rows = JSON.parse(or.body || '[]');
  const order = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!order) return { ok: false, error: 'Order not found' };
  const paidAt = order.paid_at ?? order.created_at;
  const leadId = order.lead_id ?? null;
  const out = {
    ok: true,
    orderId: order.id,
    leadId,
    paidAt,
    acquisition: leadId ? 'lead_attributed' : 'direct_purchase',
    events: [],
  };
  if (!leadId) return out;

  let visitorId = null;
  const lq = `leads?id=eq.${encodeURIComponent(String(leadId))}&select=visitor_id&limit=1`;
  const lres = await supabaseRequest(cfg, 'GET', lq);
  if (lres.code >= 200 && lres.code < 300) {
    const lr = JSON.parse(lres.body || '[]');
    if (lr[0]?.visitor_id) visitorId = String(lr[0].visitor_id);
  }

  const visitorBefore = [];
  if (visitorId) {
    const vUrl =
      `visitor_events?visitor_id=eq.${encodeURIComponent(visitorId)}` +
      '&select=id,session_id,event_type,event_name,path,referrer,meta,created_at' +
      `&created_at=lte.${encodeURIComponent(String(paidAt))}` +
      '&order=created_at.asc&limit=500';
    const vr = await supabaseRequest(cfg, 'GET', vUrl);
    if (vr.code >= 200 && vr.code < 300) {
      const evs = JSON.parse(vr.body || '[]');
      visitorBefore.push(...(Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'visitor' })) : []));
    }
  }

  const evUrl =
    `lead_events?lead_id=eq.${encodeURIComponent(String(leadId))}` +
    '&select=id,session_id,event_type,event_name,stage,path,referrer,utm_source,utm_medium,utm_campaign,meta,created_at' +
    `&created_at=lte.${encodeURIComponent(String(paidAt))}` +
    '&order=created_at.asc&limit=500';
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let leadEvs = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    leadEvs = Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'lead' })) : [];
  }

  const merged = dedupeMergedTimelineEvents(
    [...visitorBefore, ...leadEvs].sort(
      (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
    )
  );
  out.events = merged;
  out.visitorEvents = visitorBefore;
  out.leadOnlyEvents = leadEvs;
  return out;
}

export async function adminGetAnalytics(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const w = resolveAnalyticsWindow(q);
  const startIso = w.startIso;
  const endIso = w.endIso;
  const label = w.label;

  const leadsPath = `leads?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;
  const leadsConvPath = `leads?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})&converted_order_id=not.is.null`;

  const leadsIn = await supabaseCountRows(cfg, leadsPath);
  const leadsConv = await supabaseCountRows(cfg, leadsConvPath);
  const leadPct = leadsIn > 0 ? Math.round((leadsConv / leadsIn) * 1000) / 10 : 0;

  const ordersPath = `orders?select=amount_paise,lead_id,currency&and=(paid_at.gte.${encodeURIComponent(startIso)},paid_at.lt.${encodeURIComponent(endIso)},payment_status.eq.paid)&limit=5000`;
  const or = await supabaseRequest(cfg, 'GET', ordersPath);
  let ordersRows = [];
  if (or.code >= 200 && or.code < 300) {
    const rows = JSON.parse(or.body || '[]');
    ordersRows = Array.isArray(rows) ? rows : [];
  }
  let rev = 0;
  let fromLead = 0;
  let direct = 0;
  for (const o of ordersRows) {
    rev += parseInt(o.amount_paise ?? 0, 10);
    if (o.lead_id) fromLead++;
    else direct++;
  }
  const ordersPaid = ordersRows.length;

  const allOrders = await supabaseCountRows(cfg, 'orders?select=id');
  const allLeads = await supabaseCountRows(cfg, 'leads?select=id');
  const allCust = await supabaseCountRows(cfg, 'customers?select=id');
  const allVisitors = await supabaseCountRows(cfg, 'visitors?select=id');

  const visitorsPath = `visitors?select=id&and=(first_seen_at.gte.${encodeURIComponent(startIso)},first_seen_at.lt.${encodeURIComponent(endIso)})`;
  const visitorsNew = await supabaseCountRows(cfg, visitorsPath);
  const visitorToLeadPath = `visitors?select=id&and=(conversion_at.gte.${encodeURIComponent(startIso)},conversion_at.lt.${encodeURIComponent(endIso)})&converted_lead_id=not.is.null`;
  const visitorsConvertedToLead = await supabaseCountRows(cfg, visitorToLeadPath);

  const lsWin = `and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`;
  const intentHigh = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.high`);
  const intentMedium = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.medium`);
  const intentLow = await supabaseCountRows(cfg, `leads?select=id&${lsWin}&intent_tier=eq.low`);

  const pageViewsQ = `lead_events?select=id&and=(created_at.gte.${encodeURIComponent(startIso)},created_at.lt.${encodeURIComponent(endIso)},event_name.eq.page_view)`;
  const pageViewsTotal = await supabaseCountRows(cfg, pageViewsQ);

  const visitorsActiveQ = `visitors?select=id&and=(last_seen_at.gte.${encodeURIComponent(startIso)},last_seen_at.lt.${encodeURIComponent(endIso)})`;
  const visitorsActiveInPeriod = await supabaseCountRows(cfg, visitorsActiveQ);

  const abWin = `and=(last_event_at.gte.${encodeURIComponent(startIso)},last_event_at.lt.${encodeURIComponent(endIso)})`;
  const abandonedCheckoutSessions = await supabaseCountRows(cfg, `abandoned_checkouts?select=id&${abWin}`);
  const abandonedConvertedLater = await supabaseCountRows(
    cfg,
    `abandoned_checkouts?select=id&${abWin}&converted_order_id=not.is.null`
  );

  const period = {
    leadsCollected: leadsIn,
    leadsConverted: leadsConv,
    leadToOrderConversionPercent: leadPct,
    ordersPaid,
    revenuePaise: rev,
    revenueInr: Math.round(rev) / 100,
    ordersAttributedToLead: fromLead,
    ordersDirectPurchase: direct,
    visitorsNew,
    visitorsActiveInPeriod,
    pageViewsTotal,
    abandonedCheckoutSessions,
    abandonedLaterPaid: abandonedConvertedLater,
    visitorsConvertedToLead,
    visitorToLeadRatePercent:
      visitorsNew > 0 ? Math.round((visitorsConvertedToLead / visitorsNew) * 1000) / 10 : 0,
    intentLeadsHigh: intentHigh,
    intentLeadsMedium: intentMedium,
    intentLeadsLow: intentLow,
  };

  return {
    ok: true,
    timezone: ADMIN_TZ,
    preset: label,
    periodStart: startIso,
    periodEnd: endIso,
    dayStart: startIso,
    dayEnd: endIso,
    today: period,
    period,
    allTime: {
      orders: allOrders,
      leads: allLeads,
      customers: allCust,
      visitors: allVisitors,
    },
  };
}

export async function adminSaveAnalyticsSnapshot(cfg, body) {
  const metrics = await adminGetAnalytics(cfg, body);
  if (!metrics.ok) {
    return { ok: false, error: String(metrics.error ?? 'Analytics failed') };
  }
  const w = resolveAnalyticsWindow(body);
  const row = {
    period_start: w.startIso,
    period_end: w.endIso,
    preset: metrics.preset ?? null,
    payload: metrics,
  };
  const ins = await supabaseRequest(cfg, 'POST', 'analytics_snapshots', JSON.stringify(row), 'return=minimal');
  if (ins.code >= 200 && ins.code < 300) return { ok: true };
  return { ok: false, error: (ins.body || '').slice(0, 400) };
}

export async function adminListAnalyticsSnapshots(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const limit = Math.min(100, Math.max(1, parseInt(String(q.limit ?? '20'), 10) || 20));
  const path = `analytics_snapshots?select=id,created_at,period_start,period_end,preset,payload&order=created_at.desc&limit=${limit}`;
  const r = await supabaseRequest(cfg, 'GET', path);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return { ok: true, rows: Array.isArray(rows) ? rows : [] };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminListVisitors(cfg, q) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const pg = pageFromQuery(q);
  const filter = buildVisitorsFilterQs(q);
  const path = `visitors?select=${encodeURIComponent(ADMIN_VISITORS_SELECT)}&order=last_seen_at.desc${filter}`;
  const r = await supabaseGetRange(cfg, path, pg.range);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    return {
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total: parseContentRangeTotal(r.contentRange),
      page: pg.page,
      perPage: pg.perPage,
    };
  }
  return { ok: false, error: (r.body || '').slice(0, 400) };
}

export async function adminVisitorTimeline(cfg, visitorId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(visitorId).trim();
  if (!id) return { ok: false, error: 'visitor id required' };
  const vq = `visitors?id=eq.${encodeURIComponent(id)}&select=${encodeURIComponent(ADMIN_VISITORS_SELECT)}&limit=1`;
  const vr = await supabaseRequest(cfg, 'GET', vq);
  if (vr.code < 200 || vr.code >= 300) {
    return { ok: false, error: (vr.body || '').slice(0, 300) };
  }
  const vrows = JSON.parse(vr.body || '[]');
  const visitor = Array.isArray(vrows) && vrows[0] ? vrows[0] : null;
  if (!visitor) return { ok: false, error: 'Visitor not found' };

  const evUrl = `visitor_events?visitor_id=eq.${encodeURIComponent(id)}&select=id,event_type,event_name,path,referrer,meta,created_at&order=created_at.asc&limit=2000`;
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let visitorEvents = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    visitorEvents = Array.isArray(evs) ? evs : [];
  }

  let leadEvents = [];
  if (visitor.converted_lead_id) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `lead_events?lead_id=eq.${encodeURIComponent(String(visitor.converted_lead_id))}&select=id,event_type,event_name,stage,path,referrer,meta,created_at&order=created_at.asc&limit=2000`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const evs = JSON.parse(lr.body || '[]');
      leadEvents = Array.isArray(evs) ? evs : [];
    }
  }

  return { ok: true, visitor, visitorEvents, leadEvents };
}

/** Prior abandon sessions + conversion label for admin abandoned-checkout expand. */
export async function adminAbandonedCheckoutContext(cfg, abandonedId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const id = String(abandonedId || '').trim();
  if (!id) return { ok: false, error: 'id required' };
  const gr = await supabaseRequest(
    cfg,
    'GET',
    `abandoned_checkouts?id=eq.${encodeURIComponent(id)}&select=id,email,name,phone,checkout_session_id,stage,last_event_at,abandoned_at,converted_order_id,converted_at,utm_source,utm_medium,utm_campaign,landing_path,referrer,product_slug,amount_paise,currency&limit=1`
  );
  if (gr.code < 200 || gr.code >= 300) {
    return { ok: false, error: (gr.body || '').slice(0, 300) };
  }
  const rows = JSON.parse(gr.body || '[]');
  const row = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!row) return { ok: false, error: 'Abandoned checkout not found' };

  const email = String(row.email || '')
    .trim()
    .toLowerCase();
  const phone = String(row.phone || '').replace(/\s+/g, '');

  let priorAbandonSessions = 0;
  if (email || phone) {
    const orParts = [];
    if (email) orParts.push(`email.eq.${encodeURIComponent(email)}`);
    if (phone) orParts.push(`phone.eq.${encodeURIComponent(phone)}`);
    const orq = orParts.join(',');
    const pq = `abandoned_checkouts?select=id&or=(${orq})&last_event_at=lt.${encodeURIComponent(String(row.last_event_at))}&id=neq.${encodeURIComponent(id)}`;
    priorAbandonSessions = await supabaseCountRows(cfg, pq);
  }

  let conversionSummary = 'Not paid yet';
  let paidOrderNumber = 0;
  if (row.converted_order_id) {
    const ordr = await supabaseRequest(
      cfg,
      'GET',
      `orders?id=eq.${encodeURIComponent(String(row.converted_order_id))}&select=id,customer_id,paid_at,payment_status&limit=1`
    );
    let ord = null;
    if (ordr.code >= 200 && ordr.code < 300) {
      const orows = JSON.parse(ordr.body || '[]');
      ord = Array.isArray(orows) && orows[0] ? orows[0] : null;
    }
    if (ord && ord.customer_id) {
      const paidAt = ord.paid_at || row.converted_at;
      const cntPath = `orders?select=id&customer_id=eq.${encodeURIComponent(String(ord.customer_id))}&payment_status=eq.paid&paid_at=lte.${encodeURIComponent(String(paidAt))}`;
      paidOrderNumber = await supabaseCountRows(cfg, cntPath);
      if (paidOrderNumber <= 1) {
        conversionSummary = 'First payment from this customer (1st time converted)';
      } else {
        conversionSummary = `Return buyer — this is payment #${paidOrderNumber} for this customer`;
      }
    } else {
      conversionSummary = 'Marked converted — order or customer link missing';
    }
  }

  return {
    ok: true,
    priorAbandonSessions,
    conversionSummary,
    paidOrderNumber,
    repeatAbandoner: priorAbandonSessions > 0,
  };
}

function phoneMatchOrParts(phoneRaw) {
  const raw = String(phoneRaw || '').trim();
  const digits = raw.replace(/\D/g, '');
  const parts = [];
  if (raw) parts.push(`phone.eq.${encodeURIComponent(raw)}`);
  if (digits && digits.length >= 8) {
    parts.push(`phone.eq.${encodeURIComponent(digits)}`);
    if (digits.length === 10) parts.push(`phone.eq.${encodeURIComponent('91' + digits)}`);
    if (digits.length === 12 && digits.startsWith('91'))
      parts.push(`phone.eq.${encodeURIComponent(digits.slice(2))}`);
  }
  return parts;
}

/** Full visitor + lead event history: prefer lead linked from a paid order, else email/phone on leads. */
export async function adminCustomerActivityTimeline(cfg, customerId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const cid = String(customerId || '').trim();
  if (!cid) return { ok: false, error: 'customer id required' };
  const cr = await supabaseRequest(
    cfg,
    'GET',
    `customers?id=eq.${encodeURIComponent(cid)}&select=id,email,name,phone,created_at&limit=1`
  );
  if (cr.code < 200 || cr.code >= 300) {
    return { ok: false, error: (cr.body || '').slice(0, 300) };
  }
  const crows = JSON.parse(cr.body || '[]');
  const customer = Array.isArray(crows) && crows[0] ? crows[0] : null;
  if (!customer) return { ok: false, error: 'Customer not found' };

  const em = String(customer.email || '').trim().toLowerCase();
  const ph = String(customer.phone || '').replace(/\s+/g, '');

  let lead = null;
  const oPath =
    `orders?customer_id=eq.${encodeURIComponent(cid)}` +
    '&lead_id=not.is.null&select=lead_id&order=paid_at.desc&limit=1';
  const orderLeadRes = await supabaseRequest(cfg, 'GET', oPath);
  let leadIdFromOrder = null;
  if (orderLeadRes.code >= 200 && orderLeadRes.code < 300) {
    const orows = JSON.parse(orderLeadRes.body || '[]');
    if (Array.isArray(orows) && orows[0] && orows[0].lead_id) {
      leadIdFromOrder = String(orows[0].lead_id);
    }
  }
  if (leadIdFromOrder) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `leads?id=eq.${encodeURIComponent(leadIdFromOrder)}&select=id,visitor_id,session_id,first_seen_at,last_seen_at&limit=1`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const lrows = JSON.parse(lr.body || '[]');
      lead = Array.isArray(lrows) && lrows[0] ? lrows[0] : null;
    }
  }

  if (!lead) {
    const orParts = [];
    if (em) {
      orParts.push(`email.eq.${encodeURIComponent(em)}`);
      orParts.push(`email.ilike.${encodeURIComponent('*' + em + '*')}`);
    }
    orParts.push(...phoneMatchOrParts(ph));
    const uniq = [...new Set(orParts)];
    if (!uniq.length) {
      return {
        ok: true,
        customer,
        lead: null,
        events: [],
        note: 'No email or phone on file — cannot match activity.',
      };
    }
    const lq = `leads?select=id,visitor_id,session_id,first_seen_at,last_seen_at&or=(${uniq.join(',')})&order=last_seen_at.desc&limit=1`;
    const lr = await supabaseRequest(cfg, 'GET', lq);
    if (lr.code >= 200 && lr.code < 300) {
      const lrows = JSON.parse(lr.body || '[]');
      lead = Array.isArray(lrows) && lrows[0] ? lrows[0] : null;
    }
  }

  if (!lead) {
    return {
      ok: true,
      customer,
      lead: null,
      events: [],
      note: 'No browsing history matched yet — it appears after they use the site with tracking, or after a linked order.',
    };
  }

  const visitorBefore = [];
  const vid = lead.visitor_id ? String(lead.visitor_id) : '';
  if (vid) {
    const vUrl =
      `visitor_events?visitor_id=eq.${encodeURIComponent(vid)}` +
      '&select=id,session_id,event_type,event_name,path,referrer,meta,created_at&order=created_at.asc&limit=2000';
    const vr = await supabaseRequest(cfg, 'GET', vUrl);
    if (vr.code >= 200 && vr.code < 300) {
      const evs = JSON.parse(vr.body || '[]');
      visitorBefore.push(
        ...(Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'visitor' })) : [])
      );
    }
  }

  const evUrl =
    `lead_events?lead_id=eq.${encodeURIComponent(String(lead.id))}` +
    '&select=id,session_id,event_type,event_name,stage,path,referrer,utm_source,utm_medium,utm_campaign,meta,created_at&order=created_at.asc&limit=2000';
  const er = await supabaseRequest(cfg, 'GET', evUrl);
  let leadEvs = [];
  if (er.code >= 200 && er.code < 300) {
    const evs = JSON.parse(er.body || '[]');
    leadEvs = Array.isArray(evs) ? evs.map((e) => ({ ...e, _source: 'lead' })) : [];
  }

  const merged = dedupeMergedTimelineEvents(
    [...visitorBefore, ...leadEvs].sort(
      (a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0)
    )
  );

  return { ok: true, customer, lead, events: merged };
}
