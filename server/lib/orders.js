import { supabaseRequest } from './supabase.js';
import { razorpayFetchOrder } from './razorpay.js';
import { normalizeEmail, normalizePhone, strTrim, uuidOk } from './strings.js';
import { attachProfileToRecord, ensurePersonProfile, refreshPersonProfileStats } from './personProfiles.js';

function normalizeNotes(notes) {
  return notes && typeof notes === 'object' ? notes : {};
}

function parseDob(s) {
  const t = String(s ?? '').trim();
  if (!t) return null;
  return /^\d{4}-\d{2}-\d{2}$/.test(t) ? t : null;
}

function parseTob(s) {
  const t = String(s ?? '').trim();
  if (!t) return null;
  if (/^\d{2}:\d{2}$/.test(t)) return `${t}:00`;
  if (/^\d{2}:\d{2}:\d{2}$/.test(t)) return t;
  return null;
}

async function ensureCustomer(cfg, name, emailLower, phone) {
  const u = `customers?select=id&email=eq.${encodeURIComponent(emailLower)}&limit=1`;
  let r = await supabaseRequest(cfg, 'GET', u);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    if (Array.isArray(rows) && rows[0]?.id) return String(rows[0].id);
  }
  const insert = {
    name: name !== '' ? strTrim(name, 500) : null,
    email: emailLower,
    phone: phone ? strTrim(phone.replace(/\s+/g, ''), 32) : null,
  };
  const ins = await supabaseRequest(cfg, 'POST', 'customers', JSON.stringify(insert), 'return=minimal');
  if (ins.code >= 200 && ins.code < 300) {
    r = await supabaseRequest(cfg, 'GET', u);
    if (r.code >= 200 && r.code < 300) {
      const rows = JSON.parse(r.body || '[]');
      if (Array.isArray(rows) && rows[0]?.id) return String(rows[0].id);
    }
  } else {
    r = await supabaseRequest(cfg, 'GET', u);
    if (r.code >= 200 && r.code < 300) {
      const rows = JSON.parse(r.body || '[]');
      if (Array.isArray(rows) && rows[0]?.id) return String(rows[0].id);
    }
  }
  return null;
}

async function resolveProfileForOrder(cfg, name, emailLower, phone, leadUuid) {
  if (leadUuid) {
    const lr = await supabaseRequest(
      cfg,
      'GET',
      `leads?id=eq.${encodeURIComponent(leadUuid)}&select=person_profile_id,first_seen_at,landing_path,source_page,referrer&limit=1`
    );
    if (lr.code >= 200 && lr.code < 300) {
      const rows = JSON.parse(lr.body || '[]');
      if (rows[0]?.person_profile_id) return String(rows[0].person_profile_id);
      if (rows[0]) {
        return ensurePersonProfile(cfg, {
          name,
          email: emailLower,
          phone,
          first_seen_at: rows[0].first_seen_at,
          last_seen_at: new Date().toISOString(),
          first_touch_path: rows[0].landing_path,
          first_touch_source: rows[0].source_page,
          first_touch_referrer: rows[0].referrer,
          last_touch_path: rows[0].landing_path,
          last_touch_source: rows[0].source_page,
          last_touch_referrer: rows[0].referrer,
          hasContact: true,
          hasPaid: true,
          lead_status: 'converted',
        });
      }
    }
  }
  return ensurePersonProfile(cfg, {
    name,
    email: normalizeEmail(emailLower),
    phone: normalizePhone(phone),
    first_seen_at: new Date().toISOString(),
    last_seen_at: new Date().toISOString(),
    hasContact: true,
    hasPaid: true,
    lead_status: 'converted',
  });
}

async function getOrderUuidByRazorpayId(cfg, rzId) {
  const u = `orders?select=id&razorpay_order_id=eq.${encodeURIComponent(rzId)}&limit=1`;
  const r = await supabaseRequest(cfg, 'GET', u);
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    if (Array.isArray(rows) && rows[0]?.id) return String(rows[0].id);
  }
  return null;
}

async function bumpCustomerSpend(cfg, customerId, amountPaise) {
  const u = `customers?id=eq.${encodeURIComponent(customerId)}&select=total_spent_paise,first_paid_at,is_paying_customer`;
  const r = await supabaseRequest(cfg, 'GET', u);
  let prev = 0;
  let firstPaid = null;
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    if (Array.isArray(rows) && rows[0]) {
      prev = parseInt(rows[0].total_spent_paise ?? 0, 10);
      firstPaid = rows[0].first_paid_at ?? null;
    }
  }
  const now = new Date().toISOString();
  const patch = {
    is_paying_customer: true,
    first_paid_at: firstPaid || now,
    total_spent_paise: prev + amountPaise,
  };
  await supabaseRequest(
    cfg,
    'PATCH',
    `customers?id=eq.${encodeURIComponent(customerId)}`,
    JSON.stringify(patch),
    'return=minimal'
  );
}

async function linkLeadAndAbandon(cfg, orderUuid, notes) {
  const leadId = String(notes.lead_id ?? '').trim();
  const checkoutSessionId = String(notes.checkout_session_id ?? '').trim();

  if (leadId !== '') {
    await supabaseRequest(
      cfg,
      'PATCH',
      `leads?id=eq.${encodeURIComponent(leadId)}`,
      JSON.stringify({
        converted_order_id: orderUuid,
        lead_status: 'converted',
        last_seen_at: new Date().toISOString(),
      }),
      'return=minimal'
    );
  }

  if (checkoutSessionId === '') return;

  const find = `abandoned_checkouts?select=id&checkout_session_id=eq.${encodeURIComponent(checkoutSessionId)}&limit=1`;
  const fr = await supabaseRequest(cfg, 'GET', find);
  let abandonId = null;
  if (fr.code >= 200 && fr.code < 300) {
    const rows = JSON.parse(fr.body || '[]');
    if (Array.isArray(rows) && rows[0]?.id) abandonId = String(rows[0].id);
  }
  if (abandonId) {
    await supabaseRequest(
      cfg,
      'PATCH',
      `abandoned_checkouts?id=eq.${encodeURIComponent(abandonId)}`,
      JSON.stringify({
        stage: 'converted',
        converted_order_id: orderUuid,
        converted_at: new Date().toISOString(),
        last_event_at: new Date().toISOString(),
      }),
      'return=minimal'
    );
  }
}

export async function upsertPaidOrder(cfg, razorpayOrderId, razorpayPaymentId) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { ok: false, skipped: true, error: 'Supabase not configured' };
  }

  let order;
  try {
    order = await razorpayFetchOrder(cfg, razorpayOrderId);
  } catch (e) {
    return { ok: false, error: e.message };
  }

  const notes = normalizeNotes(order.notes);
  const emailRaw = String(notes.customer_email ?? '')
    .trim()
    .toLowerCase();
  if (!emailRaw) {
    return { ok: false, error: 'Order notes missing customer_email' };
  }

  const existedBefore = (await getOrderUuidByRazorpayId(cfg, razorpayOrderId)) !== null;

  const name = String(notes.customer_name ?? '').trim();
  const phone = String(notes.customer_phone ?? '').replace(/\s+/g, '');
  const amountPaise = Math.round(Number(order.amount ?? 0));
  const currency = strTrim(String(order.currency ?? 'INR'), 8);
  const leadIdStr = String(notes.lead_id ?? '').trim();
  const checkoutSessionStr = String(notes.checkout_session_id ?? '').trim();

  const customerId = await ensureCustomer(cfg, name, emailRaw, phone || null);

  let abandonedCheckoutId = null;
  if (checkoutSessionStr !== '') {
    const aq = `abandoned_checkouts?select=id&checkout_session_id=eq.${encodeURIComponent(checkoutSessionStr)}&limit=1`;
    const ar = await supabaseRequest(cfg, 'GET', aq);
    if (ar.code >= 200 && ar.code < 300) {
      const rows = JSON.parse(ar.body || '[]');
      if (Array.isArray(rows) && rows[0]?.id) abandonedCheckoutId = String(rows[0].id);
    }
  }

  let leadUuid = null;
  if (leadIdStr && uuidOk(leadIdStr)) {
    const lq = `leads?select=id&id=eq.${encodeURIComponent(leadIdStr)}&limit=1`;
    const lr = await supabaseRequest(cfg, 'GET', lq);
    if (lr.code >= 200 && lr.code < 300) {
      const rows = JSON.parse(lr.body || '[]');
      if (Array.isArray(rows) && rows[0]?.id) leadUuid = String(rows[0].id);
    }
  }

  const personProfileId = await resolveProfileForOrder(cfg, name, emailRaw, phone || null, leadUuid);

  const row = {
    customer_id: customerId,
    person_profile_id: personProfileId,
    lead_id: leadUuid,
    abandoned_checkout_id: abandonedCheckoutId,
    product_slug: strTrim(String(notes.product ?? 'premium_kundli_report'), 128),
    razorpay_order_id: razorpayOrderId,
    razorpay_payment_id: razorpayPaymentId,
    receipt: order.receipt != null ? strTrim(String(order.receipt), 128) : null,
    amount_paise: amountPaise,
    currency,
    payment_status: 'paid',
    order_status: 'new',
    dob: parseDob(notes.dob),
    tob: parseTob(notes.tob),
    gender: notes.gender != null ? strTrim(String(notes.gender), 32) : null,
    birth_place: notes.birth_place != null ? strTrim(String(notes.birth_place), 500) : null,
    language: notes.language != null ? strTrim(String(notes.language), 64) : null,
    coupon: notes.coupon != null ? strTrim(String(notes.coupon), 128) : null,
    razorpay_notes: notes,
    paid_at: new Date().toISOString(),
  };

  const prefer = 'resolution=merge-duplicates,return=minimal';
  const up = await supabaseRequest(
    cfg,
    'POST',
    `orders?on_conflict=razorpay_order_id`,
    JSON.stringify(row),
    prefer
  );
  if (up.code < 200 || up.code >= 300) {
    return { ok: false, error: (up.body || '').slice(0, 500) };
  }

  const orderUuid = await getOrderUuidByRazorpayId(cfg, razorpayOrderId);
  if (orderUuid) {
    await linkLeadAndAbandon(cfg, orderUuid, notes);
    if (personProfileId) await attachProfileToRecord(cfg, 'orders', orderUuid, personProfileId);
  }

  if (customerId && !existedBefore) {
    await bumpCustomerSpend(cfg, customerId, amountPaise);
  }

  if (personProfileId) {
    if (customerId) await attachProfileToRecord(cfg, 'customers', customerId, personProfileId);
    if (leadUuid) await attachProfileToRecord(cfg, 'leads', leadUuid, personProfileId);
    if (abandonedCheckoutId) await attachProfileToRecord(cfg, 'abandoned_checkouts', abandonedCheckoutId, personProfileId);
    await refreshPersonProfileStats(cfg, personProfileId);
  }

  return { ok: true };
}
