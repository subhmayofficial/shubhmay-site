import { DateTime } from 'luxon';
import { supabaseRequest } from './supabase.js';
import { razorpayCreateOrder, verifyPaymentSignature } from './razorpay.js';
import { normalizeEmail, normalizePhone, strTrim, uuidOk } from './strings.js';
import { attachProfileToRecord, ensurePersonProfile, refreshPersonProfileStats } from './personProfiles.js';

const TZ = 'Asia/Kolkata';

export function consultancyPlans() {
  return {
    consult_15: {
      code: 'consult_15',
      name: '15-Min Consultation',
      durationMinutes: 15,
      amountPaise: 149900,
      currency: 'INR',
    },
    consult_45: {
      code: 'consult_45',
      name: '45-Min Life Reading',
      durationMinutes: 45,
      amountPaise: 499900,
      currency: 'INR',
    },
  };
}

export function resolvePlan(code) {
  const plans = consultancyPlans();
  const c = String(code ?? '').trim();
  return plans[c] || plans.consult_15;
}

export function getCandidateSlots(days = 7) {
  const tz = TZ;
  const now = DateTime.now().setZone(tz);
  const slotsPerDay = [
    { hh: '10', mm: '00', label: '10:00 AM' },
    { hh: '12', mm: '00', label: '12:00 PM' },
    { hh: '15', mm: '00', label: '03:00 PM' },
    { hh: '18', mm: '00', label: '06:00 PM' },
  ];
  const out = [];
  const unixNow = Math.floor(Date.now() / 1000);
  for (let i = 0; i < days; i++) {
    const day = now.plus({ days: i });
    const dateStr = day.toFormat('yyyy-MM-dd');
    const dayRow = {
      date: dateStr,
      label: day.toFormat('ccc, dd LLL'),
      slots: [],
    };
    for (const s of slotsPerDay) {
      const start = DateTime.fromISO(`${dateStr}T${s.hh}:${s.mm}:00`, { zone: tz });
      const startIso = start.toISO();
      const endIso = start.plus({ hours: 1 }).toISO();
      if (Math.floor(start.toSeconds()) < unixNow + 30 * 60) continue;
      dayRow.slots.push({
        key: startIso,
        label: s.label,
        startIso,
        endIso,
        available: true,
      });
    }
    if (dayRow.slots.length > 0) out.push(dayRow);
  }
  return out;
}

async function getBookedMap(cfg, minIso, maxIso) {
  const u =
    `consultancy_bookings?select=slot_start,status&status=eq.confirmed&slot_start=gte.${encodeURIComponent(minIso)}&slot_start=lte.${encodeURIComponent(maxIso)}&limit=1000`;
  const r = await supabaseRequest(cfg, 'GET', u);
  const map = {};
  if (r.code >= 200 && r.code < 300) {
    const rows = JSON.parse(r.body || '[]');
    if (Array.isArray(rows)) {
      for (const x of rows) {
        if (x.slot_start) map[String(x.slot_start)] = true;
      }
    }
  }
  return map;
}

export async function handleConsultancyConfig(cfg) {
  const plans = [];
  for (const p of Object.values(consultancyPlans())) {
    plans.push({
      code: p.code,
      name: p.name,
      durationMinutes: p.durationMinutes,
      amountPaise: p.amountPaise,
      amountRupees: p.amountPaise / 100,
      currency: p.currency,
    });
  }
  return {
    status: 200,
    json: {
      ok: true,
      plans,
      razorpayReady: Boolean(cfg.razorpayKeyId && cfg.razorpayKeySecret),
      razorpayKeyId: cfg.razorpayKeyId || null,
      timezone: TZ,
    },
  };
}

export async function handleConsultancySlots(cfg) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { status: 503, json: { ok: false, error: 'Supabase not configured' } };
  }
  const days = getCandidateSlots(7);
  const all = [];
  for (const d of days) {
    for (const s of d.slots) all.push(s);
  }
  if (all.length === 0) {
    return { status: 200, json: { ok: true, timezone: TZ, days: [] } };
  }
  const minIso = all[0].startIso;
  const maxIso = all[all.length - 1].endIso;
  const booked = await getBookedMap(cfg, minIso, maxIso);
  const filteredDays = [];
  for (const d of days) {
    const slots = d.slots
      .map((s) => ({ ...s, available: !booked[s.startIso] }))
      .filter((s) => s.available);
    if (slots.length > 0) filteredDays.push({ ...d, slots });
  }
  return { status: 200, json: { ok: true, timezone: TZ, days: filteredDays } };
}

export async function handleConsultancyOrder(cfg, body) {
  if (!cfg.razorpayKeyId || !cfg.razorpayKeySecret) {
    return { status: 503, json: { ok: false, error: 'Razorpay not configured' } };
  }
  const plan = resolvePlan(body.plan_code);
  const slotStart = String(body.slot_start ?? '').trim();
  if (!slotStart) {
    return { status: 400, json: { ok: false, error: 'slot_start required' } };
  }
  let order;
  try {
    order = await razorpayCreateOrder(cfg, {
      amount: plan.amountPaise,
      currency: plan.currency,
      receipt: `consult_${Math.floor(Date.now() / 1000)}`,
      notes: {
        product: 'consultancy',
        plan_code: plan.code,
        duration_minutes: String(plan.durationMinutes),
        slot_start: slotStart,
        customer_name: strTrim(body.name, 120),
        customer_email: body.email != null ? String(body.email).trim().toLowerCase() : null,
        customer_phone: body.phone != null ? strTrim(String(body.phone).replace(/\s+/g, ''), 20) : null,
      },
    });
  } catch (e) {
    return { status: 502, json: { ok: false, error: e.message } };
  }
  return {
    status: 200,
    json: {
      ok: true,
      keyId: cfg.razorpayKeyId,
      orderId: order.id,
      amount: order.amount,
      currency: order.currency ?? plan.currency,
      plan: {
        code: plan.code,
        name: plan.name,
        durationMinutes: plan.durationMinutes,
        amountPaise: plan.amountPaise,
      },
    },
  };
}

export async function handleConsultancyBook(cfg, body) {
  if (!cfg.supabaseUrl || !cfg.serviceRoleKey) {
    return { status: 503, json: { ok: false, error: 'Supabase not configured' } };
  }
  const name = strTrim(body.name, 500);
  const email = body.email != null ? String(body.email).trim().toLowerCase() : null;
  const phone = body.phone != null ? strTrim(String(body.phone).replace(/\s+/g, ''), 20) : null;
  const slotStart = body.slot_start != null ? String(body.slot_start).trim() : '';
  const plan = resolvePlan(body.plan_code);
  const rzOrderId = String(body.razorpay_order_id ?? '').trim();
  const rzPaymentId = String(body.razorpay_payment_id ?? '').trim();
  const rzSignature = String(body.razorpay_signature ?? '').trim();
  if (!name || !email || !phone || !slotStart || !rzOrderId || !rzPaymentId || !rzSignature) {
    return {
      status: 400,
      json: {
        ok: false,
        error:
          'name, email, phone, slot_start, razorpay_order_id, razorpay_payment_id, razorpay_signature required',
      },
    };
  }
  const secret = cfg.razorpayKeySecret;
  if (!verifyPaymentSignature(rzOrderId, rzPaymentId, rzSignature, secret)) {
    return { status: 400, json: { ok: false, error: 'Invalid payment signature' } };
  }
  const slotEnd = DateTime.fromISO(slotStart).plus({ hours: 1 }).toISO();
  const row = {
    lead_id:
      body.lead_id != null && uuidOk(String(body.lead_id)) ? String(body.lead_id) : null,
    session_id: strTrim(body.session_id, 120),
    name,
    email,
    phone,
    topic: strTrim(body.topic, 500),
    notes: strTrim(body.notes, 2000),
    slot_start: slotStart,
    slot_end: slotEnd,
    timezone: TZ,
    status: 'confirmed',
    plan_code: plan.code,
    plan_name: plan.name,
    duration_minutes: plan.durationMinutes,
    amount_paise: plan.amountPaise,
    currency: plan.currency,
    razorpay_order_id: rzOrderId,
    razorpay_payment_id: rzPaymentId,
    payment_status: 'paid',
    meta: {
      source_page: strTrim(body.source_page, 300),
      landing_path: strTrim(body.landing_path, 1000),
      referrer: strTrim(body.referrer, 2000),
      utm_source: strTrim(body.utm_source, 128),
      utm_medium: strTrim(body.utm_medium, 128),
      utm_campaign: strTrim(body.utm_campaign, 256),
      utm_content: strTrim(body.utm_content, 256),
      utm_term: strTrim(body.utm_term, 256),
    },
  };
  const personProfileId = await ensurePersonProfile(cfg, {
    name,
    email: normalizeEmail(email),
    phone: normalizePhone(phone),
    first_seen_at: new Date().toISOString(),
    last_seen_at: new Date().toISOString(),
    first_touch_path: row.meta.landing_path,
    first_touch_source: row.meta.source_page,
    first_touch_referrer: row.meta.referrer,
    last_touch_path: row.meta.landing_path,
    last_touch_source: row.meta.source_page,
    last_touch_referrer: row.meta.referrer,
    hasContact: true,
    hasPaid: true,
    lead_status: 'converted',
    lifecycle_stage: 'customer',
  });
  if (personProfileId) row.person_profile_id = personProfileId;
  const ins = await supabaseRequest(cfg, 'POST', 'consultancy_bookings', JSON.stringify(row), 'return=minimal');
  if (ins.code >= 200 && ins.code < 300) {
    if (personProfileId && row.lead_id) await attachProfileToRecord(cfg, 'leads', row.lead_id, personProfileId);
    if (personProfileId) {
      const ref = await supabaseRequest(
        cfg,
        'GET',
        `consultancy_bookings?razorpay_order_id=eq.${encodeURIComponent(rzOrderId)}&select=id&limit=1`
      );
      if (ref.code >= 200 && ref.code < 300) {
        const rows = JSON.parse(ref.body || '[]');
        if (rows[0]?.id) await attachProfileToRecord(cfg, 'consultancy_bookings', rows[0].id, personProfileId);
      }
      await refreshPersonProfileStats(cfg, personProfileId);
    }
    return { status: 200, json: { ok: true, slot_start: slotStart, timezone: TZ } };
  }
  const b = ins.body || '';
  if (ins.code === 409 || b.includes('consultancy_bookings_slot_start_uidx')) {
    return { status: 409, json: { ok: false, error: 'Slot just got booked. Please pick another slot.' } };
  }
  return { status: 502, json: { ok: false, error: b.slice(0, 300) || `HTTP ${ins.code}` } };
}
