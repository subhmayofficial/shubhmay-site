import { createHmac, timingSafeEqual } from 'crypto';

function basicAuth(keyId, keySecret) {
  return Buffer.from(`${keyId}:${keySecret}`).toString('base64');
}

export async function razorpayCreateOrder(cfg, payload) {
  const res = await fetch('https://api.razorpay.com/v1/orders', {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basicAuth(cfg.razorpayKeyId, cfg.razorpayKeySecret)}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(25_000),
  });
  const data = await res.json().catch(() => ({}));
  if (res.status < 200 || res.status >= 300) {
    const msg =
      data?.error?.description ||
      data?.error?.reason ||
      data?.message ||
      `Razorpay HTTP ${res.status}`;
    throw new Error(typeof msg === 'string' ? msg : 'Razorpay order failed');
  }
  return data;
}

export async function razorpayFetchOrder(cfg, orderId) {
  const res = await fetch(`https://api.razorpay.com/v1/orders/${encodeURIComponent(orderId)}`, {
    headers: {
      Authorization: `Basic ${basicAuth(cfg.razorpayKeyId, cfg.razorpayKeySecret)}`,
    },
    signal: AbortSignal.timeout(25_000),
  });
  const data = await res.json().catch(() => ({}));
  if (res.status < 200 || res.status >= 300) {
    throw new Error('Failed to fetch Razorpay order');
  }
  return data;
}

export function verifyPaymentSignature(orderId, paymentId, signature, secret) {
  const body = `${orderId}|${paymentId}`;
  const expected = createHmac('sha256', secret).update(body).digest('hex');
  try {
    const a = Buffer.from(expected, 'utf8');
    const b = Buffer.from(signature, 'utf8');
    return a.length === b.length && timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

export function verifyWebhookSignature(rawBody, receivedSig, secret) {
  if (!secret || !receivedSig) return false;
  const expected = createHmac('sha256', secret).update(rawBody).digest('hex');
  try {
    const a = Buffer.from(expected, 'utf8');
    const b = Buffer.from(String(receivedSig).trim(), 'utf8');
    return a.length === b.length && timingSafeEqual(a, b);
  } catch {
    return false;
  }
}
