import dotenv from 'dotenv';

dotenv.config();

export function loadConfig() {
  return {
    supabaseUrl: (process.env.SUPABASE_URL || '').replace(/\/$/, ''),
    serviceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
    anonKey: process.env.SUPABASE_ANON_KEY || '',
    schema: process.env.SUPABASE_SCHEMA || 'v2',
    razorpayKeyId: process.env.RAZORPAY_KEY_ID || '',
    razorpayKeySecret: process.env.RAZORPAY_KEY_SECRET || '',
    razorpayWebhookSecret: process.env.RAZORPAY_WEBHOOK_SECRET || '',
    kundliAmountPaise: parseInt(process.env.KUNDLI_AMOUNT_PAISE || '49900', 10),
    currency: process.env.CURRENCY || 'INR',
    googleMapsBrowserKey: process.env.GOOGLE_MAPS_BROWSER_KEY || '',
    adminSecret: process.env.ADMIN_SECRET || '',
    port: parseInt(process.env.PORT || '3000', 10),
  };
}
