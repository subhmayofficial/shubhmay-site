/**
 * VedGuide unified tracking — all events go through /api/track/* (v2 DB).
 * Load once per page: <script src="/js/shubhmay-track.js" defer></script>
 */
(function (global) {
  var STORAGE_SID = 'shubhmay_sid';
  var STORAGE_TS = 'shubhmay_session_start_ts';

  function getOrCreateSid() {
    var sid = sessionStorage.getItem(STORAGE_SID);
    if (!sid) {
      sid = 's_' + Date.now() + '_' + Math.random().toString(36).slice(2, 11);
      sessionStorage.setItem(STORAGE_SID, sid);
      sessionStorage.setItem(STORAGE_TS, String(Date.now()));
    }
    global._shubhmaySid = sid;
    return sid;
  }

  function captureUtmFromUrl() {
    try {
      var params = new URLSearchParams(global.location.search);
      ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
        var v = params.get(k);
        if (v && !sessionStorage.getItem('shubhmay_' + k)) sessionStorage.setItem('shubhmay_' + k, v);
      });
    } catch (e) {}
  }

  function utmPayload() {
    var o = {};
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
      var v = sessionStorage.getItem('shubhmay_' + k);
      if (v) o[k] = v;
    });
    return o;
  }

  function postJson(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      keepalive: true,
    }).catch(function () {});
  }

  function baseLeadBody() {
    var o = utmPayload();
    return {
      session_id: getOrCreateSid(),
      source_page: 'site',
      landing_path: global.location.pathname + global.location.search,
      referrer: global.document.referrer || null,
      document_referrer: global.document.referrer || null,
      user_agent: navigator.userAgent,
      client_language: navigator.language,
      screen_width: typeof screen !== 'undefined' && screen.width ? screen.width : null,
      screen_height: typeof screen !== 'undefined' && screen.height ? screen.height : null,
      utm_source: o.utm_source || null,
      utm_medium: o.utm_medium || null,
      utm_campaign: o.utm_campaign || null,
      utm_content: o.utm_content || null,
      utm_term: o.utm_term || null,
    };
  }

  /**
   * @param {object} opts - sourcePage, skipPageView, eventType (default 'site')
   */
  function init(opts) {
    opts = opts || {};
    captureUtmFromUrl();
    getOrCreateSid();
    var body = baseLeadBody();
    body.source_page = opts.sourcePage || opts.source_page || body.source_page;
    if (opts.landingPath) body.landing_path = opts.landingPath;
    return postJson('/api/track/lead', body).then(function () {
      if (opts.skipPageView) return;
      return trackPageView(opts);
    });
  }

  function trackPageView(opts) {
    opts = opts || {};
    var o = utmPayload();
    return postJson('/api/track/lead-event', {
      session_id: getOrCreateSid(),
      event_type: opts.eventType || 'site',
      event_name: 'page_view',
      path: opts.path || global.location.pathname + global.location.search,
      referrer: global.document.referrer || null,
      document_referrer: global.document.referrer || null,
      utm_source: o.utm_source || null,
      utm_medium: o.utm_medium || null,
      utm_campaign: o.utm_campaign || null,
      utm_content: o.utm_content || null,
      utm_term: o.utm_term || null,
      meta: opts.meta || { page: opts.pageKey || '', title: opts.title || '' },
    });
  }

  /**
   * @param {string} eventName
   * @param {object} payload - merged into meta
   * @param {object} opts - eventType, path
   */
  function track(eventName, payload, opts) {
    opts = opts || {};
    var o = utmPayload();
    var meta = Object.assign({}, payload || {});
    return postJson('/api/track/lead-event', {
      session_id: getOrCreateSid(),
      event_type: opts.eventType || 'site',
      event_name: String(eventName || 'event').slice(0, 128),
      path: opts.path || global.location.pathname + global.location.search,
      referrer: global.document.referrer || null,
      document_referrer: global.document.referrer || null,
      utm_source: o.utm_source || null,
      utm_medium: o.utm_medium || null,
      utm_campaign: o.utm_campaign || null,
      utm_content: o.utm_content || null,
      utm_term: o.utm_term || null,
      meta: meta,
    });
  }

  function submitLead(fields) {
    var body = Object.assign(baseLeadBody(), fields || {});
    return postJson('/api/track/lead', body);
  }

  var api = {
    init: init,
    track: track,
    trackPageView: trackPageView,
    submitLead: submitLead,
    getSessionId: getOrCreateSid,
  };

  global.ShubhmayTrack = api;
  global._shubhmayTrack = function (et, pl) {
    return track(et, pl || {}, {});
  };
})(typeof window !== 'undefined' ? window : this);
