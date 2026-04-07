/**
 * VedGuide unified tracking — all events go through /api/track/* (v2 DB).
 * Load once per page: <script src="/js/vedguide-track.js" defer></script>
 */
(function (global) {
  var STORAGE_SID = 'vedguide_sid';
  var STORAGE_SID_LEGACY = 'sm_site_session';
  var STORAGE_TS = 'vedguide_session_start_ts';
  /** Stable per-browser id (survives session_id rotation); one device ≈ one client_id. */
  var STORAGE_CLIENT_ID = 'vedguide_client_id';

  function storageGet(key, store) {
    try {
      return store.getItem(key);
    } catch (e) {
      return null;
    }
  }
  function storageSet(key, val, store) {
    try {
      store.setItem(key, val);
    } catch (e) {}
  }

  /**
   * One id per browser profile (localStorage) so all tabs share the same visitor row.
   * sessionStorage alone created duplicate visitors when opening links in new tabs.
   */
  function getOrCreateSid() {
    var sid =
      storageGet(STORAGE_SID, global.localStorage) ||
      storageGet(STORAGE_SID, global.sessionStorage);
    if (!sid) {
      var legacy =
        storageGet(STORAGE_SID_LEGACY, global.sessionStorage) ||
        storageGet(STORAGE_SID_LEGACY, global.localStorage);
      if (legacy) sid = legacy;
    }
    if (!sid) {
      sid = 's_' + Date.now() + '_' + Math.random().toString(36).slice(2, 11);
      storageSet(STORAGE_TS, String(Date.now()), global.sessionStorage);
    }
    storageSet(STORAGE_SID, sid, global.localStorage);
    storageSet(STORAGE_SID, sid, global.sessionStorage);
    storageSet(STORAGE_SID_LEGACY, sid, global.sessionStorage);
    storageSet(STORAGE_SID_LEGACY, sid, global.localStorage);
    global._vedguideSid = sid;
    return sid;
  }

  function getOrCreateClientId() {
    var cid = storageGet(STORAGE_CLIENT_ID, global.localStorage);
    if (!cid) {
      cid = 'c_' + Date.now() + '_' + Math.random().toString(36).slice(2, 12);
      storageSet(STORAGE_CLIENT_ID, cid, global.localStorage);
    }
    global._vedguideClientId = cid;
    return cid;
  }

  function captureUtmFromUrl() {
    try {
      var params = new URLSearchParams(global.location.search);
      ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
        var v = params.get(k);
        if (v && !sessionStorage.getItem('vedguide_' + k)) sessionStorage.setItem('vedguide_' + k, v);
      });
    } catch (e) {}
  }

  function utmPayload() {
    var o = {};
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
      var v = sessionStorage.getItem('vedguide_' + k);
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
      client_id: getOrCreateClientId(),
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

  /** Avoid double page_view from duplicate init() or fast reloads (same path ~3s). */
  var _lastPvPath = null;
  var _lastPvTs = 0;

  function trackPageView(opts) {
    opts = opts || {};
    var path = opts.path || global.location.pathname + global.location.search;
    var now = Date.now();
    if (path === _lastPvPath && now - _lastPvTs < 3200) {
      return Promise.resolve();
    }
    _lastPvPath = path;
    _lastPvTs = now;
    var o = utmPayload();
    return postJson('/api/track/lead-event', {
      session_id: getOrCreateSid(),
      client_id: getOrCreateClientId(),
      event_type: opts.eventType || 'site',
      event_name: 'page_view',
      path: path,
      referrer: global.document.referrer || null,
      document_referrer: global.document.referrer || null,
      utm_source: o.utm_source || null,
      utm_medium: o.utm_medium || null,
      utm_campaign: o.utm_campaign || null,
      utm_content: o.utm_content || null,
      utm_term: o.utm_term || null,
      meta: Object.assign(
        {
          page: opts.pageKey || '',
          title: opts.title || '',
          visibility: typeof document !== 'undefined' ? document.visibilityState : null,
        },
        opts.meta || {}
      ),
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
      client_id: getOrCreateClientId(),
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
    getClientId: getOrCreateClientId,
  };

  global.VedGuideTrack = api;
  global._vedguideTrack = function (et, pl) {
    return track(et, pl || {}, {});
  };
})(typeof window !== 'undefined' ? window : this);
