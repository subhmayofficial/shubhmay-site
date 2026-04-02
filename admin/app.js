(function () {
  var STORAGE_KEY = 'shubhmay_admin_secret';
  var titles = {
    dashboard: 'Dashboard',
    orders: 'Orders',
    customers: 'Customers',
    visitors: 'Site Visitors',
    leads: 'Contacts',
    analytics: 'Page Traffic',
    abandoned: 'Left Without Paying',
    settings: 'Settings',
  };

  var currentPanel = 'dashboard';
  var adminConnected = false;
  var lastVerifiedSecret = '';
  var serverAdminApiEnabled = true;

  function migrateStorage() {
    try {
      var ss = sessionStorage.getItem(STORAGE_KEY);
      if (ss && !localStorage.getItem(STORAGE_KEY)) {
        localStorage.setItem(STORAGE_KEY, ss);
        sessionStorage.removeItem(STORAGE_KEY);
      }
    } catch (e) {}
  }

  function loadStoredSecret() {
    try {
      return localStorage.getItem(STORAGE_KEY) || '';
    } catch (e) {
      return '';
    }
  }

  function saveStoredSecret(s) {
    try {
      if (s) localStorage.setItem(STORAGE_KEY, s);
      else localStorage.removeItem(STORAGE_KEY);
    } catch (e) {}
  }

  var secretInput = document.getElementById('adminSecretInput');
  var connectBtn = document.getElementById('adminConnectBtn');
  var disconnectBtn = document.getElementById('adminDisconnectBtn');

  migrateStorage();
  if (secretInput) {
    var stored = loadStoredSecret();
    if (stored) secretInput.value = stored;
  }

  function getSecret() {
    return secretInput ? secretInput.value.trim() : '';
  }

  function canAuth() {
    return adminConnected && lastVerifiedSecret && getSecret() === lastVerifiedSecret;
  }

  function authHeaders() {
    return { 'X-Admin-Secret': getSecret() };
  }

  function updateDisconnectBtn() {
    if (disconnectBtn) disconnectBtn.classList.toggle('hidden', !canAuth());
  }

  function updateAdminPill() {
    var el = document.getElementById('adminConnPill');
    if (!el) return;
    el.className = 'pill pill--admin';
    if (!serverAdminApiEnabled) {
      el.textContent = 'Admin API off on server';
      el.classList.add('pill--bad');
      return;
    }
    if (el.dataset.checking === '1') {
      el.textContent = 'Admin: verifying…';
      return;
    }
    if (canAuth()) {
      el.textContent = 'Admin: connected';
      el.classList.add('pill--ok');
    } else if (getSecret()) {
      el.textContent = 'Admin: not connected — click Connect';
      el.classList.add('pill--bad');
    } else {
      el.textContent = 'Admin: not connected';
    }
  }

  function verifyAdmin(secret) {
    return fetch('/api/admin/ping', { headers: { 'X-Admin-Secret': secret } }).then(function (r) {
      if (r.status === 503) return { ok: false, reason: 'server' };
      if (r.ok) return { ok: true };
      return { ok: false, reason: 'bad' };
    });
  }

  function applyVerifyResult(res, secret) {
    if (res.ok) {
      lastVerifiedSecret = secret;
      adminConnected = true;
      saveStoredSecret(secret);
    } else {
      adminConnected = false;
      lastVerifiedSecret = '';
      if (res.reason === 'bad') {
        saveStoredSecret('');
        if (secretInput) secretInput.value = '';
      }
    }
    updateAdminPill();
    updateDisconnectBtn();
  }

  function readRouteHash() {
    var m = /^#\/([^/?]+)/.exec(location.hash || '');
    var id = m && m[1] ? m[1] : 'dashboard';
    return titles[id] ? id : 'dashboard';
  }

  function setRouteHash(id) {
    if (!titles[id]) id = 'dashboard';
    var h = '#/' + id;
    if (location.hash === h) return;
    location.hash = h;
  }

  function showPanel(id, skipHash) {
    if (!titles[id]) id = 'dashboard';
    currentPanel = id;
    document.querySelectorAll('main section.panel').forEach(function (el) {
      var pid = (el.id || '').replace(/^panel-/, '');
      if (pid === id) el.classList.remove('hidden');
      else el.classList.add('hidden');
    });
    document.querySelectorAll('.nav-item[data-panel]').forEach(function (a) {
      a.classList.toggle('active', a.getAttribute('data-panel') === id);
    });
    var t = document.getElementById('pageTitle');
    if (t) t.textContent = titles[id] || 'Admin';
    if (!skipHash) setRouteHash(id);
    loadCurrentPanel();
  }

  document.querySelectorAll('.nav-item[data-panel]').forEach(function (a) {
    a.addEventListener('click', function (e) {
      e.preventDefault();
      showPanel(a.getAttribute('data-panel'));
    });
  });

  window.addEventListener('hashchange', function () {
    var id = readRouteHash();
    if (id !== currentPanel) showPanel(id, true);
  });

  (function initAdminRoute() {
    if (!location.hash || location.hash === '#') {
      history.replaceState(null, '', location.pathname + location.search + '#/dashboard');
    }
    showPanel(readRouteHash(), true);
  })();

  if (secretInput) {
    secretInput.addEventListener('input', function () {
      if (getSecret() !== lastVerifiedSecret) {
        adminConnected = false;
      }
      updateAdminPill();
      updateDisconnectBtn();
    });
    secretInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') handleConnect();
    });
  }

  function handleConnect() {
    var secret = getSecret();
    if (!secret) {
      updateAdminPill();
      return;
    }
    var pill = document.getElementById('adminConnPill');
    if (pill) pill.dataset.checking = '1';
    updateAdminPill();
    verifyAdmin(secret).then(function (res) {
      if (pill) delete pill.dataset.checking;
      applyVerifyResult(res, secret);
      if (canAuth()) loadCurrentPanel();
    });
  }

  function handleDisconnect() {
    saveStoredSecret('');
    adminConnected = false;
    lastVerifiedSecret = '';
    if (secretInput) secretInput.value = '';
    var dashCard = document.getElementById('dashSummary');
    if (dashCard) dashCard.hidden = true;
    var dashApi = document.getElementById('dashApiSummary');
    if (dashApi) dashApi.hidden = true;
    var snapCard = document.getElementById('dashSnapshotsCard');
    if (snapCard) snapCard.hidden = true;
    updateAdminPill();
    updateDisconnectBtn();
  }

  if (connectBtn) connectBtn.addEventListener('click', handleConnect);
  if (disconnectBtn) disconnectBtn.addEventListener('click', handleDisconnect);

  function esc(s) {
    if (s == null || s === '') return '—';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtTs(iso) {
    if (!iso) return '—';
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return esc(iso);
      return esc(d.toLocaleString());
    } catch {
      return esc(iso);
    }
  }

  function fmtMoneyPaise(paise, currency) {
    if (paise == null || paise === '') return '—';
    var n = Number(paise);
    if (!Number.isFinite(n)) return '—';
    var cur = currency || 'INR';
    return '₹' + (n / 100).toFixed(2) + ' ' + cur;
  }

  function dedupeMergedTimelineEvents(merged) {
    var sorted = merged.slice().sort(function (a, b) {
      return new Date(a.created_at || 0) - new Date(b.created_at || 0);
    });
    var out = [];
    for (var i = 0; i < sorted.length; i++) {
      var ev = sorted[i];
      var prev = out[out.length - 1];
      if (prev) {
        var dt = Math.abs(new Date(ev.created_at || 0) - new Date(prev.created_at || 0));
        var sameKind =
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

  function pathToShortLabel(path) {
    var p = (path || '').split('?')[0] || '';
    if (p === '/' || p === '') return 'home';
    if (p === '/index.html') return 'home';
    if (p.indexOf('/tracking') === 0) return 'tracking test';
    return p.replace(/^\//, '') || 'page';
  }

  /** Plain English for admin timeline rows (no jargon). */
  function simpleTimelineTitle(ev) {
    var et = String(ev.event_type || '').toLowerCase();
    var en = String(ev.event_name || ev.stage || '').toLowerCase();
    var path = ev.path || '';
    var meta = ev.meta && typeof ev.meta === 'object' ? ev.meta : {};
    var pageHint = String(meta.page || '').toLowerCase();
    if (et === 'site' && en === 'page_view') {
      var pl = (path || '').toLowerCase();
      if (path === '/' || path === '/index.html' || pageHint === 'home' || pageHint === 'index') {
        return 'Visited home page';
      }
      if (path.indexOf('/tracking') === 0) {
        return 'Opened tracking test page';
      }
      if (pl.indexOf('kundli') >= 0 || pl.indexOf('/lp/kundli') >= 0) {
        return 'Viewed kundli / birth-details page';
      }
      return 'Viewed page: ' + pathToShortLabel(path);
    }
    if (et === 'checkout') {
      return 'Checkout: ' + (ev.event_name || ev.stage || 'step');
    }
    if (en === 'lead_upsert' || (et === 'lead' && en === 'lead_upsert')) {
      return 'Saved contact details (email / phone)';
    }
    if (et === 'journey' && en === 'session_touch') {
      return 'Visit recorded (session only — no contact yet)';
    }
    if (et === 'journey' && en === 'contact_shared') {
      return 'Contact details saved';
    }
    if (et === 'metal_finder' || en.indexOf('metal') >= 0) {
      return (ev.event_name || ev.event_type || 'Tool').replace(/_/g, ' ');
    }
    return (ev.event_type || 'Event') + ' · ' + (ev.event_name || ev.stage || '—');
  }

  var ordersPage = 1;
  var leadsPage = 1;
  var visitorsPage = 1;
  var customersPage = 1;
  var abandonedPage = 1;
  var analyticsPreset = 'last7';
  var analyticsSelectedPath = '';

  function dtLocalToIso(v) {
    if (!v || typeof v !== 'string') return '';
    var d = new Date(v);
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }

  function isoToDatetimeLocal(iso) {
    if (!iso) return '';
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return '';
    function pad(n) {
      return String(n).padStart(2, '0');
    }
    return (
      d.getFullYear() +
      '-' +
      pad(d.getMonth() + 1) +
      '-' +
      pad(d.getDate()) +
      'T' +
      pad(d.getHours()) +
      ':' +
      pad(d.getMinutes())
    );
  }

  var DATE_PANEL_IDS = {
    orders: ['ordersFilterDateFrom', 'ordersFilterDateTo'],
    customers: ['customersFilterDateFrom', 'customersFilterDateTo'],
    leads: ['leadsFilterDateFrom', 'leadsFilterDateTo'],
    visitors: ['visitorsFilterDateFrom', 'visitorsFilterDateTo'],
    analytics: ['analyticsDateFrom', 'analyticsDateTo'],
    abandoned: ['abandonedFilterDateFrom', 'abandonedFilterDateTo'],
  };

  /** Server sends IST day windows as ISO; datetime-local is browser-local — use raw ISO for API when a chip set these. */
  var presetDateIsoByPanel = {
    orders: null,
    customers: null,
    leads: null,
    visitors: null,
    analytics: null,
    abandoned: null,
  };

  function appendDateRangeFromInputsOrPreset(p, panelKey, idFrom, idTo) {
    var preset = presetDateIsoByPanel[panelKey];
    if (preset && preset.from && preset.to) {
      p.set('date_from', preset.from);
      p.set('date_to', preset.to);
      return;
    }
    var df = document.getElementById(idFrom);
    var dt = document.getElementById(idTo);
    var dfi = df && df.value ? dtLocalToIso(df.value) : '';
    var dti = dt && dt.value ? dtLocalToIso(dt.value) : '';
    if (dfi) p.set('date_from', dfi);
    if (dti) p.set('date_to', dti);
  }

  var dashPreset = 'today';

  function updateDashChipActive() {
    document.querySelectorAll('[data-dash-preset]').forEach(function (b) {
      b.classList.toggle('filter-chip--active', b.getAttribute('data-dash-preset') === dashPreset);
    });
  }

  function buildAnalyticsQuery() {
    var p = new URLSearchParams();
    var df = document.getElementById('dashDateFrom');
    var dt = document.getElementById('dashDateTo');
    if (df && dt && df.value && dt.value) {
      p.set('date_from', dtLocalToIso(df.value));
      p.set('date_to', dtLocalToIso(dt.value));
    } else {
      p.set('preset', dashPreset || 'today');
    }
    return p.toString();
  }

  function applyDatePresetForPanel(panel, preset) {
    if (!canAuth()) return;
    var ids = DATE_PANEL_IDS[panel];
    if (!ids) return;
    fetch('/api/admin/date-window?' + new URLSearchParams({ preset: preset }), { headers: authHeaders() })
      .then(function (r) {
        return r.json();
      })
      .then(function (w) {
        if (!w || !w.ok) return;
        if (panel === 'analytics') analyticsPreset = preset;
        presetDateIsoByPanel[panel] = { from: w.startIso, to: w.endIso };
        var df = document.getElementById(ids[0]);
        var dt = document.getElementById(ids[1]);
        if (df) df.value = isoToDatetimeLocal(w.startIso);
        if (dt) dt.value = isoToDatetimeLocal(w.endIso);
        if (panel === 'orders') {
          ordersPage = 1;
          loadOrders();
        } else if (panel === 'customers') {
          customersPage = 1;
          loadCustomers();
        } else if (panel === 'leads') {
          leadsPage = 1;
          loadLeads();
        } else if (panel === 'visitors') {
          visitorsPage = 1;
          loadVisitors();
        } else if (panel === 'analytics') {
          loadPageAnalytics();
        } else if (panel === 'abandoned') {
          abandonedPage = 1;
          loadAbandoned();
        }
      })
      .catch(function () {});
  }

  function loadSnapshotsList() {
    var ul = document.getElementById('dashSnapshotsList');
    var card = document.getElementById('dashSnapshotsCard');
    if (!ul || !card) return;
    if (!canAuth()) {
      card.hidden = true;
      return;
    }
    fetch('/api/admin/analytics/snapshots?limit=15', { headers: authHeaders() })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          ul.innerHTML = '<li class="snapshots-empty">Could not load snapshots.</li>';
          return;
        }
        var rows = j.snapshots || [];
        if (!rows.length) {
          ul.innerHTML = '<li class="snapshots-empty">No snapshots yet — use “Save snapshot” after picking a range.</li>';
          return;
        }
        ul.innerHTML = '';
        rows.forEach(function (row) {
          var li = document.createElement('li');
          li.className = 'snapshots-item';
          var p = row.payload && row.payload.period ? row.payload.period : {};
          var rev = p.revenueInr != null ? '₹' + Number(p.revenueInr).toFixed(2) : '—';
          var conv = p.leadToOrderConversionPercent != null ? p.leadToOrderConversionPercent + '% L→O' : '—';
          li.innerHTML =
            '<span class="snapshots-meta">' +
            fmtTs(row.created_at) +
            '</span> <span class="snapshots-preset">' +
            esc(row.preset || '') +
            '</span> · leads ' +
            esc(String(p.leadsCollected != null ? p.leadsCollected : '—')) +
            ' · orders ' +
            esc(String(p.ordersPaid != null ? p.ordersPaid : '—')) +
            ' · ' +
            esc(conv) +
            ' · rev ' +
            esc(rev);
          ul.appendChild(li);
        });
      })
      .catch(function () {
        ul.innerHTML = '<li class="snapshots-empty">Network error loading snapshots.</li>';
      });
  }

  function setPagination(prefix, page, perPage, total) {
    var info = document.getElementById(prefix + 'PageInfo');
    var pag = document.getElementById(prefix + 'Pagination');
    if (!info || !pag) return;
    var t = total != null ? total : 0;
    var lastPage = Math.max(1, Math.ceil(t / (perPage || 10)) || 1);
    info.textContent = 'Page ' + page + ' / ' + lastPage + ' · ' + t + ' total';
    pag.classList.toggle('hidden', t === 0);
    var prev = document.getElementById(prefix + 'PagePrev');
    var next = document.getElementById(prefix + 'PageNext');
    if (prev) prev.disabled = page <= 1;
    if (next) next.disabled = page >= lastPage;
  }

  function setMsg(el, text, isErr) {
    if (!el) return;
    el.hidden = !text;
    el.textContent = text || '';
    el.style.color = isErr ? '#e8a598' : 'var(--muted)';
  }

  var detailModal = document.getElementById('detailModal');
  var detailModalPanel = document.getElementById('detailModalPanel');
  var detailModalBody = document.getElementById('detailModalBody');
  var detailTitle = document.getElementById('detailModalTitle');
  var detailCloseTimer = null;

  function renderTimelineCardsFromEvents(evs, introP) {
    var intro = introP || '<p class="detail-muted" style="margin-bottom:10px;">What they did on the site (oldest → newest).</p>';
    if (!evs || !evs.length) {
      return sectionHtml('Site activity', intro + '<p class="detail-muted">No events stored yet.</p>');
    }
    var cards = evs
      .map(function (ev) {
        var when = fmtTs(ev.created_at);
        var src =
          ev._source === 'visitor'
            ? '<span class="badge badge--dim">Browsing</span> '
            : ev._source === 'lead'
              ? '<span class="badge badge--ok">Tracked</span> '
              : '';
        var titleText = simpleTimelineTitle(ev);
        var path = ev.path ? '<div class="timeline-event-path">' + esc(ev.path) + '</div>' : '';
        var ref = ev.referrer ? '<div class="timeline-event-utm">From: ' + esc(ev.referrer) + '</div>' : '';
        var utm = [ev.utm_source, ev.utm_medium, ev.utm_campaign].filter(Boolean).join(' · ');
        var utmLine = utm ? '<div class="timeline-event-utm">Campaign: ' + esc(utm) + '</div>' : '';
        var meta =
          ev.meta && typeof ev.meta === 'object'
            ? '<div class="timeline-event-meta">' + esc(JSON.stringify(ev.meta).slice(0, 400)) + '</div>'
            : '';
        return (
          '<article class="timeline-event-card"><div class="timeline-event-time">' +
          esc(when) +
          '</div><div class="timeline-event-title">' +
          src +
          esc(titleText) +
          '</div>' +
          path +
          ref +
          utmLine +
          meta +
          '</article>'
        );
      })
      .join('');
    return sectionHtml('Site activity', intro + '<div class="timeline-scroll">' + cards + '</div>');
  }

  /* ── Journey timeline helpers ──────────────────────────────── */

  /** Returns node class, body class, and milestone label for an event */
  function journeyStepInfo(ev) {
    var et = String(ev.event_type || '').toLowerCase();
    var en = String(ev.event_name || ev.stage || '').toLowerCase();
    if (et === 'site' && en === 'page_view') {
      return { node: 'journey-node--view', stepClass: '', tag: 'Page visit', tagClass: '' };
    }
    if (et === 'checkout') {
      return { node: 'journey-node--checkout', stepClass: '', tag: 'Checkout step', tagClass: '' };
    }
    if (en === 'lead_upsert' || en === 'contact_shared' || (et === 'journey' && en === 'contact_shared')) {
      return { node: 'journey-node--contact', stepClass: 'journey-step--milestone', tag: '\u2B50 Contact saved!', tagClass: 'journey-step-tag--contact' };
    }
    if (et === 'metal_finder' || en.indexOf('metal') >= 0) {
      return { node: 'journey-node--cta', stepClass: '', tag: 'Used tool', tagClass: '' };
    }
    if (en.indexOf('cta') >= 0 || en.indexOf('click') >= 0 || en.indexOf('button') >= 0) {
      return { node: 'journey-node--cta', stepClass: '', tag: 'CTA click', tagClass: '' };
    }
    return { node: '', stepClass: '', tag: 'Event', tagClass: '' };
  }

  /** Friendly “X time later” label between two ISO timestamps */
  function timeBetween(isoA, isoB) {
    if (!isoA || !isoB) return 'Some time later';
    var ms = new Date(isoB) - new Date(isoA);
    if (ms < 0) ms = -ms;
    var mins = Math.round(ms / 60000);
    if (mins < 2) return 'Moments later';
    if (mins < 60) return mins + ' min later';
    var hrs = Math.round(mins / 60);
    if (hrs < 24) return hrs + ' hour' + (hrs > 1 ? 's' : '') + ' later';
    var days = Math.round(hrs / 24);
    return days + ' day' + (days > 1 ? 's' : '') + ' later';
  }

  /** Build ordinal: 1→”1st”, 2→”2nd”, 3→”3rd” */
  function ordinal(n) {
    var s = ['th','st','nd','rd'];
    var v = n % 100;
    return n + (s[(v-20)%10] || s[v] || s[0]);
  }

  /**
   * Core journey renderer. evs = flat sorted array of events.
   * Pass sessionGroups=[{sessionId, events}] for session-aware rendering.
   */
  function renderJourneyTimeline(evs, heading) {
    if (!evs || !evs.length) {
      return sectionHtml(heading || 'Journey', '<p class=”detail-muted”>No activity recorded yet.</p>');
    }

    /* Track page visit counts per path for “1st visit”, “2nd visit” labels */
    var pathCounts = {};

    function stepHtml(ev, stepNum, isLast) {
      var info = journeyStepInfo(ev);
      var titleText = simpleTimelineTitle(ev);
      var path = (ev.path || '').split('?')[0];

      /* Build plain English description */
      if (path && String(ev.event_type || '').toLowerCase() === 'site' && String(ev.event_name || '').toLowerCase() === 'page_view') {
        pathCounts[path] = (pathCounts[path] || 0) + 1;
        var n = pathCounts[path];
        var pageName = path.replace(/^\//, '').replace(/\.html$/, '') || 'home';
        titleText = n === 1
          ? 'Opened \u201C' + pageName + '\u201D page for the first time'
          : 'Came back to \u201C' + pageName + '\u201D page (' + ordinal(n) + ' visit)';
      }

      var when = fmtTs(ev.created_at);
      var metaSub = path
        ? '<div class=”journey-step-meta”>' + esc(path.length > 55 ? path.slice(0, 55) + '\u2026' : path) + ' \u00B7 ' + esc(when) + '</div>'
        : '<div class=”journey-step-meta”>' + esc(when) + '</div>';

      var html = '<div class=”journey-step ' + info.stepClass + '”>';
      html += '<div class=”journey-track”>';
      html += '<div class=”journey-node ' + info.node + '”></div>';
      if (!isLast) html += '<div class=”journey-rail”></div>';
      html += '</div>';
      html += '<div class=”journey-body”>';
      html += '<div class=”journey-step-tag ' + info.tagClass + '”>' + esc(info.tag) + '</div>';
      html += '<div class=”journey-step-title”>' + esc(titleText) + '</div>';
      html += metaSub;
      html += '</div></div>';
      return html;
    }

    var html = '<div class=”journey-wrap”>';
    evs.forEach(function (ev, idx) {
      html += stepHtml(ev, idx + 1, idx === evs.length - 1);
    });
    html += '</div>';

    return sectionHtml(heading || 'Journey', html);
  }

  /** Session-grouped journey: shows session breaks with curved connectors */
  function renderSessionJourney(sessions, heading) {
    if (!sessions || !sessions.length) {
      return sectionHtml(heading || 'Journey', '<p class=”detail-muted”>No activity recorded yet.</p>');
    }

    var pathCounts = {};

    function stepHtml(ev, isLast, isGlobalLast) {
      var info = journeyStepInfo(ev);
      var titleText = simpleTimelineTitle(ev);
      var et = String(ev.event_type || '').toLowerCase();
      var en = String(ev.event_name || '').toLowerCase();
      var path = (ev.path || '').split('?')[0];

      if (path && et === 'site' && en === 'page_view') {
        pathCounts[path] = (pathCounts[path] || 0) + 1;
        var n = pathCounts[path];
        var pageName = path.replace(/^\//, '').replace(/\.html$/, '') || 'home';
        titleText = n === 1
          ? 'Opened \u201C' + pageName + '\u201D page'
          : 'Returned to \u201C' + pageName + '\u201D page (' + ordinal(n) + ' visit)';
      }

      var when = fmtTs(ev.created_at);
      var metaSub = path
        ? '<div class=”journey-step-meta”>' + esc(path.length > 55 ? path.slice(0, 55) + '\u2026' : path) + ' \u00B7 ' + esc(when) + '</div>'
        : '<div class=”journey-step-meta”>' + esc(when) + '</div>';

      var html = '<div class=”journey-step ' + info.stepClass + '”>';
      html += '<div class=”journey-track”>';
      html += '<div class=”journey-node ' + info.node + '”></div>';
      if (!isGlobalLast) html += '<div class=”journey-rail”></div>';
      html += '</div>';
      html += '<div class=”journey-body”>';
      html += '<div class=”journey-step-tag ' + info.tagClass + '”>' + esc(info.tag) + '</div>';
      html += '<div class=”journey-step-title”>' + esc(titleText) + '</div>';
      html += metaSub;
      html += '</div></div>';
      return html;
    }

    var html = '<div class=”journey-wrap”>';

    sessions.forEach(function (sess, sIdx) {
      var events = sess.events || [];
      var first = events[0] || {};
      var last = events[events.length - 1] || {};
      var isLastSession = sIdx === sessions.length - 1;

      /* Session header */
      html += '<div class=”journey-session-head”>';
      html += '<span class=”journey-session-chip”>Visit ' + (sIdx + 1) + '</span>';
      html += '<span class=”journey-session-time”>' + esc(fmtTs(first.created_at)) + '</span>';
      html += '</div>';

      /* Events */
      events.forEach(function (ev, evIdx) {
        var isGlobalLast = isLastSession && evIdx === events.length - 1;
        html += stepHtml(ev, evIdx === events.length - 1, isGlobalLast);
      });

      /* Session break connector */
      if (!isLastSession) {
        var nextFirst = (sessions[sIdx + 1].events || [])[0];
        var gap = timeBetween(last.created_at, nextFirst ? nextFirst.created_at : null);
        html += '<div class=”journey-break”>';
        html += '<div class=”journey-break-track”><div class=”journey-break-curve”></div></div>';
        html += '<div class=”journey-break-body”><span class=”journey-break-tag”>\u21A9 ' + esc(gap) + ' \u00B7 New session</span></div>';
        html += '</div>';
      }
    });

    html += '</div>';
    return sectionHtml(heading || 'Journey', html);
  }

  /** Flat-events version (used for customer activity + visitor timeline) */
  function renderHumanActivityTimeline(evs, introP) {
    if (!evs || !evs.length) {
      return sectionHtml('Journey', '<p class=”detail-muted”>No visits recorded yet.</p>');
    }
    return renderJourneyTimeline(evs, 'Journey');
  }

  function renderAbandonedContextSection(ctx) {
    if (!ctx || !ctx.ok) {
      return sectionHtml('Conversion insight', '<p class="detail-muted">Could not load.</p>');
    }
    var rep = ctx.repeatAbandoner ? '<span class="badge badge--bad">Repeat abandoner</span>' : '<span class="badge badge--dim">First abandon (this contact)</span>';
    return sectionHtml(
      'Conversion insight',
      '<p class="detail-muted" style="margin-bottom:10px;">' +
        rep +
        ' · Earlier abandon sessions (before this one): <strong>' +
        esc(String(ctx.priorAbandonSessions != null ? ctx.priorAbandonSessions : '—')) +
        '</strong></p>' +
        kvRow('Paid summary', ctx.conversionSummary) +
        (ctx.paidOrderNumber ? kvRow('Customer payment #', String(ctx.paidOrderNumber)) : '')
    );
  }

  function fillPrimaryMetricsRow(el, period, subtitle) {
    if (!el) return;
    var t = period || {};
    var tiles = [
      { label: 'Revenue (window)', value: t.revenueInr != null ? '₹' + Number(t.revenueInr).toFixed(2) : '—', sub: 'Paid orders in range' },
      { label: 'Paid orders', value: t.ordersPaid, sub: 'Razorpay paid' },
      { label: 'New leads', value: t.leadsCollected, sub: 'First touch in range' },
      { label: 'Leads → paid', value: t.leadsConverted, sub: (t.leadToOrderConversionPercent != null ? t.leadToOrderConversionPercent + '%' : '—') + ' of cohort' },
      { label: 'Visitors (active)', value: t.visitorsActiveInPeriod, sub: 'Had activity in range' },
      { label: 'Page views', value: t.pageViewsTotal, sub: 'Tracked page_view events' },
    ];
    el.innerHTML = '';
    if (subtitle) {
      var p = document.createElement('p');
      p.className = 'detail-muted';
      p.style.cssText = 'grid-column:1/-1;margin:0 0 8px;font-size:12px;';
      p.textContent = subtitle;
      el.appendChild(p);
    }
    tiles.forEach(function (x) {
      var d = document.createElement('div');
      d.className = 'primary-metric-card';
      d.innerHTML =
        '<div class="primary-metric-label">' +
        esc(x.label) +
        '</div><div class="primary-metric-value">' +
        esc(String(x.value != null ? x.value : '—')) +
        '</div><div class="primary-metric-sub">' +
        esc(x.sub || '') +
        '</div>';
      el.appendChild(d);
    });
  }

  function fetchAnalyticsParams(queryStr) {
    return fetch('/api/admin/analytics?' + queryStr, { headers: authHeaders() }).then(function (r) {
      return r.json();
    });
  }

  function loadStripToday(stripEl) {
    if (!stripEl || !canAuth()) {
      if (stripEl) stripEl.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;">Connect to load today’s funnel.</p>';
      return;
    }
    fetchAnalyticsParams('preset=today')
      .then(function (a) {
        if (!a || !a.ok) {
          stripEl.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;">Metrics unavailable.</p>';
          return;
        }
        fillPrimaryMetricsRow(stripEl, a.period || a.today, 'Today (IST midnight window)');
      })
      .catch(function () {
        stripEl.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;">Network error.</p>';
      });
  }

  function loadStripAnalyticsPanel() {
    var stripEl = document.getElementById('analyticsPrimaryStrip');
    if (!stripEl || !canAuth()) return;
    var q = buildPageAnalyticsQuery();
    Promise.all([fetchAnalyticsParams(q), fetch('/api/admin/analytics/pages?' + q, { headers: authHeaders() }).then(function (r) { return r.json(); })])
      .then(function (pair) {
        var a = pair[0];
        var pages = pair[1];
        if (!a || !a.ok) {
          stripEl.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;">Load failed.</p>';
          return;
        }
        fillPrimaryMetricsRow(stripEl, a.period || a.today, 'Same window as page table below');
        if (pages && pages.ok && Array.isArray(pages.pages) && pages.pages.length) {
          var top = pages.pages.slice().sort(function (x, y) { return (y.events || 0) - (x.events || 0); })[0];
          var hint = document.createElement('div');
          hint.className = 'primary-metric-card';
          hint.style.gridColumn = '1 / -1';
          hint.innerHTML =
            '<div class="primary-metric-label">Top page (events)</div><div class="primary-metric-value" style="font-size:15px;">' +
            esc(top.label || top.path) +
            '</div><div class="primary-metric-sub">' +
            esc(String(top.events)) +
            ' events · ' +
            esc(String(top.uniqueSessions)) +
            ' unique sessions</div>';
          stripEl.appendChild(hint);
        }
      })
      .catch(function () {
        stripEl.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;">Network error.</p>';
      });
  }

  function sectionHtml(heading, inner) {
    return (
      '<section class="detail-section"><h3 class="detail-subhead">' +
      esc(heading) +
      '</h3>' +
      inner +
      '</section>'
    );
  }

  function kvRow(label, value, mono) {
    var v = value;
    if (v == null || v === '') v = '—';
    else if (typeof v === 'object') v = JSON.stringify(v);
    var cls = 'detail-v' + (mono ? ' detail-v--mono' : '');
    return (
      '<div class="detail-kv"><span class="detail-k">' +
      esc(label) +
      '</span><span class="' +
      cls +
      '">' +
      esc(String(v)) +
      '</span></div>'
    );
  }

  function flatObjectKv(obj) {
    if (!obj || typeof obj !== 'object') return kvRow('—', '');
    var keys = Object.keys(obj);
    if (!keys.length) return kvRow('(empty)', '—');
    return keys
      .map(function (k) {
        var val = obj[k];
        if (val && typeof val === 'object') val = JSON.stringify(val);
        return kvRow(k, val, true);
      })
      .join('');
  }

  function renderOrderDetail(row) {
    var o = row || {};
    var cust = o.customers && typeof o.customers === 'object' ? o.customers : null;
    var lead = o.leads && typeof o.leads === 'object' ? o.leads : null;
    var ab = o.abandoned_checkouts && typeof o.abandoned_checkouts === 'object' ? o.abandoned_checkouts : null;
    var notes = o.razorpay_notes;

    var orderBlock =
      kvRow('Order ID', o.id, true) +
      kvRow(
        'Acquisition',
        o.lead_id
          ? 'Lead-attributed (session tracked before checkout)'
          : 'Direct purchase — no lead_id on order (guest / email-only checkout)'
      ) +
      kvRow('Product', o.product_slug) +
      kvRow('Order status', o.order_status) +
      kvRow('Payment status', o.payment_status) +
      kvRow('Amount', fmtMoneyPaise(o.amount_paise, o.currency)) +
      kvRow('Currency', o.currency) +
      kvRow('Razorpay order ID', o.razorpay_order_id, true) +
      kvRow('Razorpay payment ID', o.razorpay_payment_id, true) +
      kvRow('Receipt', o.receipt) +
      kvRow('Date of birth', o.dob) +
      kvRow('Time of birth', o.tob) +
      kvRow('Birth place', o.birth_place) +
      kvRow('Report language', o.language) +
      kvRow('Coupon', o.coupon) +
      kvRow('Created at', fmtTs(o.created_at)) +
      kvRow('Updated at', fmtTs(o.updated_at)) +
      kvRow('Paid at', fmtTs(o.paid_at)) +
      kvRow('Customer ID (FK)', o.customer_id, true) +
      kvRow('Lead ID (FK)', o.lead_id, true) +
      kvRow('Abandoned checkout ID (FK)', o.abandoned_checkout_id, true);

    var html = sectionHtml('Order', orderBlock);

    if (cust) {
      html +=
        sectionHtml(
          'Linked customer',
          kvRow('Email', cust.email) +
            kvRow('Name', cust.name) +
            kvRow('Phone', cust.phone) +
            kvRow('Paying customer', cust.is_paying_customer ? 'Yes' : 'No') +
            kvRow('First paid', fmtTs(cust.first_paid_at)) +
            kvRow('Total spent', fmtMoneyPaise(cust.total_spent_paise, 'INR')) +
            kvRow('Customer since', fmtTs(cust.created_at))
        );
    }

    if (lead) {
      html +=
        sectionHtml(
          'Lead & tracking',
          kvRow('Session ID', lead.session_id, true) +
            kvRow('Lead status', lead.lead_status) +
            kvRow('Email', lead.email) +
            kvRow('Name', lead.name) +
            kvRow('Phone', lead.phone) +
            kvRow('UTM source', lead.utm_source) +
            kvRow('UTM medium', lead.utm_medium) +
            kvRow('UTM campaign', lead.utm_campaign) +
            kvRow('Landing path', lead.landing_path) +
            kvRow('Referrer', lead.referrer) +
            kvRow('Source page', lead.source_page) +
            kvRow('First seen', fmtTs(lead.first_seen_at)) +
            kvRow('Last seen', fmtTs(lead.last_seen_at))
        );
    }

    if (ab) {
      html +=
        sectionHtml(
          'Abandoned checkout (context)',
          kvRow('Checkout session', ab.checkout_session_id, true) +
            kvRow('Stage', ab.stage) +
            kvRow('Abandoned at', fmtTs(ab.abandoned_at)) +
            kvRow('Last event', fmtTs(ab.last_event_at)) +
            kvRow('UTM source', ab.utm_source) +
            kvRow('UTM medium', ab.utm_medium) +
            kvRow('UTM campaign', ab.utm_campaign)
        );
    }

    if (notes && typeof notes === 'object') {
      html += sectionHtml('Razorpay order notes', flatObjectKv(notes));
    } else if (notes) {
      html += sectionHtml('Razorpay order notes', kvRow('Raw', String(notes), true));
    }

    return html;
  }

  function renderPrePurchaseTimeline(data) {
    if (!data || data.ok === false) {
      return sectionHtml('Before payment', '<p class="detail-muted">Could not load timeline.</p>');
    }
    if (data.acquisition === 'direct_purchase' || !data.leadId) {
      return sectionHtml(
        'Before payment',
        '<p class="detail-muted">Paid without a tracked lead session (guest checkout or lead id missing in payment).</p>'
      );
    }
    var evs = data.events || [];
    if (!evs.length) {
      return sectionHtml(
        'Before payment',
        '<p class="detail-muted">No activity logged before payment for this lead.</p>'
      );
    }
    var cards = evs
      .map(function (ev) {
        var when = fmtTs(ev.created_at);
        var src =
          ev._source === 'visitor'
            ? '<span class="badge badge--dim">Browsing</span> '
            : ev._source === 'lead'
              ? '<span class="badge badge--ok">On file</span> '
              : '';
        var titleText = simpleTimelineTitle(ev);
        var path = ev.path ? '<div class="timeline-event-path">' + esc(ev.path) + '</div>' : '';
        var ref = ev.referrer ? '<div class="timeline-event-utm">From link: ' + esc(ev.referrer) + '</div>' : '';
        var utm = [ev.utm_source, ev.utm_medium, ev.utm_campaign].filter(Boolean).join(' · ');
        var utmLine = utm ? '<div class="timeline-event-utm">Ad / campaign: ' + esc(utm) + '</div>' : '';
        var meta =
          ev.meta && typeof ev.meta === 'object'
            ? '<div class="timeline-event-meta">' + esc(JSON.stringify(ev.meta).slice(0, 400)) + '</div>'
            : '';
        return (
          '<article class="timeline-event-card"><div class="timeline-event-time">' +
          esc(when) +
          '</div><div class="timeline-event-title">' +
          src +
          esc(titleText) +
          '</div>' +
          path +
          ref +
          utmLine +
          meta +
          '</article>'
        );
      })
      .join('');
    return sectionHtml(
      'Before payment',
      '<p class="detail-muted" style="margin-bottom:10px;">What this person did on the site before paying (newest at bottom).</p><div class="timeline-scroll">' +
        cards +
        '</div>'
    );
  }

  function renderCustomerDetail(row) {
    var c = row || {};
    var html =
      sectionHtml(
        'Customer',
        kvRow('ID', c.id, true) +
          kvRow('Email', c.email) +
          kvRow('Name', c.name) +
          kvRow('Phone', c.phone) +
          kvRow('Paying', c.is_paying_customer ? 'Yes' : 'No') +
          kvRow('First paid', fmtTs(c.first_paid_at)) +
          kvRow('Total spent', fmtMoneyPaise(c.total_spent_paise, 'INR')) +
          kvRow('Notes', c.notes) +
          kvRow('Created', fmtTs(c.created_at)) +
          kvRow('Updated', fmtTs(c.updated_at))
      );
    if (c.meta && typeof c.meta === 'object') {
      html += sectionHtml('Meta', flatObjectKv(c.meta));
    }
    return html;
  }

  function phoneLooksValidIN(p) {
    if (!p || typeof p !== 'string') return false;
    var d = p.replace(/\D/g, '');
    return d.length === 10 && /^[6-9]/.test(d);
  }

  function leadJourneySteps(L) {
    var ev = Array.isArray(L.lead_events) ? L.lead_events : [];
    var orders = Array.isArray(L.orders) ? L.orders : [];
    var abandoned = Array.isArray(L.abandoned_checkouts) ? L.abandoned_checkouts : [];
    var bookings = Array.isArray(L.consultancy_bookings) ? L.consultancy_bookings : [];

    function hasCtaOrDeepEngagement() {
      return ev.some(function (e) {
        var n = String(e.event_name || '').toLowerCase();
        var t = String(e.event_type || '').toLowerCase();
        if (n.indexOf('cta') >= 0) return true;
        if (n === 'rashi_selected' || n === 'metal_finder_phone_submit' || n === 'whatsapp_click') return true;
        if (t === 'checkout' && n && n !== 'page_view') return true;
        if (t === 'metal_finder' && n && n !== 'page_view' && n !== 'lead_upsert' && n !== 'lead_created')
          return true;
        return false;
      });
    }

    var visited = true;
    var engaged =
      hasCtaOrDeepEngagement() ||
      abandoned.length > 0 ||
      orders.length > 0 ||
      bookings.length > 0;
    var phoneSaved =
      phoneLooksValidIN(L.phone) ||
      ev.some(function (e) {
        return String(e.event_name || '') === 'metal_finder_phone_submit';
      });
    var checkoutStarted =
      abandoned.length > 0 ||
      ev.some(function (e) {
        return String(e.event_type || '') === 'checkout';
      });
    var paidOrBooked =
      orders.length > 0 ||
      !!L.converted_order_id ||
      bookings.some(function (b) {
        return (b.payment_status || '').toLowerCase() === 'paid' || (b.status || '') === 'confirmed';
      });

    return [
      { label: 'Visited the site', ok: visited },
      { label: 'Clicked something important (checkout, tool, or CTA)', ok: engaged },
      { label: 'Phone saved (10-digit India number)', ok: phoneSaved },
      { label: 'Started checkout', ok: checkoutStarted },
      { label: 'Paid or booked', ok: paidOrBooked },
    ];
  }

  function renderLeadJourneyHtml(L) {
    var steps = leadJourneySteps(L);
    var items = steps
      .map(function (s) {
        var cls = s.ok ? 'lead-journey-step lead-journey-step--ok' : 'lead-journey-step lead-journey-step--no';
        var mark = s.ok ? '✓' : '○';
        return (
          '<div class="' +
          cls +
          '"><span class="lead-journey-mark" aria-hidden="true">' +
          esc(mark) +
          '</span><span class="lead-journey-label">' +
          esc(s.label) +
          '</span></div>'
        );
      })
      .join('');
    return (
      '<div class="lead-journey"><div class="lead-journey-title">Journey checklist</div><div class="lead-journey-rail">' +
      items +
      '</div></div>'
    );
  }

  function renderLeadDetail(row) {
    var L = row || {};
    var shortId = (L.id || '').slice(0, 8).toUpperCase();

    var journey = renderLeadJourneyHtml(L);

    var visitorFrom = '';
    var V = L.visitors;
    if (V && typeof V === 'object' && !Array.isArray(V)) {
      var cs = V.conversion_source && typeof V.conversion_source === 'object' ? V.conversion_source : {};
      visitorFrom =
        sectionHtml(
          'How they became a lead',
          '<p class="detail-muted">Visitor id <code>' +
            esc(V.id) +
            '</code></p>' +
            kvRow('When they shared contact', fmtTs(V.conversion_at)) +
            kvRow('Page / form', cs.page || cs.source_page) +
            kvRow('Form name / goal', [cs.form, cs.goal].filter(Boolean).join(' · ') || '—')
        );
    }

    var attr =
      '<div class="lead-attribution">' +
      '<div class="lead-attribution-title">Where they came from</div>' +
      '<div class="lead-attribution-grid">' +
      kvRow('Referrer (first link)', L.referrer) +
      kvRow('Referrer (browser)', L.document_referrer) +
      kvRow('UTM source', L.utm_source) +
      kvRow('UTM medium', L.utm_medium) +
      kvRow('UTM campaign', L.utm_campaign) +
      kvRow('UTM content', L.utm_content) +
      kvRow('UTM term', L.utm_term) +
      kvRow('Landing page', L.landing_path) +
      kvRow('Source page', L.source_page) +
      '</div></div>';

    var summary =
      kvRow('Lead code', shortId || L.id, true) +
      kvRow('Intent score', L.intent_score != null ? String(L.intent_score) : '—') +
      kvRow('Intent level', L.intent_tier || '—') +
      kvRow('Session ID', L.session_id, true) +
      kvRow('Status', L.lead_status) +
      kvRow('Email', L.email) +
      kvRow('Name', L.name) +
      kvRow('Phone', L.phone) +
      kvRow('First seen', fmtTs(L.first_seen_at)) +
      kvRow('Last seen', fmtTs(L.last_seen_at)) +
      kvRow('Client language', L.client_language) +
      kvRow('Screen', (L.screen_width || '—') + ' × ' + (L.screen_height || '—')) +
      kvRow('User agent', L.user_agent, true) +
      kvRow('Converted order ID', L.converted_order_id, true);

    var ordersHtml = '';
    if (Array.isArray(L.orders) && L.orders.length) {
      var ob = L.orders
        .slice()
        .sort(function (a, b) {
          return new Date(b.created_at || 0) - new Date(a.created_at || 0);
        })
        .map(function (o) {
          return (
            '<div class="lead-order-card">' +
            '<div class="lead-order-line"><strong>' +
            esc(o.product_slug || 'order') +
            '</strong> · ' +
            esc(fmtMoneyPaise(o.amount_paise, o.currency)) +
            '</div>' +
            '<div class="lead-order-line detail-v--mono">' +
            esc(String(o.razorpay_order_id || '')) +
            '</div>' +
            '<div class="lead-order-meta">' +
            esc((o.payment_status || '') + ' · ' + (o.order_status || '') + ' · paid ' + fmtTs(o.paid_at)) +
            '</div></div>'
          );
        })
        .join('');
      ordersHtml = sectionHtml('Orders (paid)', '<div class="lead-order-list">' + ob + '</div>');
    } else {
      ordersHtml = sectionHtml('Orders (paid)', '<p class="detail-muted">No orders linked to this lead yet.</p>');
    }

    var bookHtml = '';
    if (Array.isArray(L.consultancy_bookings) && L.consultancy_bookings.length) {
      var bh = L.consultancy_bookings
        .slice()
        .sort(function (a, b) {
          return new Date(b.created_at || 0) - new Date(a.created_at || 0);
        })
        .map(function (b) {
          return (
            '<div class="lead-order-card">' +
            '<div class="lead-order-line"><strong>' +
            esc(b.plan_name || b.plan_code || 'Consultancy') +
            '</strong> · ' +
            esc(fmtMoneyPaise(b.amount_paise, b.currency)) +
            '</div>' +
            '<div class="lead-order-meta">' +
            esc(
              (b.payment_status || '') +
                ' · slot ' +
                fmtTs(b.slot_start) +
                ' → ' +
                fmtTs(b.slot_end)
            ) +
            '</div></div>'
          );
        })
        .join('');
      bookHtml = sectionHtml('Consultancy bookings', '<div class="lead-order-list">' + bh + '</div>');
    }

    var abandonHtml = '';
    if (Array.isArray(L.abandoned_checkouts) && L.abandoned_checkouts.length) {
      var ah = L.abandoned_checkouts
        .map(function (a) {
          return (
            '<div class="lead-order-card">' +
            '<div class="lead-order-line"><strong>' +
            esc(a.product_slug || 'checkout') +
            '</strong> · stage <span class="badge">' +
            esc(a.stage || '') +
            '</span></div>' +
            '<div class="lead-order-meta">' +
            esc('Last event ' + fmtTs(a.last_event_at) + (a.referrer ? ' · ref ' + a.referrer : '')) +
            '</div></div>'
          );
        })
        .join('');
      abandonHtml = sectionHtml('Checkout sessions (abandoned / in progress)', '<div class="lead-order-list">' + ah + '</div>');
    }

    var meta = L.meta && typeof L.meta === 'object' ? sectionHtml('Lead meta (JSON)', flatObjectKv(L.meta)) : '';

    // Timeline from lead_events, grouped by session in alternating rows.
    var timeline = '';
    if (Array.isArray(L.lead_events) && L.lead_events.length) {
      var sorted = L.lead_events.slice().sort(function (a, b) {
        return new Date(a.created_at || 0) - new Date(b.created_at || 0);
      });

      var grouped = {};
      var sessionOrder = [];
      sorted.forEach(function (ev) {
        var sid = String(ev.session_id || L.session_id || 'unknown');
        if (!grouped[sid]) {
          grouped[sid] = [];
          sessionOrder.push(sid);
        }
        grouped[sid].push(ev);
      });

      var rows = sessionOrder
        .map(function (sid, idx) {
          var events = grouped[sid] || [];
          var first = events[0] || {};
          var last = events[events.length - 1] || {};
          var rowCls = idx % 2 === 0 ? 'timeline-row' : 'timeline-row timeline-row--right';
          var cards = events
            .map(function (ev) {
              var when = fmtTs(ev.created_at);
              var primary = simpleTimelineTitle(ev);
              var path = ev.path || '';
              var evRef = ev.referrer || '';
              var utm =
                [ev.utm_source, ev.utm_medium, ev.utm_campaign, ev.utm_content, ev.utm_term]
                  .filter(Boolean)
                  .join(' · ') || '';
              var metaLine = '';
              if (ev.meta && typeof ev.meta === 'object') {
                var mk = Object.keys(ev.meta);
                if (mk.length) {
                  try {
                    metaLine =
                      '<div class="timeline-event-meta">' + esc(JSON.stringify(ev.meta).slice(0, 420)) + '</div>';
                  } catch (e) {
                    metaLine = '';
                  }
                }
              }
              var card = '<article class="timeline-event-card">';
              card += '<div class="timeline-event-time">' + esc(when) + '</div>';
              card += '<div class="timeline-event-title">' + esc(primary) + '</div>';
              if (path) card += '<div class="timeline-event-path">' + esc(path) + '</div>';
              if (evRef) card += '<div class="timeline-event-utm">Ref: ' + esc(evRef) + '</div>';
              if (utm) card += '<div class="timeline-event-utm">UTM: ' + esc(utm) + '</div>';
              card += metaLine;
              card += '</article>';
              return card;
            })
            .join('');

          var sidShort = sid.length > 18 ? sid.slice(0, 18) + '…' : sid;
          var header = '<div class="timeline-session-head">';
          header += '<span class="timeline-session-chip">Session ' + esc(String(idx + 1)) + '</span>';
          header += '<span class="timeline-session-id" title="' + esc(sid) + '">' + esc(sidShort) + '</span>';
          header += '<span class="timeline-session-range">' + esc(fmtTs(first.created_at)) + ' → ' + esc(fmtTs(last.created_at)) + '</span>';
          header += '</div>';

          return '<section class="' + rowCls + '">' + header + '<div class="timeline-cards-row">' + cards + '</div></section>';
        })
        .join('');

      timeline = sectionHtml('Activity (what they did)', '<div class="timeline-wrap">' + rows + '</div>');
    }

    return (
      journey +
      visitorFrom +
      attr +
      sectionHtml('Identity &amp; device', summary) +
      ordersHtml +
      bookHtml +
      abandonHtml +
      meta +
      timeline
    );
  }

  function renderAbandonedDetail(row) {
    var a = row || {};
    var lead = a.leads && typeof a.leads === 'object' ? a.leads : null;
    var block =
      kvRow('ID', a.id, true) +
      kvRow('Checkout session', a.checkout_session_id, true) +
      kvRow('Lead ID (FK)', a.lead_id, true) +
      kvRow('Stage', a.stage) +
      kvRow('Email', a.email) +
      kvRow('Name', a.name) +
      kvRow('Phone', a.phone) +
      kvRow('Product', a.product_slug) +
      kvRow('Razorpay order ID', a.razorpay_order_id, true) +
      kvRow('Amount', a.amount_paise != null ? fmtMoneyPaise(a.amount_paise, a.currency) : '—') +
      kvRow('UTM source', a.utm_source) +
      kvRow('UTM medium', a.utm_medium) +
      kvRow('UTM campaign', a.utm_campaign) +
      kvRow('Referrer', a.referrer) +
      kvRow('Landing path', a.landing_path) +
      kvRow('Last event', fmtTs(a.last_event_at)) +
      kvRow('Abandoned at', fmtTs(a.abandoned_at)) +
      kvRow('Converted order ID', a.converted_order_id, true) +
      kvRow('Converted at', fmtTs(a.converted_at)) +
      kvRow('Created', fmtTs(a.created_at)) +
      kvRow('Updated', fmtTs(a.updated_at));
    var html = sectionHtml('Abandoned checkout', block);
    if (lead) {
      html +=
        sectionHtml(
          'Linked lead',
          kvRow('Session', lead.session_id, true) +
            kvRow('UTM campaign', lead.utm_campaign) +
            kvRow('UTM source', lead.utm_source) +
            kvRow('UTM medium', lead.utm_medium) +
            kvRow('Landing', lead.landing_path)
        );
    }
    if (a.meta && typeof a.meta === 'object') {
      html += sectionHtml('Meta', flatObjectKv(a.meta));
    }
    return html;
  }

  function openDetailHtml(title, html) {
    if (detailCloseTimer) {
      clearTimeout(detailCloseTimer);
      detailCloseTimer = null;
    }
    if (detailTitle) detailTitle.textContent = title || 'Details';
    if (detailModalBody) detailModalBody.innerHTML = html || '<p style="color:var(--muted);">No data.</p>';
    if (detailModal) {
      detailModal.classList.remove('hidden');
      detailModal.setAttribute('aria-hidden', 'false');
    }
    if (detailModalPanel) {
      detailModalPanel.style.transition = 'none';
      detailModalPanel.style.transform = 'translateY(100%)';
      requestAnimationFrame(function () {
        requestAnimationFrame(function () {
          detailModalPanel.style.transition = 'transform 0.36s cubic-bezier(0.22, 1, 0.36, 1)';
          detailModalPanel.style.transform = 'translateY(0)';
        });
      });
    }
  }

  function closeDetail() {
    if (detailModalPanel) {
      detailModalPanel.style.transition = 'transform 0.28s cubic-bezier(0.4, 0, 0.2, 1)';
      detailModalPanel.style.transform = 'translateY(100%)';
    }
    detailCloseTimer = setTimeout(function () {
      if (detailModal) {
        detailModal.classList.add('hidden');
        detailModal.setAttribute('aria-hidden', 'true');
      }
      if (detailModalBody) detailModalBody.innerHTML = '';
      detailCloseTimer = null;
    }, 280);
  }

  var detailClose = document.getElementById('detailModalClose');
  var detailBackdrop = document.getElementById('detailModalBackdrop');
  if (detailClose) detailClose.addEventListener('click', closeDetail);
  if (detailBackdrop) detailBackdrop.addEventListener('click', closeDetail);

  if (detailModal) {
    detailModal.addEventListener('click', function (e) {
      var btn = e.target && e.target.closest ? e.target.closest('.js-load-order-journey') : null;
      if (!btn || !detailModal.contains(btn)) return;
      e.preventDefault();
      var oid = btn.getAttribute('data-order-id');
      if (!oid) return;
      var mount = document.getElementById('orderJourneyMount');
      if (!mount) return;
      mount.innerHTML = '<p class="detail-muted">Loading activity before payment…</p>';
      fetch('/api/admin/orders/' + encodeURIComponent(oid) + '/pre-purchase-timeline', { headers: authHeaders() })
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          mount.innerHTML = renderPrePurchaseTimeline(data);
        })
        .catch(function () {
          mount.innerHTML = '<p class="detail-muted">Could not load timeline.</p>';
        });
    });
  }

  function renderVisitorTimeline(data) {
    if (!data || !data.ok) {
      return '<p class="detail-muted">Could not load timeline.</p>';
    }
    var v = data.visitor || {};
    var head =
      '<div class="lead-attribution"><div class="lead-attribution-title">Visitor</div>' +
      '<div class="lead-attribution-grid">' +
      kvRow('Session', v.session_id, true) +
      kvRow('First visit', fmtTs(v.first_seen_at)) +
      kvRow('Last visit', fmtTs(v.last_seen_at)) +
      kvRow('Lead id (if they shared contact)', v.converted_lead_id, true) +
      kvRow('Became lead at', fmtTs(v.conversion_at)) +
      '</div></div>';
    var raw = []
      .concat((data.visitorEvents || []).map(function (e) { return { t: 'visitor', e: e, _source: 'visitor' }; }))
      .concat((data.leadEvents || []).map(function (e) { return { t: 'lead', e: e, _source: 'lead' }; }))
      .sort(function (a, b) {
        return new Date(a.e.created_at || 0) - new Date(b.e.created_at || 0);
      });
    var deduped = dedupeMergedTimelineEvents(
      raw.map(function (x) {
        var ev = x.e;
        return Object.assign({}, ev, { _source: x._source });
      })
    );
    var cards = deduped
      .map(function (ev) {
        var badge =
          ev._source === 'visitor'
            ? '<span class="badge badge--dim">Browsing</span>'
            : '<span class="badge badge--ok">On file</span>';
        var when = fmtTs(ev.created_at);
        var line = simpleTimelineTitle(ev);
        var path = ev.path ? '<div class="timeline-event-path">' + esc(ev.path) + '</div>' : '';
        return (
          '<article class="timeline-event-card"><div class="timeline-event-time">' +
          esc(when) +
          '</div><div class="timeline-event-title">' +
          badge +
          ' ' +
          esc(line) +
          '</div>' +
          path +
          '</article>'
        );
      })
      .join('');
    return head + sectionHtml('Activity (what they did)', '<div class="timeline-wrap">' + cards + '</div>');
  }

  function bindExpand(btn, kind, row) {
    btn.addEventListener('click', function () {
      var html = '';
      var title = 'Details';
      if (kind === 'visitor') {
        title = 'Visitor activity';
        openDetailHtml(title, '<p class="detail-muted">Loading…</p>');
        fetch('/api/admin/visitors/' + encodeURIComponent(row.id) + '/timeline', { headers: authHeaders() })
          .then(function (r) {
            return r.json();
          })
          .then(function (data) {
            openDetailHtml(title, renderVisitorTimeline(data));
          })
          .catch(function () {
            openDetailHtml(title, '<p class="detail-muted">Failed to load.</p>');
          });
        return;
      }
      if (kind === 'order') {
        title = 'Order details';
        var orderBase =
          renderOrderDetail(row) +
          '<div class="detail-section" style="margin-top:12px;"><h3 class="detail-subhead">Before payment</h3>' +
          '<p class="detail-muted">Load only when you need the full click path (can be long).</p>' +
          '<button type="button" class="btn btn--small js-load-order-journey" data-order-id="' +
          esc(row.id) +
          '">Show site activity before payment</button>' +
          '<div id="orderJourneyMount" style="margin-top:12px;"></div></div>';
        openDetailHtml(title, orderBase);
        return;
      } else if (kind === 'customer') {
        title = 'Customer details';
        openDetailHtml(title, renderCustomerDetail(row) + '<p class="detail-muted" id="custActLoading">Loading site activity…</p><div id="custActMount"></div>');
        if (!row.id) return;
        fetch('/api/admin/customers/' + encodeURIComponent(row.id) + '/activity-timeline', { headers: authHeaders() })
          .then(function (r) {
            return r.json().then(function (data) {
              return { ok: r.ok, data: data };
            });
          })
          .then(function (res) {
            var loadEl = document.getElementById('custActLoading');
            var mount = document.getElementById('custActMount');
            if (loadEl) loadEl.remove();
            if (!mount) return;
            var data = res.data || {};
            if (!res.ok || !data.ok) {
              mount.innerHTML =
                '<p class="detail-muted">' + esc((data && data.error) || 'Could not load activity.') + '</p>';
              return;
            }
            var parts = [];
            if (data.note) parts.push('<p class="detail-muted">' + esc(data.note) + '</p>');
            if (data.events && data.events.length) {
              parts.push(
                renderHumanActivityTimeline(
                  data.events,
                  '<p class="detail-muted">Matched to their visit history (same email/phone or order link).</p>'
                )
              );
            } else if (!data.note) {
              parts.push('<p class="detail-muted">No activity to show yet.</p>');
            }
            mount.innerHTML = parts.join('');
          })
          .catch(function () {
            var loadEl = document.getElementById('custActLoading');
            var mount = document.getElementById('custActMount');
            if (loadEl) loadEl.textContent = 'Could not load activity.';
            if (mount) mount.innerHTML = '';
          });
        return;
      } else if (kind === 'lead') {
        title = 'Lead details';
        html = renderLeadDetail(row);
      } else if (kind === 'abandoned') {
        title = 'Abandoned checkout details';
        openDetailHtml(
          title,
          renderAbandonedDetail(row) + '<p class="detail-muted" id="abCtxLoading">Loading conversion insight…</p><div id="abCtxMount"></div>'
        );
        if (!row.id) return;
        fetch('/api/admin/abandoned-checkouts/' + encodeURIComponent(row.id) + '/context', { headers: authHeaders() })
          .then(function (r) {
            return r.json();
          })
          .then(function (ctx) {
            var le = document.getElementById('abCtxLoading');
            var mount = document.getElementById('abCtxMount');
            if (le) le.remove();
            if (mount) mount.innerHTML = renderAbandonedContextSection(ctx);
          })
          .catch(function () {
            var le = document.getElementById('abCtxLoading');
            var mount = document.getElementById('abCtxMount');
            if (le) le.textContent = 'Could not load insight.';
            if (mount) mount.innerHTML = '';
          });
        return;
      }
      openDetailHtml(title, html);
    });
  }

  function ordersFilterParams() {
    var p = new URLSearchParams();
    p.set('page', String(ordersPage));
    p.set('per_page', '10');
    var s = document.getElementById('ordersFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var os = document.getElementById('ordersFilterOrderStatus');
    if (os && os.value) p.set('order_status', os.value);
    var ac = document.getElementById('ordersFilterAcquisition');
    if (ac && ac.value) p.set('acquisition', ac.value);
    appendDateRangeFromInputsOrPreset(p, 'orders', 'ordersFilterDateFrom', 'ordersFilterDateTo');
    return p.toString();
  }

  /** Same filters as the table, without pagination — for revenue totals. */
  function ordersSummaryQueryString() {
    var p = new URLSearchParams();
    var s = document.getElementById('ordersFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var os = document.getElementById('ordersFilterOrderStatus');
    if (os && os.value) p.set('order_status', os.value);
    var ac = document.getElementById('ordersFilterAcquisition');
    if (ac && ac.value) p.set('acquisition', ac.value);
    appendDateRangeFromInputsOrPreset(p, 'orders', 'ordersFilterDateFrom', 'ordersFilterDateTo');
    return p.toString();
  }

  function loadOrdersStripFromSummary() {
    var strip = document.getElementById('ordersPrimaryStrip');
    if (!strip || !canAuth()) {
      if (strip) strip.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;margin:0;">Connect to see revenue.</p>';
      return;
    }
    strip.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;margin:0;">Loading totals…</p>';
    fetch('/api/admin/orders/summary?' + ordersSummaryQueryString(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j || !res.j.ok) {
          strip.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;margin:0;">Could not load revenue totals.</p>';
          return;
        }
        var j = res.j;
        var subNote = j.truncated ? 'First 8k rows — total may be higher.' : 'Matches filters above.';
        strip.innerHTML = '';
        var tiles = [
          {
            label: 'Total revenue',
            value: j.revenueInr != null ? '₹' + Number(j.revenueInr).toFixed(2) : '—',
            sub: subNote,
          },
          { label: 'Orders', value: j.orderCount != null ? j.orderCount : '—', sub: 'In this filter' },
          {
            label: 'From saved contact',
            value: j.ordersAttributedToLead != null ? j.ordersAttributedToLead : '—',
            sub: 'Linked to a lead',
          },
          {
            label: 'Direct checkout',
            value: j.ordersDirectPurchase != null ? j.ordersDirectPurchase : '—',
            sub: 'No lead on order',
          },
        ];
        tiles.forEach(function (x) {
          var d = document.createElement('div');
          d.className = 'primary-metric-card';
          d.innerHTML =
            '<div class="primary-metric-label">' +
            esc(x.label) +
            '</div><div class="primary-metric-value">' +
            esc(String(x.value != null ? x.value : '—')) +
            '</div><div class="primary-metric-sub">' +
            esc(x.sub || '') +
            '</div>';
          strip.appendChild(d);
        });
        var leg = document.createElement('p');
        leg.className = 'detail-muted';
        leg.style.cssText = 'grid-column:1/-1;margin:8px 0 0;font-size:12px;line-height:1.45;';
        leg.innerHTML =
          '<strong>From saved contact</strong> = payment tied to someone who left email/phone before paying. ' +
          '<strong>Direct checkout</strong> = paid without that link (still a real customer).';
        strip.appendChild(leg);
      })
      .catch(function () {
        strip.innerHTML = '<p class="detail-muted" style="grid-column:1/-1;margin:0;">Network error loading totals.</p>';
      });
  }

  function loadOrders() {
    var msg = document.getElementById('ordersMsg');
    var wrap = document.getElementById('ordersTableWrap');
    var tbody = document.getElementById('ordersTbody');
    if (!canAuth()) {
      setMsg(msg, 'Enter admin secret and click Connect.', true);
      if (wrap) wrap.hidden = true;
      return;
    }
    loadOrdersStripFromSummary();
    setMsg(msg, 'Loading…', false);
    fetch('/api/admin/orders?' + ordersFilterParams(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j.ok) {
          setMsg(msg, (res.j && res.j.error) || 'Failed to load orders', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        setMsg(msg, '', false);
        var rows = res.j.orders || [];
        var total = res.j.total != null ? res.j.total : rows.length;
        var perPage = res.j.perPage || 10;
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        rows.forEach(function (row, idx) {
          var cust = row.customers || {};
          var acq = row.lead_id
            ? '<span class="badge badge--ok" title="Payment linked to a saved contact (lead).">From contact</span>'
            : '<span class="badge" title="Paid without a lead id on this order — direct checkout.">Direct</span>';
          var tr = document.createElement('tr');
          var st = String(row.order_status || 'new');
          var phoneDisp = cust.phone != null && cust.phone !== '' ? esc(cust.phone) : '—';
          var custCell =
            '<strong>' +
            esc(cust.name || '—') +
            '</strong><div style="font-size:11px;color:var(--muted);margin-top:2px;">' +
            esc(cust.email || '—') +
            '</div>';
          var receiptCell =
            '<span style="font-size:11px;">' +
            esc(row.receipt || '—') +
            '</span><div class="cell-mono" style="font-size:10px;margin-top:2px;opacity:0.85;">' +
            esc(row.razorpay_order_id || '') +
            '</div>';
          tr.innerHTML =
            '<td>' +
            fmtTs(row.paid_at) +
            '</td><td>' +
            fmtMoneyPaise(row.amount_paise, row.currency) +
            '</td><td class="cell-clip">' +
            esc(row.product_slug || '—') +
            '</td><td class="cell-clip">' +
            custCell +
            '</td><td class="cell-mono">' +
            phoneDisp +
            '</td><td>' +
            acq +
            '</td><td class="cell-clip">' +
            receiptCell +
            '</td><td><div class="order-status-row" data-order-status-cell="' +
            esc(row.id) +
            '"><span class="badge badge--dim" style="margin-right:6px;">' +
            esc(row.payment_status || '') +
            '</span><select class="order-status-sel" data-oid="' +
            esc(row.id) +
            '">' +
            ['new', 'processing', 'delivered', 'cancelled']
              .map(function (x) {
                return (
                  '<option value="' +
                  esc(x) +
                  '"' +
                  (st === x ? ' selected' : '') +
                  '>' +
                  esc(x) +
                  '</option>'
                );
              })
              .join('') +
            '</select><button type="button" class="btn btn--small order-status-save" data-oid="' +
            esc(row.id) +
            '">Save</button></div></td><td><button type="button" class="btn btn--small btn--ghost" data-detail-order="' +
            idx +
            '">Open</button></td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-detail-order="' + idx + '"]');
          if (btn) bindExpand(btn, 'order', row);
          var saveBtn = tr.querySelector('.order-status-save');
          if (saveBtn) {
            saveBtn.addEventListener('click', function () {
              var sel = tr.querySelector('.order-status-sel');
              var v = sel ? sel.value : 'new';
              fetch('/api/admin/orders/' + encodeURIComponent(row.id), {
                method: 'PATCH',
                headers: Object.assign({ 'Content-Type': 'application/json' }, authHeaders()),
                body: JSON.stringify({ order_status: v }),
              })
                .then(function (r) {
                  return r.json();
                })
                .then(function (j) {
                  if (!j.ok) {
                    setMsg(msg, j.error || 'Update failed', true);
                    return;
                  }
                  setMsg(msg, 'Status updated.', false);
                  loadOrders();
                })
                .catch(function () {
                  setMsg(msg, 'Update failed', true);
                });
            });
          }
        });
        wrap.hidden = rows.length === 0;
        if (rows.length === 0) setMsg(msg, 'No orders match filters.', false);
        setPagination('orders', ordersPage, perPage, total);
      })
      .catch(function () {
        setMsg(msg, 'Network error', true);
      });
  }

  function customersFilterParams() {
    var p = new URLSearchParams();
    p.set('page', String(customersPage));
    p.set('per_page', '10');
    var s = document.getElementById('customersFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var pay = document.getElementById('customersFilterPaying');
    if (pay && pay.value !== '') p.set('paying', pay.value);
    appendDateRangeFromInputsOrPreset(p, 'customers', 'customersFilterDateFrom', 'customersFilterDateTo');
    return p.toString();
  }

  function loadCustomers() {
    var msg = document.getElementById('customersMsg');
    var wrap = document.getElementById('customersTableWrap');
    var tbody = document.getElementById('customersTbody');
    if (!canAuth()) {
      setMsg(msg, 'Enter admin secret and click Connect.', true);
      if (wrap) wrap.hidden = true;
      return;
    }
    setMsg(msg, 'Loading…', false);
    fetch('/api/admin/customers?' + customersFilterParams(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j.ok) {
          setMsg(msg, (res.j && res.j.error) || 'Failed', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        setMsg(msg, '', false);
        var rows = res.j.customers || [];
        var total = res.j.total != null ? res.j.total : rows.length;
        var perPage = res.j.perPage || 10;
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        rows.forEach(function (row, idx) {
          var tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' +
            fmtTs(row.created_at) +
            '</td><td>' +
            esc(row.email) +
            '</td><td>' +
            esc(row.name) +
            '</td><td>' +
            esc(row.phone) +
            '</td><td>' +
            (row.is_paying_customer ? '<span class="badge badge--ok">Yes</span>' : '<span class="badge">No</span>') +
            '</td><td>' +
            fmtTs(row.first_paid_at) +
            '</td><td>' +
            fmtMoneyPaise(row.total_spent_paise, 'INR') +
            '</td><td><button type="button" class="btn btn--small btn--ghost" data-detail-cu="' +
            idx +
            '">Expand</button></td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-detail-cu="' + idx + '"]');
          if (btn) bindExpand(btn, 'customer', row);
        });
        wrap.hidden = rows.length === 0;
        if (rows.length === 0) setMsg(msg, 'No customers match.', false);
        setPagination('customers', customersPage, perPage, total);
      })
      .catch(function () {
        setMsg(msg, 'Network error', true);
      });
  }

  function leadsFilterParams() {
    var p = new URLSearchParams();
    p.set('page', String(leadsPage));
    p.set('per_page', '10');
    var conly = document.getElementById('leadsFilterContactsOnly');
    if (conly && conly.value) p.set('contacts_only', conly.value);
    var s = document.getElementById('leadsFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var u1 = document.getElementById('leadsFilterUtmSource');
    if (u1 && u1.value.trim()) p.set('utm_source', u1.value.trim());
    var u2 = document.getElementById('leadsFilterUtmMedium');
    if (u2 && u2.value.trim()) p.set('utm_medium', u2.value.trim());
    var u3 = document.getElementById('leadsFilterUtmCampaign');
    if (u3 && u3.value.trim()) p.set('utm_campaign', u3.value.trim());
    var ls = document.getElementById('leadsFilterLeadStatus');
    if (ls && ls.value.trim()) p.set('lead_status', ls.value.trim());
    var cv = document.getElementById('leadsFilterConverted');
    if (cv && cv.value) p.set('converted', cv.value);
    var it = document.getElementById('leadsFilterIntentTier');
    if (it && it.value) p.set('intent_tier', it.value);
    var imin = document.getElementById('leadsFilterIntentMin');
    if (imin && imin.value !== '' && !Number.isNaN(Number(imin.value))) p.set('intent_min', String(imin.value));
    var imax = document.getElementById('leadsFilterIntentMax');
    if (imax && imax.value !== '' && !Number.isNaN(Number(imax.value))) p.set('intent_max', String(imax.value));
    var df = document.getElementById('leadsFilterDateField');
    if (df && df.value) p.set('date_field', df.value);
    appendDateRangeFromInputsOrPreset(p, 'leads', 'leadsFilterDateFrom', 'leadsFilterDateTo');
    return p.toString();
  }

  function visitorsFilterParams() {
    var p = new URLSearchParams();
    p.set('page', String(visitorsPage));
    p.set('per_page', '10');
    var s = document.getElementById('visitorsFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var cv = document.getElementById('visitorsFilterConverted');
    if (cv && cv.value) p.set('converted', cv.value);
    appendDateRangeFromInputsOrPreset(p, 'visitors', 'visitorsFilterDateFrom', 'visitorsFilterDateTo');
    return p.toString();
  }

  function loadVisitors() {
    var msg = document.getElementById('visitorsMsg');
    var wrap = document.getElementById('visitorsTableWrap');
    var tbody = document.getElementById('visitorsTbody');
    if (!canAuth()) {
      setMsg(msg, 'Enter admin secret and click Connect.', true);
      if (wrap) wrap.hidden = true;
      return;
    }
    setMsg(msg, 'Loading…', false);
    fetch('/api/admin/visitors?' + visitorsFilterParams(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j.ok) {
          setMsg(msg, (res.j && res.j.error) || 'Failed', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        setMsg(msg, '', false);
        var rows = res.j.visitors || [];
        var total = res.j.total != null ? res.j.total : rows.length;
        var perPage = res.j.perPage || 10;
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        rows.forEach(function (row, idx) {
          var tr = document.createElement('tr');
          var conv = row.converted_lead_id
            ? '<span class="badge badge--ok">Lead</span>'
            : '<span class="badge">Visitor</span>';
          var utmSm = [row.utm_source, row.utm_medium].filter(Boolean).join(' / ') || '—';
          tr.innerHTML =
            '<td>' +
            fmtTs(row.last_seen_at) +
            '</td><td class="cell-mono cell-clip">' +
            esc((row.session_id || '').slice(0, 14)) +
            '</td><td class="cell-clip">' +
            esc(row.landing_path) +
            '</td><td class="cell-clip">' +
            esc(utmSm) +
            '</td><td>' +
            conv +
            '</td><td><button type="button" class="btn btn--small btn--ghost" data-detail-visitor="' +
            idx +
            '">Timeline</button></td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-detail-visitor="' + idx + '"]');
          if (btn) bindExpand(btn, 'visitor', row);
        });
        wrap.hidden = rows.length === 0;
        if (rows.length === 0) setMsg(msg, 'No visitors match.', false);
        setPagination('visitors', visitorsPage, perPage, total);
      })
      .catch(function () {
        setMsg(msg, 'Network error', true);
      });
  }

  function buildPageAnalyticsQuery() {
    var p = new URLSearchParams();
    var preset = presetDateIsoByPanel.analytics;
    if (preset && preset.from && preset.to) {
      p.set('date_from', preset.from);
      p.set('date_to', preset.to);
      return p.toString();
    }
    var df = document.getElementById('analyticsDateFrom');
    var dt = document.getElementById('analyticsDateTo');
    if (df && dt && df.value && dt.value) {
      p.set('date_from', dtLocalToIso(df.value));
      p.set('date_to', dtLocalToIso(dt.value));
    } else {
      p.set('preset', analyticsPreset || 'last7');
    }
    return p.toString();
  }

  function loadPageAnalytics() {
    var msg = document.getElementById('analyticsPagesMsg');
    var wrap = document.getElementById('analyticsPagesWrap');
    var tbody = document.getElementById('analyticsPagesTbody');
    var detailCard = document.getElementById('analyticsPageDetailCard');
    if (!canAuth()) {
      if (msg) setMsg(msg, 'Connect admin first.', true);
      if (wrap) wrap.hidden = true;
      if (detailCard) detailCard.hidden = true;
      return;
    }
    if (msg) setMsg(msg, 'Loading page analytics…', false);
    fetch('/api/admin/analytics/pages?' + buildPageAnalyticsQuery(), { headers: authHeaders() })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          if (msg) setMsg(msg, (j && j.error) || 'Failed', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        if (msg) setMsg(msg, j.truncated ? 'Partial window (row cap) — narrow the date range if needed.' : '', false);
        var pages = j.pages || [];
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        pages.forEach(function (pg) {
          var tr = document.createElement('tr');
          tr.innerHTML =
            '<td class="cell-clip">' +
            esc(pg.path) +
            '</td><td>' +
            esc(pg.label) +
            '</td><td>' +
            esc(String(pg.events)) +
            '</td><td>' +
            esc(String(pg.uniqueSessions)) +
            '</td><td><button type="button" class="btn btn--small btn--ghost" data-analytics-path="' +
            esc(pg.path) +
            '">Day breakdown</button></td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-analytics-path]');
          if (btn) {
            btn.addEventListener('click', function () {
              analyticsSelectedPath = pg.path;
              loadPageAnalyticsDetail(pg.path);
            });
          }
        });
        wrap.hidden = pages.length === 0;
        if (pages.length === 0 && msg) setMsg(msg, 'No path data in this range.', false);
      })
      .catch(function () {
        if (msg) setMsg(msg, 'Network error', true);
      });
  }

  function loadPageAnalyticsDetail(path) {
    var hint = document.getElementById('analyticsDetailHint');
    var title = document.getElementById('analyticsDetailTitle');
    var tbody = document.getElementById('analyticsDetailTbody');
    var card = document.getElementById('analyticsPageDetailCard');
    var wrap = document.getElementById('analyticsDetailWrap');
    if (!path || !canAuth()) return;
    var q = buildPageAnalyticsQuery() + '&path=' + encodeURIComponent(path);
    fetch('/api/admin/analytics/pages/detail?' + q, { headers: authHeaders() })
      .then(function (r) {
        return r.json();
      })
      .then(function (j) {
        if (!j || !j.ok) {
          if (hint) hint.textContent = (j && j.error) || 'Failed';
          return;
        }
        if (title) title.textContent = j.label || j.path;
        if (hint)
          hint.textContent =
            'Total events ' +
            (j.totalEvents != null ? j.totalEvents : '—') +
            ' · unique sessions ' +
            (j.uniqueSessions != null ? j.uniqueSessions : '—');
        if (tbody) {
          tbody.innerHTML = '';
          (j.byDay || []).forEach(function (d) {
            var tr = document.createElement('tr');
            tr.innerHTML = '<td>' + esc(d.date) + '</td><td>' + esc(String(d.count)) + '</td>';
            tbody.appendChild(tr);
          });
        }
        if (card) card.hidden = false;
        if (wrap) wrap.hidden = (j.byDay || []).length === 0;
      })
      .catch(function () {
        if (hint) hint.textContent = 'Network error';
      });
  }

  function loadLeads() {
    var msg = document.getElementById('leadsMsg');
    var wrap = document.getElementById('leadsTableWrap');
    var tbody = document.getElementById('leadsTbody');
    if (!canAuth()) {
      setMsg(msg, 'Enter admin secret and click Connect.', true);
      if (wrap) wrap.hidden = true;
      return;
    }
    setMsg(msg, 'Loading…', false);
    fetch('/api/admin/leads?' + leadsFilterParams(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j.ok) {
          setMsg(msg, (res.j && res.j.error) || 'Failed', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        setMsg(msg, '', false);
        var rows = res.j.leads || [];
        var total = res.j.total != null ? res.j.total : rows.length;
        var perPage = res.j.perPage || 10;
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        rows.forEach(function (row, idx) {
          var tr = document.createElement('tr');
          var code = (row.id || '').slice(0, 8).toUpperCase();
          var utmSm = [row.utm_source, row.utm_medium].filter(Boolean).join(' / ') || '—';
          tr.innerHTML =
            '<td>' +
            fmtTs(row.last_seen_at) +
            '</td><td class="cell-mono cell-clip">' +
            esc(code || row.session_id) +
            '</td><td>' +
            esc(row.email) +
            '</td><td>' +
            esc(row.name) +
            '</td><td class="cell-clip">' +
            esc(row.phone) +
            '</td><td class="cell-clip">' +
            esc(utmSm) +
            '</td><td class="cell-clip">' +
            esc(row.utm_campaign) +
            '</td><td class="cell-clip">' +
            esc(row.landing_path) +
            '</td><td class="cell-clip">' +
            esc(row.referrer) +
            '</td><td>' +
            esc(row.intent_score != null ? String(row.intent_score) : '—') +
            '</td><td>' +
            esc(row.intent_tier || '—') +
            '</td><td>' +
            esc(row.lead_status) +
            '</td><td>' +
            '<button type="button" class="btn btn--small btn--ghost" data-detail-lead="' +
            idx +
            '">Expand</button> ' +
            '<button type="button" class="btn btn--small btn--ghost" data-delete-lead="' +
            idx +
            '">Delete</button>' +
            '</td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-detail-lead="' + idx + '"]');
          if (btn) bindExpand(btn, 'lead', row);
          var del = tr.querySelector('[data-delete-lead="' + idx + '"]');
          if (del) {
            del.addEventListener('click', function () {
              if (!window.confirm('Delete this lead profile? This cannot be undone.')) return;
              fetch('/api/admin/leads/' + encodeURIComponent(row.id), {
                method: 'DELETE',
                headers: authHeaders(),
              })
                .then(function (r) {
                  return r.json().catch(function () {
                    return { ok: r.ok };
                  });
                })
                .then(function (j) {
                  if (!j.ok) {
                    setMsg(msg, j.error || 'Delete failed', true);
                    return;
                  }
                  loadLeads();
                })
                .catch(function () {
                  setMsg(msg, 'Delete failed', true);
                });
            });
          }
        });
        wrap.hidden = rows.length === 0;
        if (rows.length === 0) setMsg(msg, 'No leads match filters.', false);
        setPagination('leads', leadsPage, perPage, total);
      })
      .catch(function () {
        setMsg(msg, 'Network error', true);
      });
  }

  function abandonedFilterParams() {
    var p = new URLSearchParams();
    p.set('page', String(abandonedPage));
    p.set('per_page', '10');
    var s = document.getElementById('abandonedFilterSearch');
    if (s && s.value.trim()) p.set('search', s.value.trim());
    var st = document.getElementById('abandonedFilterStage');
    if (st && st.value.trim()) p.set('stage', st.value.trim());
    var u = document.getElementById('abandonedFilterUtmCampaign');
    if (u && u.value.trim()) p.set('utm_campaign', u.value.trim());
    appendDateRangeFromInputsOrPreset(p, 'abandoned', 'abandonedFilterDateFrom', 'abandonedFilterDateTo');
    return p.toString();
  }

  function loadAbandoned() {
    var msg = document.getElementById('abandonedMsg');
    var wrap = document.getElementById('abandonedTableWrap');
    var tbody = document.getElementById('abandonedTbody');
    if (!canAuth()) {
      setMsg(msg, 'Enter admin secret and click Connect.', true);
      if (wrap) wrap.hidden = true;
      return;
    }
    setMsg(msg, 'Loading…', false);
    fetch('/api/admin/abandoned-checkouts?' + abandonedFilterParams(), { headers: authHeaders() })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, j: j };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.j.ok) {
          setMsg(msg, (res.j && res.j.error) || 'Failed', true);
          if (wrap) wrap.hidden = true;
          return;
        }
        setMsg(msg, '', false);
        var rows = res.j.abandonedCheckouts || [];
        var total = res.j.total != null ? res.j.total : rows.length;
        var perPage = res.j.perPage || 10;
        if (!tbody || !wrap) return;
        tbody.innerHTML = '';
        rows.forEach(function (row, idx) {
          var tr = document.createElement('tr');
          var conv = row.converted_order_id
            ? '<span class="badge badge--ok">Paid</span>'
            : '<span class="badge">Dropped</span>';
          var phone = row.phone != null && row.phone !== '' ? esc(row.phone) : '—';
          var utm = [row.utm_source, row.utm_campaign].filter(Boolean).join(' · ') || '—';
          tr.innerHTML =
            '<td>' +
            fmtTs(row.last_event_at) +
            '</td><td><span class="badge">' +
            esc(row.stage) +
            '</span></td><td class="cell-mono">' +
            phone +
            '</td><td class="cell-clip">' +
            esc(row.name || '—') +
            '</td><td class="cell-clip">' +
            esc(row.email || '—') +
            '</td><td class="cell-clip">' +
            esc(row.product_slug || '—') +
            '</td><td class="cell-mono cell-clip" style="font-size:10px;">' +
            esc(row.checkout_session_id) +
            '</td><td class="cell-clip" style="font-size:11px;">' +
            esc(utm) +
            '</td><td>' +
            conv +
            '</td><td><button type="button" class="btn btn--small btn--ghost" data-detail-ab="' +
            idx +
            '">Open</button></td>';
          tbody.appendChild(tr);
          var btn = tr.querySelector('[data-detail-ab="' + idx + '"]');
          if (btn) bindExpand(btn, 'abandoned', row);
        });
        wrap.hidden = rows.length === 0;
        if (rows.length === 0) setMsg(msg, 'No rows match filters.', false);
        setPagination('abandoned', abandonedPage, perPage, total);
      })
      .catch(function () {
        setMsg(msg, 'Network error', true);
      });
  }

  function loadDashboardSummary() {
    var grid = document.getElementById('dashSummaryGrid');
    var apiCard = document.getElementById('dashApiSummary');
    var analyticsGrid = document.getElementById('analyticsGrid');
    var dashCard = document.getElementById('dashSummary');
    var hint = document.getElementById('analyticsHint');
    var rangeHint = document.getElementById('dashRangeHint');
    var primaryRow = document.getElementById('dashPrimaryRow');
    if (!canAuth()) {
      if (primaryRow) {
        primaryRow.innerHTML =
          '<p class="detail-muted" style="grid-column:1/-1;margin:0;">Connect admin to see revenue, leads, and conversions.</p>';
      }
      if (dashCard) dashCard.hidden = true;
      if (apiCard) apiCard.hidden = true;
      var snapCard0 = document.getElementById('dashSnapshotsCard');
      if (snapCard0) snapCard0.hidden = true;
      return;
    }
    if (dashCard) dashCard.hidden = false;
    if (apiCard) apiCard.hidden = true;
    updateDashChipActive();
    if (analyticsGrid) {
      analyticsGrid.classList.add('hidden');
      analyticsGrid.innerHTML = '<p class="detail-muted">Loading analytics…</p>';
    }
    if (hint) hint.textContent = 'Revenue and visits for the dates you pick.';
    if (rangeHint) rangeHint.textContent = '';

    fetch('/api/admin/analytics?' + buildAnalyticsQuery(), { headers: authHeaders() })
      .then(function (r) {
        return r.json();
      })
      .then(function (a) {
        if (!a || !a.ok) return;
        var t = a.period || a.today || {};
        var al = a.allTime || {};
        var pr = a.preset || '';
        if (primaryRow) fillPrimaryMetricsRow(primaryRow, t, null);
        if (!analyticsGrid) {
          return;
        }
        if (rangeHint && a.periodStart && a.periodEnd) {
          rangeHint.textContent =
            'Window: ' +
            fmtTs(a.periodStart) +
            ' → ' +
            fmtTs(a.periodEnd) +
            (pr ? ' · preset: ' + pr : '');
        }
        var tiles = [
          { label: 'New visitors (period)', value: t.visitorsNew, sub: 'first_seen in window' },
          { label: 'High intent leads (period)', value: t.intentLeadsHigh, sub: 'last_seen in window' },
          { label: 'Medium intent leads', value: t.intentLeadsMedium, sub: 'last_seen in window' },
          { label: 'Low intent leads', value: t.intentLeadsLow, sub: 'last_seen in window' },
          { label: 'Visitor → lead (period)', value: t.visitorsConvertedToLead, sub: 'Shared email/phone' },
          { label: 'Visitor → lead %', value: (t.visitorToLeadRatePercent != null ? t.visitorToLeadRatePercent + '%' : '—'), sub: 'Of new visitors in window' },
          { label: 'Leads collected (period)', value: t.leadsCollected, sub: 'first_seen in window' },
          { label: 'Converted (same cohort)', value: t.leadsConverted, sub: 'Has converted_order_id' },
          { label: 'Lead → order %', value: (t.leadToOrderConversionPercent != null ? t.leadToOrderConversionPercent + '%' : '—'), sub: 'Of leads collected in window' },
          { label: 'Paid orders (period)', value: t.ordersPaid, sub: 'Revenue ₹' + (t.revenueInr != null ? t.revenueInr.toFixed(2) : '—') },
          { label: 'From lead tracking', value: t.ordersAttributedToLead, sub: 'Orders with lead_id' },
          { label: 'Direct purchases', value: t.ordersDirectPurchase, sub: 'No lead_id on order' },
          { label: 'Page views (tracked)', value: t.pageViewsTotal, sub: 'page_view events' },
          { label: 'Visitors active', value: t.visitorsActiveInPeriod, sub: 'last_seen in window' },
          { label: 'Abandon rows', value: t.abandonedCheckoutSessions, sub: 'checkout last_event in window' },
          { label: 'Abandons → paid', value: t.abandonedLaterPaid, sub: 'later got converted_order_id' },
          { label: 'All-time orders', value: al.orders, sub: '—' },
          { label: 'All-time visitors', value: al.visitors, sub: '—' },
          { label: 'All-time leads', value: al.leads, sub: '—' },
          { label: 'Customers', value: al.customers, sub: '—' },
        ];
        analyticsGrid.innerHTML = '';
        tiles.forEach(function (x) {
          var d = document.createElement('div');
          d.className = 'analytics-tile';
          d.innerHTML =
            '<div class="analytics-tile-label">' +
            esc(x.label) +
            '</div><div class="analytics-tile-value">' +
            esc(String(x.value != null ? x.value : '—')) +
            '</div><div class="analytics-tile-sub">' +
            esc(x.sub || '') +
            '</div>';
          analyticsGrid.appendChild(d);
        });
        analyticsGrid.classList.add('hidden');
      })
      .catch(function () {
        if (analyticsGrid) {
          analyticsGrid.innerHTML = '<p class="detail-muted">Analytics unavailable.</p>';
          analyticsGrid.classList.add('hidden');
        }
      });

    if (!grid) return;
    grid.textContent = 'Loading…';
    Promise.all([
      fetch('/api/admin/orders?page=1&per_page=1', { headers: authHeaders() }).then(function (r) {
        return r.json();
      }),
      fetch('/api/admin/customers?page=1&per_page=1', { headers: authHeaders() }).then(function (r) {
        return r.json();
      }),
      fetch('/api/admin/leads?page=1&per_page=1', { headers: authHeaders() }).then(function (r) {
        return r.json();
      }),
      fetch('/api/admin/abandoned-checkouts?page=1&per_page=1', { headers: authHeaders() }).then(function (r) {
        return r.json();
      }),
    ])
      .then(function (results) {
        var parts = [
          { label: 'Orders API', ok: results[0] && results[0].ok },
          { label: 'Customers API', ok: results[1] && results[1].ok },
          { label: 'Leads API', ok: results[2] && results[2].ok },
          { label: 'Abandoned API', ok: results[3] && results[3].ok },
        ];
        grid.innerHTML = '';
        parts.forEach(function (p) {
          var div = document.createElement('div');
          div.className = 'dash-pill';
          div.innerHTML =
            '<span class="dash-pill-label">' +
            esc(p.label) +
            '</span> ' +
            (p.ok ? '<span class="badge badge--ok">OK</span>' : '<span class="badge badge--bad">Error</span>');
          grid.appendChild(div);
        });
      })
      .catch(function () {
        grid.textContent = 'Could not reach admin APIs.';
      });
  }

  function loadCurrentPanel() {
    switch (currentPanel) {
      case 'orders':
        loadOrders();
        break;
      case 'customers':
        loadStripToday(document.getElementById('customersPrimaryStrip'));
        loadCustomers();
        break;
      case 'visitors':
        loadStripToday(document.getElementById('visitorsPrimaryStrip'));
        loadVisitors();
        break;
      case 'leads':
        loadStripToday(document.getElementById('leadsPrimaryStrip'));
        loadLeads();
        break;
      case 'analytics':
        loadStripAnalyticsPanel();
        loadPageAnalytics();
        break;
      case 'abandoned':
        loadStripToday(document.getElementById('abandonedPrimaryStrip'));
        loadAbandoned();
        break;
      case 'dashboard':
        loadDashboardSummary();
        break;
      default:
        break;
    }
  }

  function wireFilters() {
    var main = document.querySelector('main.main');
    if (main) {
      main.addEventListener('click', function (e) {
        var t = e.target;
        if (!t || !t.closest) return;

        var dashChip = t.closest('button[data-dash-preset]');
        if (dashChip && dashChip.getAttribute('data-dash-preset')) {
          e.preventDefault();
          dashPreset = dashChip.getAttribute('data-dash-preset');
          var dFrom = document.getElementById('dashDateFrom');
          var dTo = document.getElementById('dashDateTo');
          if (dFrom) dFrom.value = '';
          if (dTo) dTo.value = '';
          updateDashChipActive();
          loadDashboardSummary();
          return;
        }

        var dateChip = t.closest('button[data-date-panel]');
        if (dateChip && dateChip.getAttribute('data-date-panel') && dateChip.getAttribute('data-date-preset')) {
          e.preventDefault();
          applyDatePresetForPanel(dateChip.getAttribute('data-date-panel'), dateChip.getAttribute('data-date-preset'));
          return;
        }

        var btn = t.closest('button');
        if (!btn || !btn.id) return;
        var bid = btn.id;

        if (bid === 'dashAnalyticsApply') {
          e.preventDefault();
          document.querySelectorAll('[data-dash-preset]').forEach(function (b) {
            b.classList.remove('filter-chip--active');
          });
          loadDashboardSummary();
          return;
        }
        if (bid === 'dashSaveSnapshot') {
          e.preventDefault();
          if (!canAuth()) return;
          var snapBody = {};
          var sdf = document.getElementById('dashDateFrom');
          var sdt = document.getElementById('dashDateTo');
          if (sdf && sdt && sdf.value && sdt.value) {
            snapBody.date_from = dtLocalToIso(sdf.value);
            snapBody.date_to = dtLocalToIso(sdt.value);
          } else {
            snapBody.preset = dashPreset || 'today';
          }
          fetch('/api/admin/analytics/snapshot', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, authHeaders()),
            body: JSON.stringify(snapBody),
          })
            .then(function (r) {
              return r.json();
            })
            .then(function (j) {
              var sc = document.getElementById('dashSnapshotsCard');
              if (j && j.ok && sc && !sc.hidden) loadSnapshotsList();
            })
            .catch(function () {});
          return;
        }

        if (bid === 'dashToggleHealth') {
          e.preventDefault();
          var hg = document.getElementById('dashHealthGrid');
          var isHidden = hg && hg.classList.contains('hidden');
          if (hg) {
            if (isHidden) hg.classList.remove('hidden');
            else hg.classList.add('hidden');
          }
          btn.textContent = isHidden ? 'Hide server health' : 'Server health';
          btn.setAttribute('aria-expanded', isHidden ? 'true' : 'false');
          return;
        }
        if (bid === 'dashToggleAllMetrics') {
          e.preventDefault();
          var ag = document.getElementById('analyticsGrid');
          var hid = ag && ag.classList.contains('hidden');
          if (ag) {
            if (hid) ag.classList.remove('hidden');
            else ag.classList.add('hidden');
          }
          btn.textContent = hid ? 'Hide all metrics' : 'All metrics';
          btn.setAttribute('aria-expanded', hid ? 'true' : 'false');
          return;
        }
        if (bid === 'dashToggleApiSnap') {
          e.preventDefault();
          var snap = document.getElementById('dashApiSummary');
          if (snap) snap.hidden = !snap.hidden;
          btn.textContent = snap && !snap.hidden ? 'Hide API check' : 'API check';
          return;
        }
        if (bid === 'dashToggleSnapshots') {
          e.preventDefault();
          var sc = document.getElementById('dashSnapshotsCard');
          var hidden = sc && sc.hidden;
          if (sc) sc.hidden = !hidden;
          btn.textContent = hidden ? 'Hide saved snapshots' : 'Show saved snapshots';
          btn.setAttribute('aria-expanded', hidden ? 'true' : 'false');
          if (hidden && canAuth()) loadSnapshotsList();
          return;
        }

        if (bid === 'ordersFilterApply') {
          e.preventDefault();
          ordersPage = 1;
          loadOrders();
          return;
        }
        if (bid === 'ordersFilterReset') {
          e.preventDefault();
          presetDateIsoByPanel.orders = null;
          ['ordersFilterSearch', 'ordersFilterDateFrom', 'ordersFilterDateTo'].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.value = '';
          });
          var os = document.getElementById('ordersFilterOrderStatus');
          if (os) os.value = '';
          var ac = document.getElementById('ordersFilterAcquisition');
          if (ac) ac.value = '';
          ordersPage = 1;
          loadOrders();
          return;
        }
        if (bid === 'customersFilterApply') {
          e.preventDefault();
          customersPage = 1;
          loadCustomers();
          return;
        }
        if (bid === 'customersFilterReset') {
          e.preventDefault();
          presetDateIsoByPanel.customers = null;
          var s = document.getElementById('customersFilterSearch');
          if (s) s.value = '';
          var cdf = document.getElementById('customersFilterDateFrom');
          var cdt = document.getElementById('customersFilterDateTo');
          if (cdf) cdf.value = '';
          if (cdt) cdt.value = '';
          var pay = document.getElementById('customersFilterPaying');
          if (pay) pay.value = '1';
          customersPage = 1;
          loadCustomers();
          return;
        }
        if (bid === 'leadsFilterApply') {
          e.preventDefault();
          leadsPage = 1;
          loadLeads();
          return;
        }
        if (bid === 'leadsFilterReset') {
          e.preventDefault();
          presetDateIsoByPanel.leads = null;
          [
            'leadsFilterSearch',
            'leadsFilterUtmSource',
            'leadsFilterUtmMedium',
            'leadsFilterUtmCampaign',
            'leadsFilterLeadStatus',
            'leadsFilterIntentTier',
            'leadsFilterIntentMin',
            'leadsFilterIntentMax',
            'leadsFilterDateFrom',
            'leadsFilterDateTo',
          ].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.value = '';
          });
          var cv = document.getElementById('leadsFilterConverted');
          if (cv) cv.value = '';
          var lc = document.getElementById('leadsFilterContactsOnly');
          if (lc) lc.value = '1';
          var ldf = document.getElementById('leadsFilterDateField');
          if (ldf) ldf.value = 'last_seen';
          leadsPage = 1;
          loadLeads();
          return;
        }
        if (bid === 'visitorsFilterApply') {
          e.preventDefault();
          visitorsPage = 1;
          loadVisitors();
          return;
        }
        if (bid === 'visitorsFilterReset') {
          e.preventDefault();
          presetDateIsoByPanel.visitors = null;
          ['visitorsFilterSearch', 'visitorsFilterConverted', 'visitorsFilterDateFrom', 'visitorsFilterDateTo'].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.value = '';
          });
          visitorsPage = 1;
          loadVisitors();
          return;
        }
        if (bid === 'analyticsApply') {
          e.preventDefault();
          loadPageAnalytics();
          if (analyticsSelectedPath) loadPageAnalyticsDetail(analyticsSelectedPath);
          return;
        }
        if (bid === 'abandonedFilterApply') {
          e.preventDefault();
          abandonedPage = 1;
          loadAbandoned();
          return;
        }
        if (bid === 'abandonedFilterReset') {
          e.preventDefault();
          presetDateIsoByPanel.abandoned = null;
          ['abandonedFilterSearch', 'abandonedFilterStage', 'abandonedFilterUtmCampaign', 'abandonedFilterDateFrom', 'abandonedFilterDateTo'].forEach(
            function (id) {
              var el = document.getElementById(id);
              if (el) el.value = '';
            }
          );
          abandonedPage = 1;
          loadAbandoned();
        }
      });
    }

    [['ordersPagePrev', -1], ['ordersPageNext', 1]].forEach(function (x) {
      var el = document.getElementById(x[0]);
      if (el)
        el.addEventListener('click', function () {
          ordersPage = Math.max(1, ordersPage + x[1]);
          loadOrders();
        });
    });
    [['customersPagePrev', -1], ['customersPageNext', 1]].forEach(function (x) {
      var el = document.getElementById(x[0]);
      if (el)
        el.addEventListener('click', function () {
          customersPage = Math.max(1, customersPage + x[1]);
          loadCustomers();
        });
    });
    [['leadsPagePrev', -1], ['leadsPageNext', 1]].forEach(function (x) {
      var el = document.getElementById(x[0]);
      if (el)
        el.addEventListener('click', function () {
          leadsPage = Math.max(1, leadsPage + x[1]);
          loadLeads();
        });
    });
    [['visitorsPagePrev', -1], ['visitorsPageNext', 1]].forEach(function (x) {
      var el = document.getElementById(x[0]);
      if (el)
        el.addEventListener('click', function () {
          visitorsPage = Math.max(1, visitorsPage + x[1]);
          loadVisitors();
        });
    });
    [['abandonedPagePrev', -1], ['abandonedPageNext', 1]].forEach(function (x) {
      var el = document.getElementById(x[0]);
      if (el)
        el.addEventListener('click', function () {
          abandonedPage = Math.max(1, abandonedPage + x[1]);
          loadAbandoned();
        });
    });
  }
  wireFilters();

  (function wirePresetDateClearListeners() {
    Object.keys(DATE_PANEL_IDS).forEach(function (panel) {
      var ids = DATE_PANEL_IDS[panel];
      ids.forEach(function (id) {
        var el = document.getElementById(id);
        if (el) {
          el.addEventListener('input', function () {
            presetDateIsoByPanel[panel] = null;
          });
        }
      });
    });
  })();

  (function wireOrderFiltersUx() {
    var ordersSearchTimer = null;
    var osch = document.getElementById('ordersFilterSearch');
    if (osch) {
      osch.addEventListener('input', function () {
        clearTimeout(ordersSearchTimer);
        ordersSearchTimer = setTimeout(function () {
          if (currentPanel !== 'orders' || !canAuth()) return;
          ordersPage = 1;
          loadOrders();
        }, 420);
      });
    }
    ['ordersFilterOrderStatus', 'ordersFilterAcquisition'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) {
        el.addEventListener('change', function () {
          if (currentPanel !== 'orders' || !canAuth()) return;
          ordersPage = 1;
          loadOrders();
        });
      }
    });
  })();

  (function wireDashDateInputs() {
    var ddf = document.getElementById('dashDateFrom');
    var ddt = document.getElementById('dashDateTo');
    function onDashDateInput() {
      document.querySelectorAll('[data-dash-preset]').forEach(function (b) {
        b.classList.remove('filter-chip--active');
      });
    }
    if (ddf) ddf.addEventListener('input', onDashDateInput);
    if (ddt) ddt.addEventListener('input', onDashDateInput);
  })();

  fetch('/api/health')
    .then(function (r) {
      return r.json();
    })
    .then(function (data) {
      var pill = document.getElementById('envPill');
      if (pill) pill.textContent = data.ok ? 'API OK' : 'Error';

      var ok = document.getElementById('healthOk');
      if (ok) ok.textContent = data.ok ? 'OK' : 'Error';

      var mode = document.getElementById('healthMode');
      if (mode) mode.textContent = '/api/health';

      var sb = document.getElementById('sbUrl');
      if (sb) {
        sb.textContent =
          data.supabase && data.supabase.url ? 'Connected' : 'Not in .env';
      }

      var sch = document.getElementById('schemaState');
      if (sch) sch.textContent = (data.supabase && data.supabase.schema) || 'v2';

      var odb = document.getElementById('ordersDbState');
      if (odb) odb.textContent = data.ordersDb ? 'Ready' : 'Need service role';

      var adm = document.getElementById('adminApiState');
      if (adm) adm.textContent = data.adminApi ? 'On' : 'Off';

      serverAdminApiEnabled = Boolean(data.adminApi);
      updateAdminPill();
    })
    .catch(function () {
      var pill = document.getElementById('envPill');
      if (pill) pill.textContent = 'API unreachable';
      var ok = document.getElementById('healthOk');
      if (ok) ok.textContent = '—';
    });

  function initAdminAuth() {
    if (!getSecret()) {
      updateAdminPill();
      updateDisconnectBtn();
      return;
    }
    var pill = document.getElementById('adminConnPill');
    if (pill) pill.dataset.checking = '1';
    updateAdminPill();
    var s = getSecret();
    verifyAdmin(s).then(function (res) {
      if (pill) delete pill.dataset.checking;
      applyVerifyResult(res, s);
      if (canAuth()) loadCurrentPanel();
    });
  }

  (function wireSettingsAndHealth() {
    var GTM_KEY = 'vedguide_admin_gtm_id';
    var inp = document.getElementById('gtmIdInput');
    var saveBtn = document.getElementById('gtmSaveBtn');
    var checkBtn = document.getElementById('gtmCheckBtn');
    var resEl = document.getElementById('gtmCheckResult');
    try {
      if (inp && localStorage.getItem(GTM_KEY)) inp.value = localStorage.getItem(GTM_KEY);
    } catch (e) {}
    if (saveBtn && inp) {
      saveBtn.addEventListener('click', function () {
        try {
          localStorage.setItem(GTM_KEY, inp.value.trim());
          if (resEl) resEl.textContent = 'Saved in this browser only. Add the same snippet on the live site.';
        } catch (e) {
          if (resEl) resEl.textContent = 'Could not save.';
        }
      });
    }
    if (checkBtn && inp) {
      checkBtn.addEventListener('click', function () {
        var id = inp.value.trim();
        if (resEl) resEl.textContent = 'Fetching homepage…';
        fetch('/', { credentials: 'same-origin' })
          .then(function (r) {
            return r.text();
          })
          .then(function (html) {
            var okId = id && html.indexOf(id) !== -1;
            var okGtm = html.indexOf('googletagmanager.com/gtm.js') !== -1 || html.indexOf('GTM-') !== -1;
            if (resEl) {
              resEl.textContent =
                (okId ? 'Homepage HTML mentions this id. ' : 'Id not found in raw HTML — paste GTM snippet in site layout. ') +
                (okGtm ? 'A GTM reference appears in the page.' : 'No GTM loader string found (site may use another tag).');
            }
          })
          .catch(function () {
            if (resEl) resEl.textContent = 'Could not fetch /. Check origin / CORS.';
          });
      });
    }
    var shBtn = document.getElementById('siteHealthBtn');
    var shList = document.getElementById('siteHealthList');
    if (shBtn && shList) {
      shBtn.addEventListener('click', function () {
        shList.innerHTML = '';
        function addLine(label, ok, detail) {
          var li = document.createElement('li');
          li.className = 'snapshots-item';
          li.innerHTML =
            esc(label) +
            ': ' +
            (ok ? '<span class="badge badge--ok">OK</span>' : '<span class="badge badge--bad">Fail</span>') +
            (detail ? ' — ' + esc(detail) : '');
          shList.appendChild(li);
        }
        fetch('/api/health')
          .then(function (r) {
            return r.json().then(function (j) {
              return { ok: r.ok, j: j };
            });
          })
          .then(function (x) {
            addLine('/api/health', Boolean(x.ok && x.j && x.j.ok), x.j && x.j.ordersDb ? 'writes OK' : 'check Supabase keys');
            return fetch('/');
          })
          .then(function (r) {
            addLine('GET / (homepage)', r.ok, 'HTTP ' + r.status);
          })
          .catch(function () {
            addLine('Health check', false, 'network');
          });
      });
    }
  })();

  initAdminAuth();
})();

/* ── Admin 2.0 enhancements ──────────────────────────────────────────── */
(function () {
  /* Mobile sidebar toggle */
  var sidebar = document.getElementById('sidebar');
  var overlay = document.getElementById('sidebarOverlay');
  var hamburger = document.getElementById('hamburgerBtn');

  function openSidebar() {
    if (sidebar) sidebar.classList.add('open');
    if (overlay) overlay.classList.add('active');
    document.body.style.overflow = 'hidden';
  }
  function closeSidebar() {
    if (sidebar) sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('active');
    document.body.style.overflow = '';
  }

  if (hamburger) hamburger.addEventListener('click', openSidebar);
  if (overlay) overlay.addEventListener('click', closeSidebar);

  /* Close sidebar when a nav item is clicked on mobile */
  document.querySelectorAll('.nav-item').forEach(function (a) {
    a.addEventListener('click', function () {
      if (window.innerWidth <= 768) closeSidebar();
    });
  });

  /* ── Copy toast ─── */
  var toastEl = document.getElementById('copyToast');
  var toastTimer = null;

  function showCopyToast(text) {
    if (!toastEl) return;
    toastEl.textContent = '\u2713 Copied: ' + (text.length > 30 ? text.slice(0, 30) + '\u2026' : text);
    toastEl.classList.add('show');
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { toastEl.classList.remove('show'); }, 2200);
  }

  function copyText(text) {
    if (!text || text === '\u2014') return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { showCopyToast(text); });
    } else {
      /* Fallback for older browsers */
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.cssText = 'position:fixed;opacity:0;top:0;left:0;';
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); showCopyToast(text); } catch (e) {}
      document.body.removeChild(ta);
    }
  }

  /* ── Enhance detail modal with copy + WhatsApp buttons ─── */
  function enhanceModalBody() {
    var body = document.getElementById('detailModalBody');
    if (!body) return;
    var kvRows = body.querySelectorAll('.detail-kv');
    kvRows.forEach(function (kv) {
      if (kv.dataset.enhanced) return;
      kv.dataset.enhanced = '1';
      var k = kv.querySelector('.detail-k');
      var v = kv.querySelector('.detail-v');
      if (!k || !v) return;
      var label = k.textContent.trim().toLowerCase();
      var val = v.textContent.trim();
      if (!val || val === '\u2014') return;

      var isPhone = label === 'phone';
      var isEmail = label === 'email';

      if (isPhone || isEmail) {
        /* Copy button */
        var copyBtn = document.createElement('button');
        copyBtn.className = 'btn btn--icon btn--ghost btn--small';
        copyBtn.title = 'Copy ' + label;
        copyBtn.textContent = '\u29C9';
        copyBtn.style.padding = '2px 7px';
        copyBtn.style.fontSize = '12px';
        copyBtn.addEventListener('click', function (e) {
          e.stopPropagation();
          copyText(val);
        });
        v.appendChild(copyBtn);
      }

      if (isPhone) {
        /* WhatsApp quick link */
        var digits = val.replace(/\D/g, '');
        if (digits.length >= 10) {
          var waNum = digits.length === 10 ? '91' + digits : digits;
          var waLink = document.createElement('a');
          waLink.className = 'btn btn--icon btn--green btn--small';
          waLink.title = 'Open WhatsApp';
          waLink.textContent = '\u{1F4AC}';
          waLink.href = 'https://wa.me/' + waNum;
          waLink.target = '_blank';
          waLink.rel = 'noopener noreferrer';
          waLink.style.padding = '2px 7px';
          waLink.style.fontSize = '12px';
          waLink.style.textDecoration = 'none';
          v.appendChild(waLink);

          /* Tel link */
          var callLink = document.createElement('a');
          callLink.className = 'btn btn--icon btn--ghost btn--small';
          callLink.title = 'Call';
          callLink.textContent = '\u{1F4DE}';
          callLink.href = 'tel:+' + waNum;
          callLink.style.padding = '2px 7px';
          callLink.style.fontSize = '12px';
          callLink.style.textDecoration = 'none';
          v.appendChild(callLink);
        }
      }
    });
  }

  /* Watch modal for open/close */
  var modalEl = document.getElementById('detailModal');
  if (modalEl && window.MutationObserver) {
    var mo = new MutationObserver(function (mutations) {
      mutations.forEach(function (m) {
        if (m.type === 'attributes' && m.attributeName === 'class') {
          if (!modalEl.classList.contains('hidden')) {
            /* Modal just opened — give app.js a moment to finish rendering */
            setTimeout(enhanceModalBody, 120);
          }
        }
      });
    });
    mo.observe(modalEl, { attributes: true });
  }

  /* ── Keyboard shortcut: press / to focus the visible search input ─── */
  document.addEventListener('keydown', function (e) {
    if (e.key !== '/' || e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    /* Find the search input in the currently visible panel */
    var panel = document.querySelector('.panel:not(.hidden)');
    if (!panel) return;
    var searchInput = panel.querySelector('input[type="search"], input[type="text"][id*="Search"]');
    if (searchInput) {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
    }
  });

})();
