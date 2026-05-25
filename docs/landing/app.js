/* ============================================================
   RELIER · 2026 LANDING — INTERACTIONS
   Live worker monitor, terminal typewriter, tabs, copy, tweaks
   ============================================================ */

/* ----- SCROLL REVEAL ----- */
(function () {
  const els = document.querySelectorAll('.reveal');
  if (!els.length) return;
  const io = new IntersectionObserver((ents) => {
    ents.forEach(e => {
      if (e.isIntersecting) {
        e.target.classList.add('in');
        io.unobserve(e.target);
      }
    });
  }, { threshold: 0.08, rootMargin: '0px 0px -40px 0px' });
  els.forEach(el => io.observe(el));
})();

/* ----- NAV ----- */
(function () {
  const nav = document.getElementById('nav');
  if (!nav) return;
  const onScroll = () => nav.classList.toggle('scrolled', window.scrollY > 24);
  onScroll();
  window.addEventListener('scroll', onScroll, { passive: true });
})();

/* ----- FEATURE TABS ----- */
(function () {
  const tabs = document.querySelectorAll('.tab');
  const panels = document.querySelectorAll('.feature');
  if (!tabs.length) return;
  tabs.forEach(t => {
    t.addEventListener('click', () => {
      tabs.forEach(x => x.classList.remove('active'));
      panels.forEach(p => p.classList.remove('active'));
      t.classList.add('active');
      const panel = document.getElementById(t.dataset.tab);
      if (panel) panel.classList.add('active');
    });
  });
})();

/* ----- COPY TO CLIPBOARD ----- */
(function () {
  document.querySelectorAll('[data-copy]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const text = btn.dataset.copy;
      try { await navigator.clipboard.writeText(text); } catch (e) {}
      btn.classList.add('copied');
      const label = btn.querySelector('.copy-label');
      if (label) {
        const prev = label.textContent;
        label.textContent = 'copied';
        setTimeout(() => {
          label.textContent = prev;
          btn.classList.remove('copied');
        }, 1600);
      } else {
        setTimeout(() => btn.classList.remove('copied'), 1600);
      }
    });
  });
})();

/* ============================================================
   LIVE WORKER MONITOR
   ============================================================ */
(function () {
  const rowsEl = document.getElementById('monitor-rows');
  const eventEl = document.getElementById('monitor-event');
  const clockEl = document.getElementById('monitor-clock');
  const queueEl = document.getElementById('m-queue');
  const p95El   = document.getElementById('m-p95');
  if (!rowsEl) return;

  // Worker state: 4 rows, indexed by row id w1..w4
  const STATE = {
    w1: { worker: 'rl-worker-1', task: 'process_document', queue: 'high_priority', dur: 12.4, status: 'live' },
    w2: { worker: 'rl-worker-2', task: 'send_invoice',     queue: 'default',       dur: 3.1,  status: 'live' },
    w3: { worker: 'rl-worker-3', task: 'classify_text',    queue: 'default',       dur: 28.2, status: 'live' },
    w4: { worker: 'rl-worker-4', task: '—',                queue: '—',             dur: 0,    status: 'idle' },
  };

  const TASK_POOL = [
    'process_document', 'send_invoice', 'classify_text',
    'index_embeddings', 'process_webhook', 'render_pdf',
    'rotate_keys', 'sync_stripe', 'enqueue_email',
  ];
  const QUEUE_POOL = ['high_priority', 'default', 'default', 'low'];

  function render() {
    rowsEl.querySelectorAll('.monitor__row').forEach(row => {
      const id = row.dataset.id;
      const s = STATE[id];
      if (!s) return;
      row.querySelector('.worker').textContent = s.worker;
      row.querySelector('.task').textContent = s.task;
      row.querySelector('.duration').textContent = s.status === 'idle' ? '—' : s.dur.toFixed(1) + 's';
      row.querySelector('.queue').textContent = s.queue;
      const st = row.querySelector('.status');
      st.classList.remove('live','idle','died','requeue','resurrected');
      st.classList.add(s.status);
      const labels = {
        live: 'running',
        idle: 'idle',
        died: 'died',
        requeue: 'requeue',
        resurrected: 'resurrected',
      };
      const ind = st.querySelector('.indicator');
      st.innerHTML = '';
      st.appendChild(ind);
      st.appendChild(document.createTextNode(' ' + labels[s.status]));
    });
  }

  function setEvent(text, kind) {
    eventEl.textContent = text;
    eventEl.classList.remove('success','warn','accent');
    if (kind) eventEl.classList.add(kind);
  }

  function flashRow(id, cls) {
    const row = rowsEl.querySelector(`.monitor__row[data-id="${id}"]`);
    if (!row) return;
    row.classList.add(cls);
    setTimeout(() => row.classList.remove(cls), 900);
  }

  // Slowly tick durations
  setInterval(() => {
    Object.values(STATE).forEach(s => {
      if (s.status === 'live' || s.status === 'requeue' || s.status === 'resurrected') {
        s.dur += 0.1;
      }
    });
    // Random queue/p95 jitter
    if (Math.random() < 0.4 && queueEl) queueEl.textContent = (140 + Math.floor(Math.random() * 30));
    if (Math.random() < 0.3 && p95El) p95El.textContent = (17 + Math.random() * 3).toFixed(1) + 's';
    render();
  }, 800);

  // Phoenix scan clock
  let scanT = 5;
  setInterval(() => {
    scanT -= 1;
    if (scanT <= 0) scanT = 5;
    if (clockEl && !clockEl.dataset.locked) {
      clockEl.textContent = `phoenix scan · ${scanT}s`;
    }
  }, 1000);

  // The main loop: every ~9s, kill a worker, resurrect it
  const sequence = [
    { at: 6000, fn: () => {
      // Worker 3 dies
      STATE.w3.status = 'died';
      STATE.w3.dur = 32.4;
      flashRow('w3', 'flash-die');
      setEvent('⚠  rl-worker-3 heartbeat expired — SIGKILL detected', 'warn');
      clockEl.textContent = 'phoenix · detecting…';
      clockEl.dataset.locked = '1';
      render();
    }},
    { at: 8400, fn: () => {
      STATE.w3.status = 'requeue';
      STATE.w3.task = 'classify_text';
      STATE.w3.dur = 0;
      setEvent('↻  task_c9f1a3 re-queued from rl-worker-3 → rl-worker-4', 'accent');
      render();
    }},
    { at: 10000, fn: () => {
      // worker 4 picks up
      STATE.w4.status = 'resurrected';
      STATE.w4.task = 'classify_text';
      STATE.w4.queue = 'default';
      STATE.w4.dur = 0.4;
      STATE.w3.status = 'idle';
      STATE.w3.task = '—';
      STATE.w3.queue = '—';
      STATE.w3.dur = 0;
      flashRow('w4', 'flash-resurrect');
      setEvent('✓  task_c9f1a3 resurrected on rl-worker-4 · 0 lost', 'success');
      render();
    }},
    { at: 14000, fn: () => {
      STATE.w4.status = 'live';
      setEvent('3 tasks running · 0 lost · phoenix idle', null);
      delete clockEl.dataset.locked;
      render();
    }},
    { at: 18000, fn: () => {
      // small mutation — swap a task
      STATE.w2.task = TASK_POOL[Math.floor(Math.random() * TASK_POOL.length)];
      STATE.w2.dur = 0.4;
      render();
    }},
  ];

  function runLoop() {
    const start = Date.now();
    sequence.forEach(step => {
      setTimeout(step.fn, step.at);
    });
    // schedule next loop
    const total = 22000;
    setTimeout(runLoop, total);
  }

  // Start when the monitor scrolls into view (or immediately for above-fold)
  const monitor = document.getElementById('monitor');
  const monIO = new IntersectionObserver((ents) => {
    if (ents[0].isIntersecting) {
      runLoop();
      monIO.disconnect();
    }
  }, { threshold: 0.1 });
  monIO.observe(monitor);
})();

/* ============================================================
   CLI TERMINAL TYPEWRITER
   ============================================================ */
(function () {
  const el = document.getElementById('terminal-body');
  if (!el) return;

  const seq = [
    { t: 'prompt' },
    { t: 'cmd', text: 'rl tasks inflight --follow', speed: 38 },
    { t: 'pause', ms: 480 },
    { t: 'block', html:
`<span class="t-dim">WORKER              TASK                 DURATION  STATUS         QUEUE</span>
<span class="t-dim">─────────────────────────────────────────────────────────────────────────</span>
<span class="t-cmd">rl-worker-1</span>         <span class="t-out">process_document</span>     <span class="t-out">12.4s</span>     <span class="t-ok">● running</span>      <span class="t-dim">high_priority</span>
<span class="t-cmd">rl-worker-2</span>         <span class="t-out">send_invoice</span>          <span class="t-out">3.1s</span>     <span class="t-ok">● running</span>      <span class="t-dim">default</span>
<span class="t-cmd">rl-worker-3</span>         <span class="t-out">classify_text</span>        <span class="t-out">28.2s</span>     <span class="t-warn">● running ⚠</span>    <span class="t-dim">default</span>
<span class="t-cmd">rl-worker-4</span>         <span class="t-out">(idle)</span>                <span class="t-dim">—</span>        <span class="t-dim">○ idle</span>         <span class="t-dim">—</span>

<span class="t-ok">✓</span> <span class="t-out">3 tasks running · 1 idle · queue depth 147 · p95 18.2s</span>` },
    { t: 'pause', ms: 2200 },
    { t: 'clear' },
    { t: 'prompt' },
    { t: 'cmd', text: 'rl chaos worker-kill --watch rl-worker-3', speed: 30 },
    { t: 'pause', ms: 420 },
    { t: 'block', html:
`<span class="t-warn">⚠</span> <span class="t-out">Sending SIGKILL to rl-worker-3 (PID 28401)…</span>
<span class="t-warn">⚠</span> <span class="t-out">Worker rl-worker-3 terminated.</span>

<span class="t-out">Phoenix resurrector scanning rl:phoenix:* …</span>
<span class="t-ok">✓</span> <span class="t-out">task_c9f1a3 (classify_text) heartbeat expired</span>
<span class="t-ok">✓</span> <span class="t-out">re-queue → rl-worker-4 · resurrection in 4.2s</span>
<span class="t-ok">✓</span> <span class="t-out">Zero tasks lost.</span>` },
    { t: 'pause', ms: 2800 },
    { t: 'clear' },
    { t: 'prompt' },
    { t: 'cmd', text: 'rl dlq list', speed: 40 },
    { t: 'pause', ms: 420 },
    { t: 'block', html:
`<span class="t-dim">ID            TASK                RESURRECTIONS   QUARANTINED_AT       LAST_ERROR</span>
<span class="t-dim">─────────────────────────────────────────────────────────────────────────────────</span>
<span class="t-cmd">task_f8a2b1</span>   <span class="t-out">process_document</span>    <span class="t-bad">5/5</span>             <span class="t-dim">2026-05-11 02:14</span>     <span class="t-bad">JSONDecodeError</span>
<span class="t-cmd">task_c3d9e2</span>   <span class="t-out">process_webhook</span>     <span class="t-bad">5/5</span>             <span class="t-dim">2026-05-11 01:58</span>     <span class="t-bad">MemoryError</span>

<span class="t-out">2 quarantined · </span><span class="t-ok">rl dlq inspect &lt;id&gt;</span><span class="t-out"> for full payload</span>` },
    { t: 'pause', ms: 3000 },
    { t: 'clear' },
  ];

  let alive = true;
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  async function typeText(text, speed) {
    for (const ch of text) {
      el.insertAdjacentHTML('beforeend', ch === '<' ? '&lt;' : ch === '>' ? '&gt;' : ch);
      el.scrollTop = el.scrollHeight;
      await sleep(speed);
      if (!alive) return;
    }
  }

  async function run() {
    while (alive) {
      for (const s of seq) {
        if (!alive) break;
        switch (s.t) {
          case 'prompt':
            el.insertAdjacentHTML('beforeend', `<span class="t-prompt">$ </span>`);
            break;
          case 'cmd':
            await typeText(s.text, s.speed || 40);
            el.insertAdjacentHTML('beforeend', '\n');
            break;
          case 'block':
            el.insertAdjacentHTML('beforeend', '\n' + s.html + '\n');
            el.scrollTop = el.scrollHeight;
            break;
          case 'pause':
            await sleep(s.ms);
            break;
          case 'clear':
            el.innerHTML = '';
            break;
        }
      }
    }
  }

  const io = new IntersectionObserver((ents) => {
    if (ents[0].isIntersecting) {
      run();
      io.disconnect();
    }
  }, { threshold: 0.3 });
  io.observe(el.parentElement);
})();

/* ============================================================
   TWEAKS PANEL · accent / theme / density
   ============================================================ */
(function () {
  const panel = document.getElementById('tweaks');
  const closeBtn = document.getElementById('tweaks-close');
  if (!panel) return;

  const defaults = window.__TWEAKS || { accent: '#6366f1', theme: 'dark', density: 'full' };
  const state = { ...defaults };

  function apply() {
    // Accent
    const a = state.accent;
    document.documentElement.style.setProperty('--accent', a);
    document.documentElement.style.setProperty('--accent-2', lighten(a, 0.18));
    document.documentElement.style.setProperty('--accent-dim', hexToRgba(a, 0.50));
    document.documentElement.style.setProperty('--accent-bg', hexToRgba(a, 0.08));
    document.documentElement.style.setProperty('--accent-line', hexToRgba(a, 0.22));

    // Theme
    document.documentElement.setAttribute('data-theme', state.theme);

    // Density
    const monitor = document.getElementById('monitor');
    const strip = document.querySelector('.hero__strip');
    if (state.density === 'minimal') {
      if (monitor) monitor.style.display = 'none';
      if (strip) strip.style.display = 'none';
    } else {
      if (monitor) monitor.style.display = '';
      if (strip) strip.style.display = '';
    }

    // Reflect into swatches
    document.querySelectorAll('#tw-accent .tweaks__swatch').forEach(b =>
      b.classList.toggle('active', b.dataset.accent.toLowerCase() === state.accent.toLowerCase()));
    document.querySelectorAll('#tw-theme button').forEach(b =>
      b.classList.toggle('active', b.dataset.theme === state.theme));
    document.querySelectorAll('#tw-density button').forEach(b =>
      b.classList.toggle('active', b.dataset.density === state.density));
  }

  function persist(patch) {
    Object.assign(state, patch);
    apply();
    try {
      window.parent.postMessage({ type: '__edit_mode_set_keys', edits: patch }, '*');
    } catch (e) {}
  }

  // —— Color helpers ——
  function hexToRgb(hex) {
    const h = hex.replace('#', '');
    const v = parseInt(h.length === 3 ? h.split('').map(c => c + c).join('') : h, 16);
    return { r: (v >> 16) & 255, g: (v >> 8) & 255, b: v & 255 };
  }
  function hexToRgba(hex, a) {
    const { r, g, b } = hexToRgb(hex);
    return `rgba(${r},${g},${b},${a})`;
  }
  function lighten(hex, amt) {
    const { r, g, b } = hexToRgb(hex);
    const L = (c) => Math.round(c + (255 - c) * amt);
    return `rgb(${L(r)},${L(g)},${L(b)})`;
  }

  // Wire controls
  document.querySelectorAll('#tw-accent .tweaks__swatch').forEach(btn =>
    btn.addEventListener('click', () => persist({ accent: btn.dataset.accent })));
  document.querySelectorAll('#tw-theme button').forEach(btn =>
    btn.addEventListener('click', () => persist({ theme: btn.dataset.theme })));
  document.querySelectorAll('#tw-density button').forEach(btn =>
    btn.addEventListener('click', () => persist({ density: btn.dataset.density })));

  // Edit-mode integration (toolbar toggle)
  window.addEventListener('message', (e) => {
    const d = e.data || {};
    if (d.type === '__activate_edit_mode') panel.classList.add('open');
    if (d.type === '__deactivate_edit_mode') panel.classList.remove('open');
  });

  closeBtn.addEventListener('click', () => {
    panel.classList.remove('open');
    try { window.parent.postMessage({ type: '__edit_mode_dismissed' }, '*'); } catch (e) {}
  });

  // Announce availability AFTER listener is wired
  try { window.parent.postMessage({ type: '__edit_mode_available' }, '*'); } catch (e) {}

  apply();
})();
