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
   CLI TERMINAL TYPEWRITER
   ============================================================ */
(function () {
  const el = document.getElementById('terminal-body');
  if (!el) return;

  const seq = [
    { t: 'clear' },
    { t: 'prompt' },
    { t: 'cmd', text: 'rl chaos worker-kill --watch', speed: 32 },
    { t: 'pause', ms: 420 },
    { t: 'block', html:
`<span class="t-bad">CHAOS</span> <span class="t-out">Worker terminated.</span>
<span class="t-cmd">WATCH</span> <span class="t-out">Streaming resurrection events for 30s…</span>
  <span class="t-dim">-&gt;</span> <span class="t-warn">c9f1a3…: RESURRECTED (awaiting pickup)</span>
  <span class="t-ok">++</span> <span class="t-out">1 new resurrection(s) confirmed on broker (total=1)</span>
  <span class="t-dim">-&gt;</span> <span class="t-ok">c9f1a3…: ALIVE (revived by replacement worker)</span>
<span class="t-cmd">WATCH</span> <span class="t-dim">Done. 1 task(s) observed in monitor.</span>` },
    { t: 'pause', ms: 2600 },
    { t: 'clear' },
    { t: 'prompt' },
    { t: 'cmd', text: 'rl worker drain rl-worker-default@a8f2c1', speed: 28 },
    { t: 'pause', ms: 420 },
    { t: 'block', html:
`<span class="t-cmd">Initiating drain sequence:</span> <span class="t-out">rl-worker-default@a8f2c1</span>
<span class="t-ok">Signal broadcasted.</span>
<span class="t-dim">Worker will now enter the drain phase and exit cleanly.</span>` },
    { t: 'pause', ms: 2400 },
    { t: 'clear' },
    { t: 'prompt' },
    { t: 'cmd', text: 'rl dlq list', speed: 40 },
    { t: 'pause', ms: 420 },
    { t: 'block', html:
`<span class="t-dim"> ID               TASK              RESURRECTIONS   QUARANTINED_AT         LAST_ERROR</span>
<span class="t-dim"> ─────────────────────────────────────────────────────────────────────────────────────────────────────</span>
 <span class="t-dim">f8a2b1e0</span>         <span class="t-warn">process_document</span>  <span class="t-bad">5/5</span>             <span class="t-dim">2026-05-24 02:14:00</span>    <span class="t-bad">JSONDecodeError</span>
 <span class="t-dim">c3d9e2f1</span>         <span class="t-warn">process_webhook</span>   <span class="t-bad">5/5</span>             <span class="t-dim">2026-05-24 01:58:00</span>    <span class="t-bad">MemoryError</span>` },
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

  const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
  const defaults = window.__TWEAKS || { accent: '#6366f1', theme: 'dark', density: 'full' };
  const state = { ...defaults, theme: currentTheme };

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
    const dashboard = document.getElementById('hero-dashboard');
    const strip = document.querySelector('.hero__strip');
    if (state.density === 'minimal') {
      if (dashboard) dashboard.style.display = 'none';
      if (strip) strip.style.display = 'none';
    } else {
      if (dashboard) dashboard.style.display = '';
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
    if (patch.theme) {
      localStorage.setItem('theme', patch.theme);
    }
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

  // Listen for external theme changes (like navbar toggle)
  window.addEventListener('themechanged', (e) => {
    state.theme = e.detail.theme;
    apply();
  });

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

/* ============================================================
   NAVBAR THEME TOGGLE
   ============================================================ */
(function () {
  const themeToggle = document.getElementById('theme-toggle');
  if (!themeToggle) return;

  themeToggle.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') || 'dark';
    const nextTheme = current === 'dark' ? 'light' : 'dark';

    // Update document attribute
    document.documentElement.setAttribute('data-theme', nextTheme);

    // Persist in localStorage
    localStorage.setItem('theme', nextTheme);

    // Broadcast message to synchronize tweaks panel if open
    window.dispatchEvent(new CustomEvent('themechanged', { detail: { theme: nextTheme } }));
  });
})();
