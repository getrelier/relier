/* ============================================================
   RELIER LANDING PAGE — JavaScript
   Three.js hero, scroll animations, typewriter, copy-to-clipboard
   ============================================================ */

// --- THREE.JS HERO BACKGROUND ---
(function initHero() {
  const canvas = document.getElementById('hero-canvas');
  if (!canvas || typeof THREE === 'undefined') return;

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 1000);
  const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true, powerPreference: 'high-performance' });
  renderer.setSize(window.innerWidth, window.innerHeight);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  renderer.setClearColor(0x000000, 0);

  // Particles
  const count = 180;
  const positions = new Float32Array(count * 3);
  const spread = 28;
  for (let i = 0; i < count; i++) {
    positions[i * 3] = (Math.random() - 0.5) * spread;
    positions[i * 3 + 1] = (Math.random() - 0.5) * spread;
    positions[i * 3 + 2] = (Math.random() - 0.5) * spread * 0.5;
  }

  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
  const mat = new THREE.PointsMaterial({
    size: 0.06,
    color: 0x6366f1,
    transparent: true,
    opacity: 0.7,
    sizeAttenuation: true,
  });
  const points = new THREE.Points(geo, mat);
  scene.add(points);

  // Connection lines
  const lineGeo = new THREE.BufferGeometry();
  const lineMat = new THREE.LineBasicMaterial({ color: 0x6366f1, transparent: true, opacity: 0.08 });
  const lines = new THREE.LineSegments(lineGeo, lineMat);
  scene.add(lines);

  camera.position.z = 14;

  const clock = new THREE.Clock();

  function updateLines() {
    const pos = points.geometry.attributes.position.array;
    const linePositions = [];
    const threshold = 4.5;
    for (let i = 0; i < count; i++) {
      for (let j = i + 1; j < count; j++) {
        const dx = pos[i*3] - pos[j*3];
        const dy = pos[i*3+1] - pos[j*3+1];
        const dz = pos[i*3+2] - pos[j*3+2];
        const dist = Math.sqrt(dx*dx + dy*dy + dz*dz);
        if (dist < threshold) {
          linePositions.push(pos[i*3], pos[i*3+1], pos[i*3+2]);
          linePositions.push(pos[j*3], pos[j*3+1], pos[j*3+2]);
        }
      }
    }
    lineGeo.setAttribute('position', new THREE.Float32BufferAttribute(linePositions, 3));
  }

  function animate() {
    requestAnimationFrame(animate);
    const t = clock.getElapsedTime();
    const pos = points.geometry.attributes.position.array;
    for (let i = 0; i < count; i++) {
      pos[i*3+1] += Math.sin(t * 0.3 + i * 0.1) * 0.002;
      pos[i*3] += Math.cos(t * 0.2 + i * 0.15) * 0.001;
    }
    points.geometry.attributes.position.needsUpdate = true;
    points.rotation.y = t * 0.015;
    if (Math.floor(t * 2) % 3 === 0) updateLines();
    renderer.render(scene, camera);
  }
  animate();

  window.addEventListener('resize', () => {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
  });
})();


// --- SCROLL-TRIGGERED ANIMATIONS (IntersectionObserver) ---
(function initScrollReveal() {
  const els = document.querySelectorAll('.reveal');
  if (!els.length) return;
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('visible');
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.12, rootMargin: '0px 0px -40px 0px' });
  els.forEach(el => observer.observe(el));
})();


// --- NAV SCROLL EFFECT ---
(function initNav() {
  const nav = document.querySelector('.nav');
  if (!nav) return;
  window.addEventListener('scroll', () => {
    nav.classList.toggle('scrolled', window.scrollY > 40);
  }, { passive: true });
})();


// --- FEATURE TABS ---
(function initTabs() {
  const btns = document.querySelectorAll('.tab-btn');
  const panels = document.querySelectorAll('.feature-panel');
  if (!btns.length) return;

  btns.forEach(btn => {
    btn.addEventListener('click', () => {
      btns.forEach(b => b.classList.remove('active'));
      panels.forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      const target = document.getElementById(btn.dataset.tab);
      if (target) target.classList.add('active');
    });
  });
})();


// --- COPY TO CLIPBOARD ---
(function initCopy() {
  document.querySelectorAll('[data-copy]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const text = btn.dataset.copy;
      try {
        await navigator.clipboard.writeText(text);
        btn.classList.add('copied');
        const orig = btn.querySelector('.copy-label');
        if (orig) {
          const prev = orig.textContent;
          orig.textContent = 'Copied!';
          setTimeout(() => { orig.textContent = prev; btn.classList.remove('copied'); }, 2000);
        } else {
          setTimeout(() => btn.classList.remove('copied'), 2000);
        }
      } catch (e) { /* fallback */ }
    });
  });
})();


// --- CLI TYPEWRITER ---
(function initTypewriter() {
  const el = document.getElementById('terminal-body');
  if (!el) return;

  const sequences = [
    { type: 'prompt', text: '$ ' },
    { type: 'command', text: 'rl tasks inflight --follow', speed: 35 },
    { type: 'pause', ms: 500 },
    { type: 'block', text: `
<span class="terminal__output">WORKER              TASK                 DURATION  STATUS    QUEUE</span>
<span class="terminal__output">─────────────────────────────────────────────────────────────────────</span>
<span class="terminal__output">rl-worker-1         process_document     12.4s     running   high_priority</span>
<span class="terminal__output">rl-worker-2         send_invoice          3.1s     running   default</span>
<span class="terminal__output">rl-worker-3         classify_text        28.2s</span> <span class="terminal__warning">⚠</span>  <span class="terminal__output">running   default</span>
<span class="terminal__output">rl-worker-4         (idle)</span>

<span class="terminal__success">✓</span> <span class="terminal__output">3 tasks running · 1 worker idle · queue depth: 147 · p95: 18.2s</span>
` },
    { type: 'pause', ms: 2500 },
    { type: 'clear' },
    { type: 'prompt', text: '$ ' },
    { type: 'command', text: 'rl chaos worker-kill --watch', speed: 35 },
    { type: 'pause', ms: 500 },
    { type: 'block', text: `
<span class="terminal__warning">⚠</span> <span class="terminal__output">Sending SIGKILL to rl-worker-3 (PID 28401)...</span>
<span class="terminal__warning">⚠</span> <span class="terminal__output">Worker rl-worker-3 terminated.</span>

<span class="terminal__output">Phoenix resurrector scanning...</span>
<span class="terminal__success">✓</span> <span class="terminal__output">task_c9f1a3 (classify_text) resurrected → rl-worker-1</span>
<span class="terminal__success">✓</span> <span class="terminal__output">Resurrection time: 4.2s</span>
<span class="terminal__success">✓</span> <span class="terminal__output">Zero tasks lost.</span>
` },
    { type: 'pause', ms: 3000 },
    { type: 'clear' },
    { type: 'prompt', text: '$ ' },
    { type: 'command', text: 'rl dlq list', speed: 40 },
    { type: 'pause', ms: 500 },
    { type: 'block', text: `
<span class="terminal__output">ID            TASK                RESURRECTIONS  QUARANTINED_AT       LAST_ERROR</span>
<span class="terminal__output">────────────────────────────────────────────────────────────────────────────────</span>
<span class="terminal__output">task_f8a2b1   process_document    5/5            2026-05-11 02:14     JSONDecodeError</span>
<span class="terminal__output">task_c3d9e2   process_webhook     5/5            2026-05-11 01:58     MemoryError</span>

<span class="terminal__output">2 tasks quarantined · Use</span> <span class="terminal__success">rl dlq inspect {id}</span> <span class="terminal__output">for full details</span>
` },
    { type: 'pause', ms: 3000 },
    { type: 'clear' },
  ];

  let running = true;

  async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  async function typeText(text, speed) {
    for (const char of text) {
      el.innerHTML += char;
      el.scrollTop = el.scrollHeight;
      await sleep(speed);
    }
  }

  async function runSequence() {
    while (running) {
      for (const seq of sequences) {
        if (!running) break;
        switch (seq.type) {
          case 'prompt':
            el.innerHTML += `<span class="terminal__prompt">${seq.text}</span>`;
            break;
          case 'command':
            await typeText(seq.text, seq.speed || 40);
            el.innerHTML += '\n';
            break;
          case 'block':
            el.innerHTML += seq.text;
            break;
          case 'pause':
            await sleep(seq.ms);
            break;
          case 'clear':
            el.innerHTML = '';
            break;
        }
      }
    }
  }

  // Start when visible
  const obs = new IntersectionObserver(entries => {
    if (entries[0].isIntersecting) {
      runSequence();
      obs.disconnect();
    }
  }, { threshold: 0.3 });
  obs.observe(el.parentElement);
})();
