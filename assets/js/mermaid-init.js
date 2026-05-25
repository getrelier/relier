(function () {
  function render() {
    if (typeof mermaid === 'undefined') return;
    var nodes = Array.from(document.querySelectorAll('pre.mermaid, div.mermaid'));
    if (!nodes.length) return;
    mermaid.initialize({
      startOnLoad: false,
      theme: 'dark',
      securityLevel: 'loose',
      themeVariables: { darkMode: true, background: '#0a0e17' }
    });
    mermaid.run({ nodes: nodes });
  }

  // Initial page load — wait for all scripts (including the mermaid CDN) to finish
  window.addEventListener('load', render);

  // MkDocs Material instant navigation re-fires this observable on each page
  if (typeof document$ !== 'undefined') {
    document$.subscribe(render);
  }
})();
