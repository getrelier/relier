(function () {
  var shellLangs = [
    'language-bash', 'language-shell',
    'language-sh', 'language-powershell'
  ];
  var selector = shellLangs.map(function (c) {
    return 'div.' + c + ' pre code';
  }).join(', ');

  function highlight() {
    document.querySelectorAll(selector).forEach(function (block) {
      if (block.dataset.rlFlagProcessed) return;

      // ENV_VAR=value: Pygments always emits
      //   <span class="nv">KEY</span><span class="o">=</span>VALUE
      // VALUE is plain text (URLs, strings) or a single span (numbers, builtins).
      // Wrap the value in .rl-env-val so CSS can colour it near-white.
      block.innerHTML = block.innerHTML.replace(
        /<span class="nv">([A-Z_][A-Z0-9_]*)<\/span><span class="o">=<\/span>(<span[^>]*>[^<]*<\/span>|[^\s<\n]+)/g,
        '<span class="rl-env-key">$1</span><span class="o">=</span><span class="rl-env-val">$2</span>'
      );

      // --flag and -f options: light-blue, distinct from teal command text.
      // Matches both double-dash long flags and single-dash short flags.
      block.innerHTML = block.innerHTML.replace(
        /(--[a-zA-Z][a-zA-Z0-9-]*|-[a-zA-Z]\b)/g,
        '<span class="rl-flag">$1</span>'
      );

      // $ prompt at the start of a line — grey/dim.
      block.innerHTML = block.innerHTML.replace(
        /(\n|^)(\s*)(\$ )/g,
        '$1$2<span class="rl-prompt">$3</span>'
      );

      // Table separator lines (─── chars) — very dim.
      block.innerHTML = block.innerHTML.replace(
        /([ \t]*─{3,}[ ─]*)/g,
        '<span class="rl-sep">$1</span>'
      );

      block.dataset.rlFlagProcessed = '1';
    });
  }

  window.addEventListener('load', highlight);

  if (typeof document$ !== 'undefined') {
    document$.subscribe(highlight);
  }
})();
