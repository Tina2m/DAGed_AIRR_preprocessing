(function(){
  if (document.querySelector('.site-header')) return;

  var here = (document.querySelector('meta[name="page"]')?.getAttribute('content') || '').toLowerCase();
  var path = location.pathname.replace(/\/+$/,'').toLowerCase();

  var header = document.createElement('header');
  header.className = 'site-header';
  header.setAttribute('role','banner');
  header.innerHTML = [
    '<div class="bar">',
      '<a class="brand" href="/ui/">',
        '<svg class="dna" viewBox="0 0 24 24" fill="none" aria-hidden="true">',
          '<defs>',
            '<linearGradient id="g-dna" x1="0" y1="0" x2="24" y2="24">',
              '<stop stop-color="var(--grad-from)"></stop>',
              '<stop offset="1" stop-color="var(--grad-to)"></stop>',
            '</linearGradient>',
          '</defs>',
          '<path d="M5 4c4 0 6 4 7 8s3 8 7 8M5 20c4 0 6-4 7-8S15 4 19 4" stroke="url(#g-dna)" stroke-width="2" stroke-linecap="round"></path>',
          '<path d="M7 8h10M7 16h10" stroke="url(#g-dna)" stroke-width="2" stroke-linecap="round" opacity=".75"></path>',
        '</svg>',
        '<span class="title">ImmunoStream</span>',
      '</a>',
      '<nav class="nav" aria-label="Primary">',
        '<a class="nav-link" data-key="home" href="/ui/">Home</a>',
        '<a class="nav-link" data-key="bulk" href="/ui/blk.html">Bulk amplicon</a>',
        '<a class="nav-link" data-key="sc"   href="/ui/sc.html">Single-Cell AIRR</a>',
        '<a class="nav-link" data-key="docs" href="/ui/docs.html">Documents</a>',
      '</nav>',
    '</div>'
  ].join('');

  document.body.classList.add('has-site-header');
  document.body.insertBefore(header, document.body.firstChild);

  var key = here;
  if (!key) {
    if (path.endsWith('/ui') || path.endsWith('/ui/index.html') || path === '/ui') key = 'home';
    else if (path.endsWith('/blk.html')) key = 'bulk';
    else if (path.endsWith('/sc.html'))  key = 'sc';
    else if (path.endsWith('/docs.html')) key = 'docs';
  }
  if (key) {
    var el = header.querySelector('.nav-link[data-key="'+key+'"]');
    if (el) el.classList.add('active');
  }
})();