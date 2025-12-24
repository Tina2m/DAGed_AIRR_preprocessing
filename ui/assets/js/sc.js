// Single-Cell page with grouped accordions, search, and compact cards
let SID = null;
let UNITS_META = [];
let FLOW = []; // [{unitId,label,params}]
let running = false;

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

// ----- Session (auto) -----
async function ensureSession(){
  if(SID) return SID;
  const r = await fetch('/session/start',{method:'POST'});
  const j = await r.json();
  SID = j.session_id;
  window.__SID__ = SID;      // keep accessible, not visible
  await refreshState();
  return SID;
}

// ----- Upload -----
async function uploadSCFiles(){
  await ensureSession();
  const files = $('#sc-files').files;
  if(!files || !files.length){ alert('Choose at least one file'); return; }
  $('#upload-msg').textContent = `Uploading ${files.length} file(s)…`;
  let ok = 0;
  for(const f of files){
    const fd = new FormData();
    fd.append('file', f);
    fd.append('name', f.name);
    const r = await fetch(`/session/${SID}/upload-aux`, {method:'POST', body:fd});
    if(r.ok) ok++;
  }
  $('#upload-msg').textContent = `Uploaded ${ok}/${files.length} files.`;
  listUploaded(files);
  await refreshState();
}
function listUploaded(fileList){
  const names = Array.from(fileList).map(f => esc(f.name));
  $('#uploaded-list').innerHTML = names.length ? 'Uploaded: ' + names.join(', ') : '';
}

// ----- Units rendering (grouped) -----
const GROUPS = [
  { id:'merge',    title:'I/O & Merge',       match:u => (u.id||'').includes('merge') },
  { id:'qc',       title:'QC & Filtering',    match:u => /(filter|remove)/.test(u.id||'') },
  { id:'other',    title:'Other',             match:u => true }
];
function groupOf(u){
  for(const g of GROUPS){ if(g.match(u)) return g.id; }
  return 'other';
}
function renderGroups(units){
  const wrap = $('#groups'); wrap.innerHTML = '';
  const buckets = Object.fromEntries(GROUPS.map(g=>[g.id, []]));
  units.forEach(u => buckets[groupOf(u)].push(u));

  for(const g of GROUPS){
    if(buckets[g.id].length === 0) continue;
    const container = document.createElement('div');
    container.className = 'unit-group open';
    container.dataset.group = g.id;
    container.innerHTML = `
      <div class="group-head" role="button" tabindex="0">
        <h3>${esc(g.title)}</h3>
        <span class="count">${buckets[g.id].length}</span>
      </div>
      <div class="group-body"></div>`;
    const body = container.querySelector('.group-body');
    buckets[g.id].forEach(u => body.appendChild(buildUnitCard(u)));
    // toggle
    const head = container.querySelector('.group-head');
    const toggle = () => container.classList.toggle('open');
    head.addEventListener('click', toggle);
    head.addEventListener('keypress', e => { if(e.key==='Enter' || e.key===' ') { e.preventDefault(); toggle(); }});
    wrap.appendChild(container);
  }
}

function buildUnitCard(u){
  const card = document.createElement('div');
  card.className = 'unit-card';
  card.dataset.unit = u.id;

  const requires = (u.requires||[]).map(x=>`<span class="pill">${esc(x)}</span>`).join(' ') || '<span class="muted">none</span>';
  let paramsHTML = '';
  for (const [k,v] of Object.entries(u.params_schema||{})) {
    const help = v.help ? ` <span class="muted">— ${esc(v.help)}</span>` : '';
    const label = `<label>${esc(k)}${help}</label>`;
    if (v.type === 'select') {
      const opts = (v.options||[]).map(o => `<option value="${esc(o)}" ${o===v.default?'selected':''}>${esc(o)}</option>`).join('');
      paramsHTML += `${label}<select name="${esc(k)}">${opts}</select>`;
    } else {
      const val = v.default ?? ''; const ph = v.placeholder ?? '';
      const t = (v.type === 'int' || v.type === 'number') ? 'number' : 'text';
      paramsHTML += `${label}<input type="${t}" name="${esc(k)}" value="${esc(val)}" placeholder="${esc(ph)}">`;
    }
  }

  card.innerHTML = `
    <div class="uc-head">
      <div class="uc-title">${esc(u.label)}</div>
      <div class="req">requires: ${requires}</div>
      <button class="params-toggle" title="Show/Hide parameters">Parameters</button>
    </div>
    <div class="params">
      <div class="params-wrap">${paramsHTML || '<div class="muted">No parameters</div>'}</div>
      <div class="row mt8">
        <button class="run">Run</button>
        <button class="secondary addflow">Add to flow</button>
      </div>
    </div>`;

  // Behavior
  const pwrap = card.querySelector('.params-wrap');
  card.querySelector('.params-toggle').addEventListener('click', ()=>{
    pwrap.classList.toggle('open');
  });
  card.querySelector('.run').addEventListener('click', ()=>runSingle(card, u.id, u.label));
  card.querySelector('.addflow').addEventListener('click', ()=>addToFlow(card, u.id, u.label));

  if(u.id === 'sc_remove_multi_heavy'){
    decorateMultiHeavyCard(card);
  }

  return card;
}

function decorateMultiHeavyCard(card){
  const select = card.querySelector('select[name="heavy_value"]');
  if(!select) return;
  const groupName = `mh-mode-${Math.random().toString(36).slice(2,8)}`;
  const modes = {
    bcr: {
      label: 'BCR',
      values: ['IGH','IGK','IGL','IGH,IGK','IGH,IGL'],
      title: 'SC: Remove cells with multiple IgH',
    },
    tcr: {
      label: 'TCR',
      values: ['TRA','TRB','TRA,TRB'],
      title: 'SC: Remove cells with multiple TRA/TRB',
    },
  };

  const block = document.createElement('div');
  block.className = 'mh-mode-block';
  block.innerHTML = `
    <label class="muted">mode — toggle BCR/TCR</label>
    <div class="mh-toggle">
      <label class="pill-input"><input type="radio" name="${groupName}" value="bcr" checked> BCR</label>
      <label class="pill-input"><input type="radio" name="${groupName}" value="tcr"> TCR</label>
    </div>
  `;
  select.insertAdjacentElement('beforebegin', block);

  const titleEl = card.querySelector('.uc-title');
  function setOptions(mode){
    select.innerHTML = modes[mode].values.map(v => `<option value="${v}">${v}</option>`).join('');
    select.value = modes[mode].values[0];
    if(titleEl) titleEl.textContent = modes[mode].title;
  }

  block.querySelectorAll(`input[name="${groupName}"]`).forEach(radio => {
    radio.addEventListener('change', () => {
      if(radio.checked){
        setOptions(radio.value);
      }
    });
  });

  setOptions('bcr');
}

function collectParams(card){
  const params = {};
  card.querySelectorAll('input,select,textarea').forEach(el => {
    if(!el.name) return;
    if(el.type === 'file') return;
    params[el.name] = (el.type === 'checkbox') ? (el.checked ? 'true' : 'false') : el.value;
  });
  return params;
}

async function renderUnits(){
  await ensureSession();
  let all = [];
  try {
    const res = await fetch(`/session/${SID}/units?group=sc`);
    all = await res.json();
  } catch (e) {
    try {
      const res2 = await fetch(`/session/${SID}/units`);
      all = await res2.json();
    } catch (e2) { all = []; }
  }
  // filter SC
  UNITS_META = (all || []).filter(u => (u.group && u.group==='sc') || (u.id||'').startsWith('sc_') || (u.label||'').toLowerCase().startsWith('sc:'));
  renderGroups(UNITS_META);
}

// Search
function applySearch(){
  const q = ($('#unit-search').value || '').trim().toLowerCase();
  const cards = Array.from($$('.unit-card'));
  const groups = Array.from($$('.unit-group'));
  // per-card show/hide
  cards.forEach(c => {
    const unitId = (c.dataset.unit||'').toLowerCase();
    const title = (c.querySelector('.uc-title')?.textContent||'').toLowerCase();
    const req = (c.querySelector('.req')?.textContent||'').toLowerCase();
    const hay = [unitId,title,req].join(' ');
    const hit = !q || hay.includes(q);
    c.style.display = hit ? '' : 'none';
  });
  // hide empty groups
  groups.forEach(g => {
    const hasAny = Array.from(g.querySelectorAll('.unit-card')).some(c => c.style.display !== 'none');
    g.style.display = hasAny ? '' : 'none';
    // auto open groups when searching
    if(q && hasAny) g.classList.add('open');
  });
}

// Expand / Collapse all
function expandAll(){ $$('.unit-group').forEach(g=>g.classList.add('open')); }
function collapseAll(){ $$('.unit-group').forEach(g=>g.classList.remove('open')); }

// ----- Flow builder -----
function addToFlow(card, unitId, label){
  const params = collectParams(card);
  FLOW.push({unitId, label, params});
  renderFlow();
}
function removeFromFlow(idx){
  FLOW.splice(idx,1);
  renderFlow();
}
function renderFlow(){
  const ul = $('#flow'); ul.innerHTML = '';
  if(FLOW.length === 0){
    ul.innerHTML = '<li class="muted">No steps yet. Use “Add to flow”.</li>';
  } else {
    FLOW.forEach((s,i)=>{
      const li = document.createElement('li');
      li.className = 'flow-item';
      li.innerHTML = `<div class="flow-step">${i+1}</div>
                      <div class="flow-label">${esc(s.label)} <span class="muted">(${esc(s.unitId)})</span></div>
                      <button class="flow-remove" title="Remove">✕</button>`;
      li.querySelector('.flow-remove').addEventListener('click', ()=>removeFromFlow(i));
      ul.appendChild(li);
    });
  }
  $('#validation').innerHTML = '—';
}

// ----- Validation & run -----
function validateFlow(){
  if(FLOW.length === 0){
    $('#validation').innerHTML = `<span class="pill err">Empty flow</span> Add steps with “Add to flow”.`;
    return {ok:false, msgs:['Empty flow']};
  }
  const msgs = [];
  let ok = true;

  const idxMerge = FLOW.findIndex(s=>s.unitId==='sc_merge_samples');
  if(idxMerge > 0){
    msgs.push('Suggestion: Place “SC: Merge samples” first for efficiency (optional).');
  }

  const idxMH = FLOW.findIndex(s=>s.unitId==='sc_remove_multi_heavy');
  const idxNH = FLOW.findIndex(s=>s.unitId==='sc_remove_no_heavy');
  if(idxMH !== -1 && idxNH !== -1 && idxMH > idxNH){
    msgs.push('Suggestion: Run “Remove multi heavy” before “Remove no heavy” (optional).');
  }

  const nonSC = FLOW.filter(s=>!s.unitId.startsWith('sc_'));
  if(nonSC.length){
    ok = false;
    msgs.push('Invalid step detected (non single-cell unit). Please remove it.');
  }

  const head = ok ? '<span class="pill ok">Looks good</span>' : '<span class="pill err">Problems found</span>';
  $('#validation').innerHTML = head + (msgs.length? ('<div class="mt8">'+msgs.map(esc).join('<br>')+'</div>') : '');
  return {ok, msgs};
}

async function runFlow(){
  if(running) return;
  const v = validateFlow();
  if(!v.ok){
    alert('Please fix flow issues and try again.');
    return;
  }
  running = true;
  $('#pstate').textContent = `starting (${FLOW.length} steps)…`;
  for(let i=0;i<FLOW.length;i++){
    const s = FLOW[i];
    $('#pstate').textContent = `running step ${i+1}/${FLOW.length}: ${s.label}`;
    const ok = await runUnit(s);
    if(!ok){
      $('#pstate').textContent = `failed at step ${i+1}: ${s.label}`;
      running = false;
      return;
    }
  }
  $('#pstate').textContent = 'finished ✓';
  running = false;
}

async function runUnit(step){
  await ensureSession();
  try{
    const r = await fetch(`/session/${SID}/run`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ unit_id: step.unitId, params: step.params })
    });
    const j = await r.json();
    if(!r.ok){
      const detail = (j.detail && (j.detail.error || j.detail)) || r.statusText;
      alert(`Error: ${detail}`);
      $('#log').textContent = (j.detail && j.detail.log_tail) ? j.detail.log_tail : '';
      return false;
    }
    await refreshState();
    const stepIdx = j.step.step_index;
    const lr = await fetch(`/session/${SID}/log/${stepIdx}`);
    $('#log').textContent = await lr.text();
    return true;
  }catch(e){
    alert('Network error running step: '+e);
    return false;
  }
}

// ----- State / artifacts -----
async function refreshState(){
  if(!SID) return;
  const r = await fetch(`/session/${SID}/state`);
  const s = await r.json();
  const chips = Object.entries(s.current||{}).map(([k,v]) => `<span class="pill">${esc(k)}: ${esc(v)}</span>`).join(' ');
  $('#statebox').innerHTML = chips || '<span class="muted">no state</span>';
  const arts = Object.values(s.artifacts||{}).map(a =>
    `<div>${esc(a.name)} — <a href="/session/${SID}/download/${encodeURIComponent(a.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = arts || '<span class="muted">none</span>';
}

// ----- Init -----
document.addEventListener('DOMContentLoaded', async () => {
  // wire buttons
  $('#upload-sc').addEventListener('click', uploadSCFiles);
  $('#validate').addEventListener('click', validateFlow);
  $('#runflow').addEventListener('click', runFlow);
  $('#clearflow').addEventListener('click', ()=>{ FLOW=[]; renderFlow(); $('#validation').textContent='—'; $('#pstate').textContent='idle'; });
  $('#unit-search').addEventListener('input', applySearch);
  $('#expAll').addEventListener('click', expandAll);
  $('#colAll').addEventListener('click', collapseAll);

  await ensureSession();
  await renderUnits();
  applySearch(); // initialize
});
