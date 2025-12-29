// Single-Cell page with grouped accordions, search, and compact cards
let SID = null;
let UNITS_META = [];
let FLOW = []; // [{unitId,label,params}]
let running = false;

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const formatUnitLabel = label => {
  const raw = (label ?? '').toString();
  const cleaned = raw.replace(/^sc:\s*/i, '').trimStart();
  return cleaned || raw.trim() || raw;
};

// ----- Session (auto) -----
async function ensureSession() {
  if (SID) {
    return SID;
  }
  const response = await fetch('/session/start', { method: 'POST' });
  const data = await response.json();
  SID = data.session_id;
  window.__SID__ = SID; // keep accessible, not visible
  await refreshState();
  return SID;
}

// ----- Upload -----
async function uploadSCFiles() {
  await ensureSession();
  const files = $('#sc-files').files;
  if (!files || !files.length) {
    alert('Choose at least one file');
    return;
  }
  $('#upload-msg').textContent = `Uploading ${files.length} file(s)…`;
  let successCount = 0;
  for (const file of files) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('name', file.name);
    const response = await fetch(`/session/${SID}/upload-aux`, {
      method: 'POST',
      body: formData
    });
    if (response.ok) {
      successCount++;
    }
  }
  $('#upload-msg').textContent = `Uploaded ${successCount}/${files.length} files.`;
  listUploaded(files);
  await refreshState();
}
function listUploaded(fileList) {
  const names = Array.from(fileList).map(file => esc(file.name));
  $('#uploaded-list').innerHTML = names.length ? 'Uploaded: ' + names.join(', ') : '';
}

// ----- Units rendering (grouped) -----
const GROUPS = [
  { id: 'merge', title: 'I/O & Merge', match: unit => (unit.id || '').includes('merge') },
  { id: 'qc', title: 'QC & Filtering', match: unit => /(filter|remove)/.test(unit.id || '') },
  { id: 'other', title: 'Other', match: unit => true }
];

function groupOf(unit) {
  for (const group of GROUPS) {
    if (group.match(unit)) {
      return group.id;
    }
  }
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
  const requiresText = (u.requires || []).join(' ');
  card.dataset.requires = requiresText.toLowerCase();
  const displayLabel = formatUnitLabel(u.label || u.id || '');
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
      <div class="uc-title">${esc(displayLabel)}</div>
      <button class="params-toggle" title="Show/Hide parameters">Parameters</button>
    </div>
    <div class="params">
      <div class="params-wrap">${paramsHTML || '<div class="muted">No parameters</div>'}</div>
      <div class="row mt8">
        <button class="run">Run</button>
        <button class="secondary addflow">Add to pipeline</button>
      </div>
    </div>`;

  // Behavior
  const pwrap = card.querySelector('.params-wrap');
  card.querySelector('.params-toggle').addEventListener('click', ()=>{
    pwrap.classList.toggle('open');
  });
  card.querySelector('.run').addEventListener('click', ()=>runSingle(card, u.id, displayLabel));
  card.querySelector('.addflow').addEventListener('click', ()=>addToFlow(card, u.id, displayLabel));

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
      title: 'Remove cells with multiple IgH',
    },
    tcr: {
      label: 'TCR',
      values: ['TRA','TRB','TRA,TRB'],
      title: 'Remove cells with multiple TRA/TRB',
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

async function renderUnits() {
  await ensureSession();
  let allUnits = [];
  try {
    const response = await fetch(`/session/${SID}/units?group=sc`);
    allUnits = await response.json();
  } catch (error) {
    try {
      const response2 = await fetch(`/session/${SID}/units`);
      allUnits = await response2.json();
    } catch (error2) {
      allUnits = [];
    }
  }
  // filter SC
  UNITS_META = (allUnits || []).filter(unit =>
    (unit.group && unit.group === 'sc') ||
    (unit.id || '').startsWith('sc_') ||
    (unit.label || '').toLowerCase().startsWith('sc:')
  );
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
    const req = (c.dataset.requires || '').toLowerCase();
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
function expandAll() {
  $$('.unit-group').forEach(group => group.classList.add('open'));
}

function collapseAll() {
  $$('.unit-group').forEach(group => group.classList.remove('open'));
}

// ----- Flow builder -----
function addToFlow(card, unitId, label) {
  const params = collectParams(card);
  FLOW.push({ unitId, label, params });
  renderFlow();
}

function removeFromFlow(index) {
  FLOW.splice(index, 1);
  renderFlow();
}
function renderFlow() {
  const flowList = $('#flow');
  flowList.innerHTML = '';
  if (FLOW.length === 0) {
    flowList.innerHTML = '<li class="muted">No steps yet. Use "Add to pipeline".</li>';
  } else {
    FLOW.forEach((step, index) => {
      const listItem = document.createElement('li');
      listItem.className = 'flow-item';
      listItem.innerHTML = `<div class="flow-step">${index + 1}</div>
                            <div class="flow-label">${esc(step.label)} <span class="muted">(${esc(step.unitId)})</span></div>
                            <button class="flow-remove" title="Remove">✕</button>`;
      listItem.querySelector('.flow-remove').addEventListener('click', () => removeFromFlow(index));
      flowList.appendChild(listItem);
    });
  }
  $('#validation').innerHTML = '—';
}

// ----- Validation & run -----
function validateFlow() {
  if (FLOW.length === 0) {
    $('#validation').innerHTML = `<span class="pill err">Empty flow</span> Add steps with "Add to pipeline".`;
    return { ok: false, msgs: ['Empty flow'] };
  }
  const messages = [];
  let isValid = true;

  const mergeIndex = FLOW.findIndex(step => step.unitId === 'sc_merge_samples');
  if (mergeIndex > 0) {
    messages.push('Suggestion: Place "Merge samples" first for efficiency (optional).');
  }

  const multiHeavyIndex = FLOW.findIndex(step => step.unitId === 'sc_remove_multi_heavy');
  const noHeavyIndex = FLOW.findIndex(step => step.unitId === 'sc_remove_no_heavy');
  if (multiHeavyIndex !== -1 && noHeavyIndex !== -1 && multiHeavyIndex > noHeavyIndex) {
    messages.push('Suggestion: Run "Remove multi heavy" before "Remove no heavy" (optional).');
  }

  const nonSC = FLOW.filter(step => !step.unitId.startsWith('sc_'));
  if (nonSC.length) {
    isValid = false;
    messages.push('Invalid step detected (non single-cell unit). Please remove it.');
  }

  const header = isValid ? '<span class="pill ok">Looks good</span>' : '<span class="pill err">Problems found</span>';
  $('#validation').innerHTML = header + (messages.length ?
    ('<div class="mt8">' + messages.map(esc).join('<br>') + '</div>') : '');
  return { ok: isValid, msgs: messages };
}

async function runFlow() {
  if (running) {
    return;
  }
  const validation = validateFlow();
  if (!validation.ok) {
    alert('Please fix flow issues and try again.');
    return;
  }
  running = true;
  $('#pstate').textContent = `starting (${FLOW.length} steps)…`;
  for (let index = 0; index < FLOW.length; index++) {
    const step = FLOW[index];
    $('#pstate').textContent = `running step ${index + 1}/${FLOW.length}: ${step.label}`;
    const success = await runUnit(step);
    if (!success) {
      $('#pstate').textContent = `failed at step ${index + 1}: ${step.label}`;
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
async function refreshState() {
  if (!SID) {
    return;
  }
  const response = await fetch(`/session/${SID}/state`);
  const state = await response.json();
  const chips = Object.entries(state.current || {}).map(([key, value]) =>
    `<span class="pill">${esc(key)}: ${esc(value)}</span>`
  ).join(' ');
  $('#statebox').innerHTML = chips || '<span class="muted">no state</span>';
  const artifacts = Object.values(state.artifacts || {}).map(artifact =>
    `<div>${esc(artifact.name)} — <a href="/session/${SID}/download/${encodeURIComponent(artifact.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = artifacts || '<span class="muted">none</span>';
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
