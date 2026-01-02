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
      let defaults = [];
      if (Array.isArray(v.default)) {
        defaults = v.default.map(val => (val ?? '').toString());
      } else if (typeof v.default === 'string') {
        if (v.default === '') {
          defaults = [''];
        } else {
          defaults = v.default
            .split(/[,\s]+/)
            .map(s => s.trim())
            .filter(s => s.length > 0)
            .map(s => s);
        }
      } else if (v.default !== undefined && v.default !== null) {
        defaults = [(v.default ?? '').toString()];
      }
      const opts = (v.options||[]).map(opt => {
        let val = opt;
        let lbl = opt;
        let disabled = false;
        let hidden = false;
        if (opt && typeof opt === 'object' && !Array.isArray(opt)) {
          val = opt.value ?? opt.label ?? '';
          lbl = opt.label ?? opt.value ?? '';
          disabled = !!opt.disabled;
          hidden = !!opt.hidden;
        }
        const valStr = (val ?? '').toString();
        const lblStr = (lbl ?? '').toString();
        const selected = defaults.includes(valStr);
        const disabledAttr = disabled ? ' disabled' : '';
        const hiddenAttr = hidden ? ' hidden' : '';
        return `<option value="${esc(valStr)}"${selected ? ' selected' : ''}${disabledAttr}${hiddenAttr}>${esc(lblStr)}</option>`;
      }).join('');
      const multiAttr = v.multiple ? ' multiple' : '';
      paramsHTML += `${label}<select name="${esc(k)}"${multiAttr}>${opts}</select>`;
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

  return card;
}


function collectParams(card){
  const params = {};
  card.querySelectorAll('input,select,textarea').forEach(el => {
    if(!el.name) return;
    if(el.type === 'file') return;
    if(el.tagName === 'SELECT' && el.multiple){
      const values = Array.from(el.selectedOptions).map(opt => opt.value).filter(v => v !== undefined && v !== null && v !== '');
      params[el.name] = values.join(', ');
    } else if(el.type === 'checkbox'){
      params[el.name] = el.checked ? 'true' : 'false';
    } else {
      params[el.name] = el.value;
    }
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
                            <div class="flow-label">${esc(step.label)}</div>
                            <button class="flow-remove" title="Remove">✕</button>`;
      listItem.querySelector('.flow-remove').addEventListener('click', () => removeFromFlow(index));
      flowList.appendChild(listItem);
    });
  }
  $('#validation').innerHTML = '—';
}

// ----- Flow progress -----
function setPipelineProgress(percent, hint) {
  const bar = $('#pstate-bar');
  const progress = $('#pstate-progress');
  const msg = $('#pstate-progress-msg');
  const numeric = Number(percent);
  const clamped = Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : 0;
  if (bar) {
    bar.style.width = clamped + '%';
  }
  if (progress) {
    progress.setAttribute('aria-valuenow', clamped.toFixed(0));
  }
  if (hint !== undefined && msg) {
    msg.textContent = hint;
  }
}
function resetPipelineProgress() {
  setPipelineProgress(0, 'Waiting to run');
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
  const totalSteps = FLOW.length;
  const stepsLabel = `step${totalSteps === 1 ? '' : 's'}`;
  const startMsg = `starting (${totalSteps} ${stepsLabel})...`;
  $('#pstate').textContent = startMsg;
  setPipelineProgress(0, `Starting ${totalSteps} ${stepsLabel}`);
  for (let index = 0; index < totalSteps; index++) {
    const step = FLOW[index];
    const runningMsg = `running step ${index + 1}/${totalSteps}: ${step.label}`;
    $('#pstate').textContent = runningMsg;
    setPipelineProgress((index / totalSteps) * 100, runningMsg);
    const success = await runUnit(step);
    if (!success) {
      const failMsg = `failed at step ${index + 1}: ${step.label}`;
      $('#pstate').textContent = failMsg;
      setPipelineProgress((index / totalSteps) * 100, `Failed at step ${index + 1}/${totalSteps}`);
      running = false;
      return;
    }
    setPipelineProgress(((index + 1) / totalSteps) * 100, `Completed ${index + 1}/${totalSteps}`);
  }
  $('#pstate').textContent = 'finished ✓';
  setPipelineProgress(100, 'Pipeline complete');
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
  $('#clearflow').addEventListener('click', ()=>{ FLOW=[]; renderFlow(); $('#validation').textContent='—'; $('#pstate').textContent='idle'; resetPipelineProgress(); });
  $('#unit-search').addEventListener('input', applySearch);
  $('#expAll').addEventListener('click', expandAll);
  $('#colAll').addEventListener('click', collapseAll);

  resetPipelineProgress();
  await ensureSession();
  await renderUnits();
  applySearch(); // initialize
});
