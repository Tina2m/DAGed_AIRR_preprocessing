// Single-Cell page with grouped accordions, search, and compact cards
let SID = null;
let UNITS_META = [];
let FLOW = []; // [{unitId,label,params}]
let running = false;
let HISTORY_SELECTED = null;
let QC_OBJECT_URLS = [];
let LAST_STATE = null;
let STOP_REQUESTED = false;
let LAST_RUN_WAS_CANCELLED = false;
let FLOW_RUN_MODE = 'continue';
let SUPPRESS_NEXT_FLOW_RUN = false;

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const formatUnitLabel = label => {
  const raw = (label ?? '').toString();
  const cleaned = raw.replace(/^sc:\s*/i, '').trimStart();
  return cleaned || raw.trim() || raw;
};
const apiFetch = (window.Auth && window.Auth.apiFetch) ? window.Auth.apiFetch : fetch;

function formatTimestamp(iso) {
  if (!iso) {
    return 'unknown';
  }
  try {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) {
      return iso;
    }
    return date.toLocaleString();
  } catch (err) {
    return iso;
  }
}

function formatParamValue(value) {
  if (Array.isArray(value)) {
    return value.map(item => formatParamValue(item)).filter(item => item !== '').join(', ');
  }
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (err) {
      return String(value);
    }
  }
  return String(value);
}

function collectParamEntries(params) {
  if (!params || typeof params !== 'object') {
    return [];
  }
  return Object.entries(params).reduce((acc, [key, value]) => {
    const formatted = formatParamValue(value);
    if (formatted !== '') {
      acc.push({ key, value: formatted });
    }
    return acc;
  }, []);
}

function formatParamTitle(entries) {
  if (!entries.length) {
    return 'No parameters.';
  }
  return entries.map(entry => `${entry.key}: ${entry.value}`).join(' | ');
}

function cssEscape(value) {
  if (window.CSS && typeof window.CSS.escape === 'function') {
    return window.CSS.escape(value);
  }
  return value.replace(/[^a-zA-Z0-9_-]/g, '\\$&');
}

function applyParamValueToField(field, value) {
  if (!field) {
    return;
  }
  if (field.type === 'checkbox') {
    field.checked = value === true || value === 'true' || value === 1 || value === '1';
    return;
  }
  if (field.tagName === 'SELECT' && field.multiple) {
    const values = Array.isArray(value)
      ? value.map(item => String(item))
      : String(value ?? '')
        .split(',')
        .map(item => item.trim())
        .filter(item => item.length > 0);
    Array.from(field.options).forEach(opt => {
      opt.selected = values.includes(opt.value);
    });
    return;
  }
  field.value = value === null || value === undefined ? '' : String(value);
}

function applyParamsToCard(card, params) {
  if (!card || !params || typeof params !== 'object') {
    return;
  }
  Object.entries(params).forEach(([key, value]) => {
    const selector = `[name="${cssEscape(key)}"]`;
    const field = card.querySelector(selector);
    if (!field) {
      return;
    }
    applyParamValueToField(field, value);
  });
}

function getUnitLabel(unitId) {
  const meta = UNITS_META.find(unit => unit.id === unitId);
  if (meta) {
    return formatUnitLabel(meta.label || meta.id || unitId);
  }
  return unitId;
}

const SC_FILTER_RE = /(filter|remove|productive)/i;
const FUNNEL_TONES = ["tone-1", "tone-2", "tone-3", "tone-4", "tone-5", "tone-6"];
let FUNNEL_REQUEST_ID = 0;
const SC_QC_LABELS = {
  sc_filter_productive: 'Productive vs non-productive',
  sc_remove_multi_heavy: 'Multi-heavy filtering'
};
const SC_QC_STAGE_LABELS = {
  ratio: 'counts'
};

function isFilteringUnit(unitId) {
  if (!unitId) {
    return false;
  }
  return SC_FILTER_RE.test(unitId);
}

function formatCount(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '0';
  }
  return numeric.toLocaleString('en-US');
}

function extractFirstNumber(text) {
  if (!text) {
    return null;
  }
  const match = String(text).match(/(\d[\d,]*)/);
  if (!match) {
    return null;
  }
  const raw = match[1].replace(/,/g, '');
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

function parseLogCounts(logText) {
  const counts = {};
  if (!logText) {
    return { pass: null, total: null, counts };
  }
  const lines = String(logText).split(/\r?\n/);
  lines.forEach(line => {
    const rowsMatch = line.match(/^\s*Wrote\s+.+\s+rows:\s*([\d,]+)/i);
    if (rowsMatch) {
      const value = extractFirstNumber(rowsMatch[1]);
      if (value !== null) {
        counts.PASS = value;
      }
      return;
    }
    const match = line.match(/^\s*([A-Z][A-Z0-9_-]*)>\s*(.+)$/);
    if (!match) {
      return;
    }
    const key = match[1].toUpperCase();
    const value = extractFirstNumber(match[2]);
    if (value !== null) {
      counts[key] = value;
    }
  });
  const pass = counts.PASS ?? counts.PASSED ?? null;
  const fail = counts.FAIL ?? counts.FAILED ?? null;
  let total = counts.SEQUENCES ?? counts.INPUT ?? counts.READS ?? counts.TOTAL ?? null;
  if (total === null && pass !== null && fail !== null) {
    total = pass + fail;
  }
  return { pass, total, counts };
}

async function fetchStepLog(stepIndex) {
  if (!Number.isFinite(stepIndex)) {
    return '';
  }
  try {
    const response = await apiFetch(`/session/${SID}/log/${stepIndex}`);
    if (!response.ok) {
      return '';
    }
    return await response.text();
  } catch (err) {
    return '';
  }
}

function renderFilteringFunnel(mount, rows) {
  mount.innerHTML = '';
  rows.forEach(row => {
    const rowEl = document.createElement('div');
    rowEl.className = `funnel-row${row.isTotal ? ' is-total' : ''}`;

    const label = document.createElement('div');
    label.className = 'funnel-label';
    label.textContent = row.label;

    const track = document.createElement('div');
    track.className = 'funnel-track';

    const bar = document.createElement('div');
    bar.className = `funnel-bar ${row.tone || ''}`.trim();
    bar.style.setProperty('--funnel-width', `${row.width}%`);
    bar.title = `${row.label}: ${formatCount(row.count)} (${row.percentLabel})`;

    const value = document.createElement('span');
    value.className = 'funnel-value';
    value.textContent = formatCount(row.count);

    const percent = document.createElement('span');
    percent.className = 'funnel-percent';
    percent.textContent = row.percentLabel;

    bar.appendChild(value);
    bar.appendChild(percent);
    track.appendChild(bar);
    rowEl.appendChild(label);
    rowEl.appendChild(track);
    mount.appendChild(rowEl);
  });
}

function updateFilteringFunnel(state) {
  const mount = $('#filter-funnel');
  if (!mount) {
    return;
  }
  const steps = Array.isArray(state?.steps) ? state.steps : [];
  const filterSteps = steps.filter(step => isFilteringUnit(step?.unit || ''));
  if (!filterSteps.length) {
    mount.innerHTML = '<div class="muted">No filtering steps run yet.</div>';
    return;
  }
  const requestId = ++FUNNEL_REQUEST_ID;
  mount.innerHTML = '<div class="muted">Loading read stats...</div>';

  (async () => {
    const results = await Promise.all(filterSteps.map(async (step) => {
      const logText = await fetchStepLog(step.step_index);
      return { step, counts: parseLogCounts(logText) };
    }));
    if (requestId !== FUNNEL_REQUEST_ID) {
      return;
    }
    let baseline = null;
    for (const result of results) {
      if (result.counts.total && result.counts.total > 0) {
        baseline = result.counts.total;
        break;
      }
    }
    if (!baseline) {
      const firstPass = results.find(result => result.counts.pass && result.counts.pass > 0);
      baseline = firstPass ? firstPass.counts.pass : 0;
    }
    if (!baseline) {
      mount.innerHTML = '<div class="muted">No pass counts found in logs yet.</div>';
      return;
    }
    const rows = [];
    rows.push({
      label: 'Total reads',
      count: baseline,
      percentLabel: '100%',
      width: 100,
      tone: FUNNEL_TONES[0],
      isTotal: true
    });
    let toneIndex = 1;
    results.forEach(result => {
      const pass = result.counts.pass;
      if (pass === null) {
        return;
      }
      const pct = baseline ? (pass / baseline) * 100 : 0;
      const rounded = Math.round(pct);
      const percentLabel = (rounded === 0 && pass > 0) ? '<1%' : `${rounded}%`;
      const width = Math.max(2, Math.min(100, pct));
      rows.push({
        label: getUnitLabel(result.step.unit || ''),
        count: pass,
        percentLabel,
        width,
        tone: FUNNEL_TONES[toneIndex % FUNNEL_TONES.length]
      });
      toneIndex += 1;
    });
    if (rows.length <= 1) {
      mount.innerHTML = '<div class="muted">No pass counts found in logs yet.</div>';
      return;
    }
    renderFilteringFunnel(mount, rows);
  })().catch(() => {
    mount.innerHTML = '<div class="muted">Unable to load read stats.</div>';
  });
}

function clearQcPlotUrls() {
  QC_OBJECT_URLS.forEach(url => URL.revokeObjectURL(url));
  QC_OBJECT_URLS = [];
}

function isPlotArtifact(artifact) {
  if (!artifact) {
    return false;
  }
  if (artifact.kind === 'plot') {
    return true;
  }
  const path = String(artifact.path || '').toLowerCase();
  return path.endsWith('.svg') && String(artifact.name || '').startsWith('plot_');
}

function parseScPlotInfo(artifact) {
  const name = String(artifact?.name || '');
  if (!name.startsWith('plot_')) {
    return null;
  }
  const parts = name.split('_');
  if (parts.length < 4) {
    return null;
  }
  const stepIndex = Number(parts[1]);
  let stage = parts[parts.length - 1];
  let unitParts = parts.slice(2, -1);
  if (parts.length >= 5 && parts[parts.length - 2] === 'by' && parts[parts.length - 1] === 'sample') {
    stage = 'by_sample';
    unitParts = parts.slice(2, -2);
  }
  return {
    stepIndex: Number.isFinite(stepIndex) ? stepIndex : null,
    unitId: unitParts.join('_'),
    stage
  };
}

async function loadPlotImage(img, sid, artName) {
  try {
    const response = await apiFetch(`/session/${sid}/download/${encodeURIComponent(artName)}`);
    if (!response.ok) {
      img.alt = 'QC plot unavailable';
      return;
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    img.src = url;
    img.alt = artName;
    QC_OBJECT_URLS.push(url);
  } catch (err) {
    img.alt = 'QC plot unavailable';
  }
}

function renderQcPlots(state) {
  const mount = $('#qc-plots');
  if (!mount) {
    return;
  }
  clearQcPlotUrls();
  const artifacts = Object.values(state?.artifacts || {}).filter(isPlotArtifact);
  if (!artifacts.length) {
    mount.innerHTML = '<div class="muted">No QC plots yet.</div>';
    return;
  }
  const stepsByIndex = new Map();
  (state.steps || []).forEach(step => stepsByIndex.set(step.step_index, step));
  const groups = new Map();
  artifacts.forEach(art => {
    const info = parseScPlotInfo(art);
    if (!info || info.stepIndex === null) {
      return;
    }
    const key = `${info.stepIndex}:${info.unitId}`;
    if (!groups.has(key)) {
      groups.set(key, { stepIndex: info.stepIndex, unitId: info.unitId, plots: [] });
    }
    groups.get(key).plots.push({ art, stage: info.stage });
  });
  const ordered = Array.from(groups.values()).sort((a, b) => a.stepIndex - b.stepIndex);
  mount.innerHTML = '';
  ordered.forEach(group => {
    const step = stepsByIndex.get(group.stepIndex) || {};
    const unitId = group.unitId || step.unit || '';
    const label = SC_QC_LABELS[unitId] || getUnitLabel(unitId) || 'QC plot';
    const wrapper = document.createElement('div');
    wrapper.className = 'qc-group';
    const title = document.createElement('div');
    title.className = 'qc-title';
    title.textContent = `Step ${group.stepIndex + 1}: ${label}`;
    wrapper.appendChild(title);
    const row = document.createElement('div');
    row.className = 'qc-row';
    group.plots.forEach(plot => {
      const card = document.createElement('div');
      card.className = 'qc-card';
      const img = document.createElement('img');
      img.alt = `${label} ${plot.stage || ''}`.trim();
      card.appendChild(img);
      const cap = document.createElement('div');
      cap.className = 'qc-caption';
      cap.textContent = SC_QC_STAGE_LABELS[plot.stage] || plot.stage || 'plot';
      card.appendChild(cap);
      row.appendChild(card);
      loadPlotImage(img, SID, plot.art.name);
    });
    wrapper.appendChild(row);
    mount.appendChild(wrapper);
  });
}

// ----- Session (auto) -----
async function ensureSession() {
  if (SID) {
    return SID;
  }
  const response = await apiFetch('/session/start', { method: 'POST' });
  const data = await response.json();
  SID = data.session_id;
  window.__SID__ = SID; // keep accessible, not visible
  await refreshState();
  return SID;
}

async function startNewSessionFromClear() {
  const prevSid = SID;
  SID = null;
  window.__SID__ = null;
  try {
    await ensureSession();
  } catch (err) {
    SID = prevSid;
    window.__SID__ = prevSid;
    alert('Unable to start a new session.');
    return;
  }
  FLOW = [];
  renderFlow();
  $('#validation').textContent = '—';
  $('#pstate').textContent = 'idle';
  resetPipelineProgress();
  const uploadMsg = $('#upload-msg');
  if (uploadMsg) {
    uploadMsg.textContent = '';
  }
  const uploadedList = $('#uploaded-list');
  if (uploadedList) {
    uploadedList.innerHTML = '';
  }
  const fileInput = $('#sc-files');
  if (fileInput) {
    fileInput.value = '';
  }
  await loadHistory();
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
    const response = await apiFetch(`/session/${SID}/upload-aux`, {
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
  await loadHistory();
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

async function runSingle(card, unitId, label) {
  if (!card || !unitId) {
    return false;
  }
  const button = card.querySelector('.run');
  const priorText = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Running...';
  }
  const params = collectParams(card);
  const normalizedFiles = resolveFilesParamFromArtifacts(params.files, LAST_STATE);
  if (normalizedFiles) {
    params.files = normalizedFiles;
  }
  if (!params.files || !String(params.files).trim()) {
    const scTableFile = resolveCurrentScTableFile(LAST_STATE);
    if (scTableFile) {
      params.files = scTableFile;
    }
  }
  if (!params.files || !String(params.files).trim()) {
    const originInputs = getOriginScInputs(LAST_STATE);
    if (originInputs.length) {
      params.files = originInputs.join(', ');
    }
  }
  try {
    const ok = await runUnit({ unitId, label, params });
    return ok;
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = priorText || 'Run';
    }
  }
}

async function renderUnits() {
  await ensureSession();
  let allUnits = [];
  try {
    const response = await apiFetch(`/session/${SID}/units?group=sc`);
    allUnits = await response.json();
  } catch (error) {
    try {
      const response2 = await apiFetch(`/session/${SID}/units`);
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
      const entries = collectParamEntries(step.params);
      listItem.title = formatParamTitle(entries);
      listItem.tabIndex = 0;
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

function setStopFlowEnabled(enabled) {
  const button = $('#stopflow');
  if (!button) {
    return;
  }
  button.disabled = !enabled;
}

async function requestStopCurrentFlow() {
  STOP_REQUESTED = true;
  if (!SID) {
    return;
  }
  $('#pstate').textContent = 'stopping...';
  setPipelineProgress(null, 'Stopping current run...');
  try {
    await apiFetch(`/session/${SID}/cancel`, { method: 'POST' });
  } catch (err) {
    // Keep local stop intent even if API request fails.
  }
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

function getFlowRunMode() {
  return FLOW_RUN_MODE;
}

function setFlowRunMode(mode) {
  FLOW_RUN_MODE = mode === 'restart' ? 'restart' : 'continue';
  $$('#runflow-menu .run-item').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.runMode === FLOW_RUN_MODE);
  });
  setPipelineProgress(
    null,
    FLOW_RUN_MODE === 'restart' ? 'Run mode: from beginning' : 'Run mode: continue from changes'
  );
}

function closeFlowRunMenu() {
  const menu = $('#runflow-menu');
  const toggle = $('#runflow-options');
  const split = $('#runflow-split');
  if (menu) {
    menu.classList.remove('open');
  }
  if (toggle) {
    toggle.setAttribute('aria-expanded', 'false');
  }
  if (split) {
    split.classList.remove('open');
  }
}

function wireFlowRunMenu() {
  const split = $('#runflow-split');
  const toggle = $('#runflow-options');
  const menu = $('#runflow-menu');
  if (!split || !toggle || !menu) {
    return;
  }

  toggle.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    const nextOpen = !menu.classList.contains('open');
    if (nextOpen) {
      menu.classList.add('open');
      toggle.setAttribute('aria-expanded', 'true');
      split.classList.add('open');
    } else {
      closeFlowRunMenu();
    }
  });

  menu.querySelectorAll('.run-item').forEach(btn => {
    btn.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const selectedMode = btn.dataset.runMode || 'continue';
      setFlowRunMode(selectedMode);
      closeFlowRunMenu();
      if (running) {
        return;
      }
      SUPPRESS_NEXT_FLOW_RUN = true;
      setTimeout(() => { SUPPRESS_NEXT_FLOW_RUN = false; }, 0);
      await runFlow(selectedMode);
    });
  });

  document.addEventListener('click', (event) => {
    if (!split.contains(event.target)) {
      closeFlowRunMenu();
    }
  });

  setFlowRunMode(FLOW_RUN_MODE);
}

function stableParamString(value) {
  if (Array.isArray(value)) {
    return '[' + value.map(item => stableParamString(item)).join(',') + ']';
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return '{' + keys.map(key => `${JSON.stringify(key)}:${stableParamString(value[key])}`).join(',') + '}';
  }
  return JSON.stringify(value);
}

function canonicalFilesForCompare(filesValue) {
  const resolved = resolveFilesParamFromArtifacts(filesValue, LAST_STATE);
  const raw = String(resolved || '').trim();
  if (!raw) {
    return '';
  }
  return raw.split(/[,\s]+/).map(s => s.trim()).filter(Boolean).join(', ');
}

function areFlowStepsEquivalent(plannedStep, completedStep) {
  const plannedUnit = (plannedStep && plannedStep.unitId) || '';
  const completedUnit = (completedStep && completedStep.unit) || '';
  if (plannedUnit !== completedUnit) {
    return false;
  }

  const plannedParams = { ...((plannedStep && plannedStep.params) || {}) };
  const completedParams = { ...((completedStep && completedStep.params) || {}) };
  const plannedFiles = canonicalFilesForCompare(plannedParams.files);
  const completedFiles = canonicalFilesForCompare(completedParams.files);

  // If user left files empty in the flow card, treat it as "use previous/default input"
  // so appended-step continues do not get flagged as middle-step edits.
  if (!plannedFiles) {
    delete plannedParams.files;
    delete completedParams.files;
  } else {
    plannedParams.files = plannedFiles;
    completedParams.files = completedFiles;
  }

  if (stableParamString(plannedParams) === stableParamString(completedParams)) {
    return true;
  }

  // Be permissive for SC continue-mode: if unit order matches, allow resume even
  // when params differ only by UI/default serialization quirks.
  return true;
}

function getCompletedScSteps() {
  const all = Array.isArray(LAST_STATE?.steps) ? LAST_STATE.steps : [];
  return all.filter(step => ((step && step.unit) || '').startsWith('sc_'));
}

function buildScRunPlan(flowSteps, modeOverride) {
  const mode = modeOverride === 'restart' || modeOverride === 'continue'
    ? modeOverride
    : getFlowRunMode();
  if (mode === 'restart') {
    return { startIndex: 0, note: 'Run mode: from beginning.' };
  }

  const completed = getCompletedScSteps();
  const sharedLength = Math.min(flowSteps.length, completed.length);
  let prefix = 0;
  while (prefix < sharedLength && areFlowStepsEquivalent(flowSteps[prefix], completed[prefix])) {
    prefix += 1;
  }

  if (prefix < completed.length && prefix < flowSteps.length) {
    return {
      error: 'Continue from changes currently supports appending new steps only. Use "Run from beginning" for edited middle steps.'
    };
  }

  if (prefix >= flowSteps.length) {
    return { startIndex: prefix, noWork: true, note: 'No new or changed steps to run.' };
  }

  if (prefix > 0) {
    const remaining = flowSteps.length - prefix;
    return {
      startIndex: prefix,
      note: `Reusing ${prefix} completed step${prefix === 1 ? '' : 's'}; running ${remaining}.`
    };
  }
  return { startIndex: 0 };
}

async function runFlow(modeOverride) {
  if (running) {
    return;
  }
  const validation = validateFlow();
  if (!validation.ok) {
    alert('Please fix flow issues and try again.');
    return;
  }
  const runPlan = buildScRunPlan(FLOW, modeOverride);
  if (runPlan.error) {
    $('#pstate').textContent = 'cannot continue from changed middle steps';
    setPipelineProgress(0, runPlan.error);
    alert(runPlan.error);
    return;
  }
  const totalSteps = FLOW.length;
  const stepsLabel = `step${totalSteps === 1 ? '' : 's'}`;
  if (runPlan.note) {
    setPipelineProgress((runPlan.startIndex / totalSteps) * 100, runPlan.note);
  }
  if (runPlan.noWork) {
    $('#pstate').textContent = 'up to date';
    setPipelineProgress(100, 'No new or changed steps to run');
    return;
  }
  running = true;
  STOP_REQUESTED = false;
  LAST_RUN_WAS_CANCELLED = false;
  setStopFlowEnabled(true);
  const startMsg = runPlan.startIndex > 0
    ? `continuing at step ${runPlan.startIndex + 1}/${totalSteps}...`
    : `starting (${totalSteps} ${stepsLabel})...`;
  $('#pstate').textContent = startMsg;
  setPipelineProgress(
    (runPlan.startIndex / totalSteps) * 100,
    runPlan.startIndex > 0 ? `Continuing from step ${runPlan.startIndex + 1}` : `Starting ${totalSteps} ${stepsLabel}`
  );
  try {
    for (let index = runPlan.startIndex; index < totalSteps; index++) {
      if (STOP_REQUESTED) {
        $('#pstate').textContent = `stopped at ${index}/${totalSteps}`;
        setPipelineProgress((index / totalSteps) * 100, `Stopped at step ${index}/${totalSteps}`);
        return;
      }
      const step = FLOW[index];
      const params = { ...(step.params || {}) };
      const normalizedFiles = resolveFilesParamFromArtifacts(params.files, LAST_STATE);
      if (normalizedFiles) {
        params.files = normalizedFiles;
      }
      if (!params.files || !String(params.files).trim()) {
        const scTableFile = resolveCurrentScTableFile(LAST_STATE);
        if (scTableFile) {
          params.files = scTableFile;
        }
      }
      if (!params.files || !String(params.files).trim()) {
        const originInputs = getOriginScInputs(LAST_STATE);
        if (originInputs.length) {
          params.files = originInputs.join(', ');
        }
      }
      const runningMsg = `running step ${index + 1}/${totalSteps}: ${step.label}`;
      $('#pstate').textContent = runningMsg;
      setPipelineProgress((index / totalSteps) * 100, runningMsg);
      const success = await runUnit({ ...step, params }, { skipHistory: true });
      if (!success) {
        if (STOP_REQUESTED || LAST_RUN_WAS_CANCELLED) {
          $('#pstate').textContent = `stopped at step ${index + 1}: ${step.label}`;
          setPipelineProgress((index / totalSteps) * 100, `Stopped at step ${index + 1}/${totalSteps}`);
          return;
        }
        const failMsg = `failed at step ${index + 1}: ${step.label}`;
        $('#pstate').textContent = failMsg;
        setPipelineProgress((index / totalSteps) * 100, `Failed at step ${index + 1}/${totalSteps}`);
        return;
      }
      setPipelineProgress(((index + 1) / totalSteps) * 100, `Completed ${index + 1}/${totalSteps}`);
    }
    $('#pstate').textContent = 'finished ✓';
    setPipelineProgress(100, 'Pipeline complete');
  } finally {
    await loadHistory();
    running = false;
    setStopFlowEnabled(false);
    STOP_REQUESTED = false;
  }
}

async function runUnit(step, opts = {}){
  await ensureSession();
  try{
    const r = await apiFetch(`/session/${SID}/run`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ unit_id: step.unitId, params: step.params })
    });
    const j = await r.json();
    if(!r.ok){
      const detail = (j.detail && (j.detail.error || j.detail)) || r.statusText;
      const cancelled = /cancelled by user/i.test(String(detail));
      LAST_RUN_WAS_CANCELLED = cancelled;
      if (!cancelled) {
        alert(`Error: ${detail}`);
      }
      $('#log').textContent = (j.detail && j.detail.log_tail) ? j.detail.log_tail : '';
      return false;
    }
    await refreshState();
    const stepIdx = j.step.step_index;
    const lr = await apiFetch(`/session/${SID}/log/${stepIdx}`);
    $('#log').textContent = await lr.text();
    if (!opts.skipHistory) {
      await loadHistory();
    }
    return true;
  }catch(e){
    alert('Network error running step: '+e);
    return false;
  }
}

function wireDownloadLinks() {
  document.querySelectorAll('.art-download').forEach(link => {
    if (link.dataset.wired) {
      return;
    }
    link.dataset.wired = '1';
    link.addEventListener('click', async (event) => {
      event.preventDefault();
      const name = link.dataset.art;
      if (!name) {
        return;
      }
      try {
        const response = await apiFetch(`/session/${SID}/download/${encodeURIComponent(name)}`);
        if (!response.ok) {
          throw new Error('Download failed');
        }
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = name;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      } catch (err) {
        alert('Download failed. Please try again.');
      }
    });
  });
}

// ----- State / artifacts -----
function applyStateSnapshot(state) {
  LAST_STATE = state || null;
  const chips = Object.entries(state.current || {}).map(([key, value]) =>
    `<span class="pill">${esc(key)}: ${esc(value)}</span>`
  ).join(' ');
  $('#statebox').innerHTML = chips || '<span class="muted">no state</span>';
  const artifacts = Object.values(state.artifacts || {}).map(artifact =>
    `<div>${esc(artifact.name)} - <a href="#" class="art-download" data-art="${esc(artifact.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = artifacts || '<span class="muted">none</span>';
  wireDownloadLinks();
  updateFilteringFunnel(state);
  renderQcPlots(state);
}

function getOriginScInputs(state) {
  if (!state) {
    return [];
  }
  const aux = Array.isArray(state.aux_files) ? state.aux_files : [];
  const auxInputs = aux.filter(name => {
    const lower = String(name || '').toLowerCase();
    return lower.endsWith('.tsv') || lower.endsWith('.tsv.gz');
  });
  if (auxInputs.length) {
    return auxInputs;
  }
  const artifacts = Object.values(state.artifacts || {});
  return artifacts
    .filter(art => art && art.from_step === -1)
    .map(art => art.path || art.name)
    .filter(name => {
      const lower = String(name || '').toLowerCase();
      return lower.endsWith('.tsv') || lower.endsWith('.tsv.gz');
    });
}

function resolveFilesParamFromArtifacts(filesValue, state) {
  const raw = String(filesValue || '').trim();
  if (!raw) {
    return '';
  }
  const artifacts = state?.artifacts || {};
  const names = raw.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
  if (!names.length) {
    return '';
  }
  const resolved = names.map(name => {
    const art = artifacts[name];
    return art?.path || name;
  });
  return resolved.join(', ');
}

function resolveCurrentScTableFile(state) {
  const key = state?.current?.SC_TABLE;
  if (!key) {
    return '';
  }
  const art = state?.artifacts?.[key];
  return art?.path || key;
}

async function refreshState() {
  if (!SID) {
    return;
  }
  const response = await apiFetch(`/session/${SID}/state`);
  const state = await response.json();
  applyStateSnapshot(state);
}

function collectUploadedItems(state) {
  const items = [];
  const artifacts = Object.values(state.artifacts || {});
  artifacts.filter(art => art.from_step === -1).forEach(art => {
    items.push({
      label: art.path || art.name,
      sub: art.name,
      download: art.name
    });
  });
  (state.aux_files || []).forEach(name => {
    items.push({ label: name, sub: 'aux file' });
  });
  return items;
}

function collectOutputItems(state) {
  const items = [];
  Object.values(state.artifacts || {}).forEach(art => {
    if (typeof art.from_step === 'number' && art.from_step >= 0) {
      const label = art.path || art.name;
      const sub = (art.path && art.path !== art.name) ? art.name : '';
      items.push({ label, sub, download: art.name });
    }
  });
  return items;
}

function renderArtifactItems(items, emptyText, sid) {
  if (!items.length) {
    return `<div class="muted">${esc(emptyText)}</div>`;
  }
  return items.map(item => {
    const sub = item.sub ? `<div class="artifact-sub">${esc(item.sub)}</div>` : '';
    const download = item.download
      ? `<a href="#" class="history-download" data-sid="${esc(sid)}" data-art="${esc(item.download)}">download</a>`
      : '';
    return `<div class="artifact-item"><div><div class="artifact-label">${esc(item.label)}</div>${sub}</div>${download}</div>`;
  }).join('');
}

function renderPipelineSteps(steps) {
  if (!steps.length) {
    return '<div class="muted">No steps run yet.</div>';
  }
  const renderParamTooltip = (entries) => {
    if (!entries.length) {
      return '<div class="pipeline-param-empty muted">No parameters.</div>';
    }
    return entries.map(entry =>
      `<div class="pipeline-param"><span class="param-key">${esc(entry.key)}</span><span class="param-val">${esc(entry.value)}</span></div>`
    ).join('');
  };
  return steps.map((step, index) => {
    const unitId = step.unit || '';
    const label = getUnitLabel(unitId);
    const entries = collectParamEntries(step.params);
    const tooltipHtml = renderParamTooltip(entries);
    const titleText = entries.length
      ? entries.map(entry => `${entry.key}: ${entry.value}`).join(' | ')
      : 'No parameters.';
    return `
      <div class="pipeline-step" tabindex="0" title="${esc(titleText)}">
        <div class="pipeline-index">${index + 1}</div>
        <div class="pipeline-info">
          <div class="pipeline-label">${esc(label)}</div>
          <div class="pipeline-unit">${esc(unitId)}</div>
        </div>
        <div class="pipeline-param-tooltip">${tooltipHtml}</div>
      </div>`;
  }).join('');
}

function syncFlowFromSteps(steps) {
  FLOW = [];
  (steps || []).forEach(step => {
    const unitId = step.unit || '';
    const label = getUnitLabel(unitId);
    const card = document.querySelector(`.unit-card[data-unit="${cssEscape(unitId)}"]`);
    if (card) {
      applyParamsToCard(card, step.params);
    }
    FLOW.push({
      unitId,
      label,
      params: step.params || {}
    });
  });
  renderFlow();
}

function setHistoryActive(sid) {
  HISTORY_SELECTED = sid;
  $$('#history-list .run-history-item').forEach(item => {
    item.classList.toggle('active', item.dataset.sid === sid);
  });
}

function renderHistoryDetails(state, sid) {
  const details = $('#history-details');
  if (!details) {
    return;
  }
  const header = `
    <div class="run-stats-meta">
      <strong>${esc(sid)}</strong>
      <span>${esc(formatTimestamp(state.updated_at || state.created_at))}</span>
    </div>`;
  const uploaded = renderArtifactItems(collectUploadedItems(state), 'No uploaded files recorded.', sid);
  const outputs = renderArtifactItems(collectOutputItems(state), 'No output files yet.', sid);
  const pipeline = renderPipelineSteps(state.steps || []);
  details.innerHTML = `
    ${header}
    <div class="history-section">
      <h3>Uploaded files</h3>
      <div class="run-artifacts">${uploaded}</div>
    </div>
    <div class="history-section">
      <h3>Pipeline</h3>
      <div class="run-pipeline">${pipeline}</div>
    </div>
    <div class="history-section">
      <h3>Output files</h3>
      <div class="run-artifacts">${outputs}</div>
    </div>
  `;
  wireHistoryDownloads();
}

async function loadHistoryDetails(sid) {
  if (!sid) {
    return;
  }
  setHistoryActive(sid);
  try {
    const response = await apiFetch(`/session/${sid}/state`);
    if (!response.ok) {
      throw new Error('history fetch failed');
    }
    const state = await response.json();
    SID = sid;
    window.__SID__ = SID;
    applyStateSnapshot(state);
    syncFlowFromSteps(state.steps || []);
    running = false;
  } catch (err) {
    alert('Unable to load history.');
  }
}

async function deleteHistorySession(sid) {
  if (!sid) {
    return;
  }
  const ok = window.confirm(`Delete session ${sid}? This cannot be undone.`);
  if (!ok) {
    return;
  }
  try {
    const response = await apiFetch(`/session/${sid}`, { method: 'DELETE' });
    if (!response.ok) {
      let message = 'Unable to delete session.';
      try {
        const data = await response.json();
        if (data?.detail) {
          message = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail);
        }
      } catch (err) {
        // best-effort parse
      }
      throw new Error(message);
    }
    if (HISTORY_SELECTED === sid) {
      HISTORY_SELECTED = null;
    }
    if (SID === sid) {
      await startNewSessionFromClear();
      return;
    }
    await loadHistory();
  } catch (err) {
    alert(err?.message || 'Unable to delete session.');
  }
}

async function renameHistorySession(sid) {
  if (!sid) {
    return;
  }
  const raw = window.prompt('Enter new session name:', sid);
  if (raw === null) {
    return;
  }
  const newSid = raw.trim();
  if (!newSid || newSid === sid) {
    return;
  }
  try {
    const response = await apiFetch(`/session/${sid}/rename`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_session_id: newSid })
    });
    if (!response.ok) {
      let message = 'Unable to rename session.';
      try {
        const data = await response.json();
        if (data?.detail) {
          message = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail);
        }
      } catch (err) {
        // best-effort parse
      }
      throw new Error(message);
    }
    const payload = await response.json();
    const updatedSid = payload?.new_session_id || newSid;
    if (HISTORY_SELECTED === sid) {
      HISTORY_SELECTED = updatedSid;
    }
    if (SID === sid) {
      SID = updatedSid;
      window.__SID__ = updatedSid;
    }
    await loadHistory();
  } catch (err) {
    alert(err?.message || 'Unable to rename session.');
  }
}

async function loadHistory() {
  const list = $('#history-list');
  if (!list) {
    return;
  }
  list.innerHTML = '<div class="muted">Loading history...</div>';
  try {
    const response = await apiFetch('/sessions');
    if (!response.ok) {
      throw new Error('history list failed');
    }
    const sessions = await response.json();
    const filtered = (sessions || []).filter(session => {
      const group = (session.group || '').toLowerCase();
      return group !== 'bulk';
    });
    const sorted = filtered.sort((a, b) => {
      const ta = Date.parse(a.updated_at || a.created_at || 0) || 0;
      const tb = Date.parse(b.updated_at || b.created_at || 0) || 0;
      return tb - ta;
    });
    if (!sorted.length) {
      list.innerHTML = '<div class="muted">No previous single-cell runs yet.</div>';
      return;
    }
    list.innerHTML = '';
    sorted.forEach(session => {
      const row = document.createElement('div');
      row.className = 'run-history-item';
      row.dataset.sid = session.session_id;
      const meta = [];
      const time = formatTimestamp(session.updated_at || session.created_at);
      if (time !== 'unknown') {
        meta.push(time);
      }
      if (typeof session.steps === 'number') {
        meta.push(`${session.steps} step${session.steps === 1 ? '' : 's'}`);
      }
      if (typeof session.artifacts === 'number') {
        meta.push(`${session.artifacts} artifact${session.artifacts === 1 ? '' : 's'}`);
      }
      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className = 'run-history-open';
      openBtn.innerHTML = `<strong>${esc(session.session_id)}</strong><small>${esc(meta.join(' | '))}</small>`;
      openBtn.addEventListener('click', () => loadHistoryDetails(session.session_id));

      const renameBtn = document.createElement('button');
      renameBtn.type = 'button';
      renameBtn.className = 'run-history-rename';
      renameBtn.title = `Rename ${session.session_id}`;
      renameBtn.setAttribute('aria-label', `Rename ${session.session_id}`);
      renameBtn.textContent = 'edit';
      renameBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        renameHistorySession(session.session_id);
      });

      const delBtn = document.createElement('button');
      delBtn.type = 'button';
      delBtn.className = 'run-history-delete';
      delBtn.title = `Delete ${session.session_id}`;
      delBtn.setAttribute('aria-label', `Delete ${session.session_id}`);
      delBtn.textContent = 'x';
      delBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        deleteHistorySession(session.session_id);
      });

      const actions = document.createElement('div');
      actions.className = 'run-history-actions';
      actions.appendChild(renameBtn);
      actions.appendChild(delBtn);

      row.appendChild(openBtn);
      row.appendChild(actions);
      list.appendChild(row);
    });
    if (HISTORY_SELECTED) {
      setHistoryActive(HISTORY_SELECTED);
    }
  } catch (err) {
    list.innerHTML = '<div class="muted">Unable to load history.</div>';
  }
}

function wireHistoryDownloads() {
  document.querySelectorAll('.history-download').forEach(link => {
    if (link.dataset.wired) {
      return;
    }
    link.dataset.wired = '1';
    link.addEventListener('click', async (event) => {
      event.preventDefault();
      const name = link.dataset.art;
      const sid = link.dataset.sid;
      if (!name || !sid) {
        return;
      }
      try {
        const response = await apiFetch(`/session/${sid}/download/${encodeURIComponent(name)}`);
        if (!response.ok) {
          throw new Error('Download failed');
        }
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = name;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      } catch (err) {
        alert('Download failed. Please try again.');
      }
    });
  });
}

// ----- Init -----
document.addEventListener('DOMContentLoaded', async () => {
  if (window.Auth && !window.Auth.requireAuth()) {
    return;
  }
  // wire buttons
  $('#upload-sc').addEventListener('click', uploadSCFiles);
  $('#validate').addEventListener('click', validateFlow);
  $('#runflow').addEventListener('click', async () => {
    if (SUPPRESS_NEXT_FLOW_RUN) {
      SUPPRESS_NEXT_FLOW_RUN = false;
      return;
    }
    await runFlow();
  });
  $('#stopflow')?.addEventListener('click', requestStopCurrentFlow);
  $('#clearflow').addEventListener('click', startNewSessionFromClear);
  wireFlowRunMenu();
  $('#session-new')?.addEventListener('click', () => window.location.reload());
  $('#unit-search').addEventListener('input', applySearch);
  $('#expAll').addEventListener('click', expandAll);
  $('#colAll').addEventListener('click', collapseAll);
  $('#history-refresh')?.addEventListener('click', loadHistory);

  resetPipelineProgress();
  setStopFlowEnabled(false);
  await ensureSession();
  await renderUnits();
  applySearch(); // initialize
  await loadHistory();
});
