// Enhanced UI logic with right-side pipeline panel and click-ordered pipeline.
let SID = null;
let UNITS_META = [];
let PIPELINE = []; // keeps {id, unit, label, card, params} in the order of user clicks
let PIPELINE_SEQ = 0; // simple counter for unique pipeline entries
let HISTORY_SELECTED = null;
let QC_OBJECT_URLS = [];
let STOP_REQUESTED = false;
let PIPELINE_RUNNING = false;
let LAST_RUN_WAS_CANCELLED = false;

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const apiFetch = (window.Auth && window.Auth.apiFetch) ? window.Auth.apiFetch : fetch;
/* ---------- Category config to reduce scrolling ---------- */
const CATEGORIES = {
  "Filtering and Quality Control": [
    "filter_quality","filter_length","filter_missing",
    "filter_repeats","filter_trimqual","filter_maskqual"
  ],
  "Tecnical processing(Primers)": [
    "mask_primers_score",
    "mask_primers_align",
    "mask_primers_extract"
  ],
  "Clustering & Consensus": [
    "collapse_seq","build_consensus"
  ],
};
const CAT_BY_ID = {};
Object.entries(CATEGORIES).forEach(([cat, ids])=>ids.forEach(id=>CAT_BY_ID[id]=cat));
function unitCategory(id) {
  return CAT_BY_ID[id] || "Other";
}

const FILTER_PREFIXES = ["filter_", "mask_primers_"];
const FUNNEL_TONES = ["tone-1", "tone-2", "tone-3", "tone-4", "tone-5", "tone-6"];
let FUNNEL_REQUEST_ID = 0;
const QC_PLOT_LABELS = {
  filter_quality: 'Quality score distribution',
  filter_length: 'Read length distribution',
  filter_missing: 'Missing bases (%N per read)',
  filter_repeats: 'Max homopolymer length',
  filter_trimqual: 'Trim quality (mean Phred)',
  filter_maskqual: 'Masked bases (%N per read)'
};

function isFilteringUnit(unitId) {
  if (!unitId) {
    return false;
  }
  const lower = unitId.toLowerCase();
  return FILTER_PREFIXES.some(prefix => lower.startsWith(prefix));
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
  // Always bump the request token so stale async log reads cannot repaint old data.
  const requestId = ++FUNNEL_REQUEST_ID;
  const steps = Array.isArray(state?.steps) ? state.steps : [];
  const filterSteps = steps.filter(step => isFilteringUnit(step?.unit || ''));
  if (!filterSteps.length) {
    mount.innerHTML = '<div class="muted">No filtering steps run yet.</div>';
    return;
  }
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

function extractPlotMeta(artifact) {
  const name = artifact?.name || '';
  const match = name.match(/(R[12])_(before|after|compare)$/i);
  if (!match) {
    return null;
  }
  return {
    channel: match[1].toUpperCase(),
    stage: match[2].toLowerCase()
  };
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
    const meta = extractPlotMeta(art);
    if (!meta) {
      return;
    }
    const stepIndex = typeof art.from_step === 'number' ? art.from_step : -1;
    if (!groups.has(stepIndex)) {
      groups.set(stepIndex, { stepIndex, channels: {} });
    }
    const group = groups.get(stepIndex);
    if (!group.channels[meta.channel]) {
      group.channels[meta.channel] = { compare: null, before: null, after: null };
    }
    group.channels[meta.channel][meta.stage] = art;
  });
  const ordered = Array.from(groups.values()).sort((a, b) => a.stepIndex - b.stepIndex);
  mount.innerHTML = '';
  ordered.forEach(group => {
    const step = stepsByIndex.get(group.stepIndex) || {};
    const unitId = step.unit || '';
    const label = QC_PLOT_LABELS[unitId] || getUnitLabel(unitId) || 'QC plot';
    const header = document.createElement('div');
    header.className = 'qc-group';
    const title = document.createElement('div');
    title.className = 'qc-title';
    title.textContent = `Step ${group.stepIndex + 1}: ${label}`;
    header.appendChild(title);
    Object.entries(group.channels).forEach(([channel, stages]) => {
      const row = document.createElement('div');
      row.className = 'qc-row';
      const card = document.createElement('div');
      card.className = 'qc-card';
      const art = stages.compare || stages.before || stages.after;
      if (art) {
        const img = document.createElement('img');
        img.alt = `${label} ${channel} before/after`;
        card.appendChild(img);
        const cap = document.createElement('div');
        cap.className = 'qc-caption';
        cap.textContent = `${channel} before vs after`;
        card.appendChild(cap);
        loadPlotImage(img, SID, art.name);
      } else {
        card.innerHTML = `<div class="muted">No plot (${channel})</div>`;
      }
      row.appendChild(card);
      header.appendChild(row);
    });
    mount.appendChild(header);
  });
}

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
  return meta ? meta.label : unitId;
}

function selectedSteps() {
  // Return pipeline in click order, but drop items whose cards are no longer on the page
  PIPELINE = PIPELINE.filter(step => step.card && step.card.isConnected);
  return [...PIPELINE];
}

function removePipelineStep(stepId){
  const before = PIPELINE.length;
  PIPELINE = PIPELINE.filter(step => step.id !== stepId);
  if(PIPELINE.length !== before){
    drawFlow();
  }
}

function drawFlow() {
  const steps = selectedSteps();
  const flow = $('#flow');
  flow.innerHTML = '';
  if (steps.length === 0) {
    flow.innerHTML = '<span class="muted">no steps selected</span>';
    return;
  }
  steps.forEach((step, index) => {
    const node = document.createElement('div');
    node.className = 'node';
    const entries = collectParamEntries(step.params);
    node.title = formatParamTitle(entries);
    node.tabIndex = 0;
    const label = document.createElement('span');
    label.className = 'node-label';
    label.textContent = `${index + 1}. ${step.label}`;
    const removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.className = 'node-remove';
    removeBtn.title = 'Remove from pipeline';
    removeBtn.setAttribute('aria-label', `Remove ${step.label} from pipeline`);
    removeBtn.textContent = 'x';
    removeBtn.addEventListener('click', () => removePipelineStep(step.id));
    node.appendChild(label);
    node.appendChild(removeBtn);
    flow.appendChild(node);
    if (index < steps.length - 1) {
      const arrow = document.createElement('div');
      arrow.className = 'arrow';
      arrow.textContent = '→';
      flow.appendChild(arrow);
    }
  });
}

function pipeMsg(text, cls = 'muted') {
  const msgElement = $('#pipe-msg');
  msgElement.className = cls;
  msgElement.textContent = text;
}

function setRunStatus(text) {
  $('#run-status').innerHTML = text;
}

function setStopButtonEnabled(enabled) {
  const button = $('#pipe-stop');
  if (!button) {
    return;
  }
  button.disabled = !enabled;
}

async function requestStopCurrentRun() {
  STOP_REQUESTED = true;
  if (!SID) {
    return;
  }
  setRunStatus('Stopping…');
  pipeMsg('Stopping current run…', 'warn');
  try {
    await apiFetch(`/session/${SID}/cancel`, { method: 'POST' });
  } catch (err) {
    // Frontend should still stop queuing additional steps even if cancel call fails.
  }
}

function setProgress(current, total) {
  const percentage = total ? Math.round((current / total) * 100) : 0;
  $('#run-bar').style.width = percentage + '%';
}

function setButtonRunning(button, isRunning, label) {
  if (!button) {
    return;
  }
  if (isRunning) {
    if (!button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.textContent;
    }
    button.textContent = label || 'Running...';
    button.disabled = true;
  } else {
    if (button.dataset.defaultLabel) {
      button.textContent = button.dataset.defaultLabel;
    }
    button.disabled = false;
  }
}

function setUploadReadsStatus(text, tone = 'info') {
  const out = $('#upload-out');
  if (!out) {
    return;
  }
  out.textContent = text || '';
  ['ok', 'warn', 'err'].forEach(cls => out.classList.remove(cls));
  if (tone && tone !== 'info') {
    out.classList.add(tone);
  }
}
async function startSession() {
  const response = await apiFetch('/session/start', { method: 'POST' });
  const data = await response.json();
  SID = data.session_id;
  HISTORY_SELECTED = null;
  const sidEl = $('#sid');
  if (sidEl) {
    sidEl.textContent = SID;
  }
  PIPELINE = []; // reset
  PIPELINE_SEQ = 0;
  await renderUnits();
  drawFlow();
  await refreshState();
  $('#validation').textContent = '—';
  setRunStatus('—');
  setProgress(0, 1);
  setUploadReadsStatus('', 'info');
  await loadHistory();
}

async function startFreshSessionLikeRefresh() {
  window.location.reload();
}

async function uploadReads() {
  const r1File = $('#r1f').files[0];
  if (!r1File) {
    alert('Choose R1');
    setUploadReadsStatus('Select an R1 FASTQ first.', 'warn');
    return;
  }
  const formData = new FormData();
  formData.append('r1', r1File);
  const r2File = $('#r2f').files[0];
  if (r2File) {
    formData.append('r2', r2File);
  }
  setUploadReadsStatus('Uploading...', 'info');
  try {
    const response = await apiFetch(`/session/${SID}/upload`, {
      method: 'POST',
      body: formData
    });
    if (!response.ok) {
      let errorText = response.statusText || 'Upload failed';
      try {
        const errData = await response.json();
        errorText = errData?.detail?.error || errData?.detail || errorText;
      } catch (err) {
        // response not JSON; ignore
      }
      throw new Error(errorText);
    }
    await refreshState();
    await loadHistory();
    const files = [r1File.name];
    if (r2File) {
      files.push(r2File.name);
    }
    setUploadReadsStatus(`Uploaded ${files.join(' + ')}`, 'ok');
  } catch (error) {
    console.error('uploadReads failed', error);
    const message = error?.message ? `Upload failed: ${error.message}` : 'Upload failed';
    setUploadReadsStatus(message, 'err');
    alert(error?.message || 'Upload failed');
  }
}

async function uploadAux() {
  const file = $('#auxf').files[0];
  if (!file) {
    alert('Choose file');
    return;
  }
  const formData = new FormData();
  formData.append('file', file);
  const name = $('#auxname').value.trim();
  if (name) {
    formData.append('name', name);
  }
  const response = await apiFetch(`/session/${SID}/upload-aux`, {
    method: 'POST',
    body: formData
  });
  const data = await response.json();
  $('#aux-out').textContent = `Stored as: ${data.stored_as}` +
    (data.role && data.role !== 'other' ? ` (auto as ${data.role})` : '');
  await refreshState();
  await loadHistory();
  if (data.role) {
    $$('.unit-card[data-unit="mask_primers_score"], .unit-card[data-unit="mask_primers_align"]')
      .forEach(card => {
        const select = card.querySelector('select[name="primer_fname"]');
        if (select && !select.value) {
          select.value = data.stored_as;
        }
      });
  }
}

function collectParams(card) {
  const params = {};
  card.querySelectorAll('input,select,textarea').forEach(element => {
    if (!element.name) {
      return;
    }
    if (element.type === 'file') {
      return;
    }
    params[element.name] = (element.type === 'checkbox') ?
      (element.checked ? 'true' : 'false') : element.value;
  });
  return params;
}
function collectAuxFiles(state){
  const names = [];
  const seen = new Set();
  const auxFiles = state?.aux_files || [];
  auxFiles.forEach(name => {
    if(name && !seen.has(name)){
      seen.add(name);
      names.push(name);
    }
  });
  const aux = state?.aux || {};
  Object.values(aux).forEach(name => {
    if(name && !seen.has(name)){
      seen.add(name);
      names.push(name);
    }
  });
  return names;
}

function updatePrimerSelects(state){
  const options = collectAuxFiles(state);
  const selects = document.querySelectorAll(
    '.unit-card[data-unit="mask_primers_score"] select[name="primer_fname"],' +
    '.unit-card[data-unit="mask_primers_align"] select[name="primer_fname"]'
  );
  selects.forEach(select => {
    const current = select.value;
    select.innerHTML = '';
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = 'choose...';
    select.appendChild(placeholder);
    options.forEach(name => {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;
      select.appendChild(opt);
    });
    if(current && Array.from(select.options).some(opt => opt.value === current)){
      select.value = current;
    }
  });
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

async function runUnit(card, unitId, forcedParams, opts = {}) {
  const params = forcedParams ? { ...forcedParams } : collectParams(card);
  const response = await apiFetch(`/session/${SID}/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ unit_id: unitId, params })
  });
  const data = await response.json();
  if (!response.ok) {
    const errorMsg = (data.detail && (data.detail.error || data.detail)) || response.statusText;
    const cancelled = /cancelled by user/i.test(String(errorMsg));
    LAST_RUN_WAS_CANCELLED = cancelled;
    if (!cancelled) {
      alert(`Error: ${errorMsg}`);
    }
    $('#log').textContent = (data.detail && data.detail.log_tail) ? data.detail.log_tail : '';
    return false;
  }
  await refreshState();
  const stepIndex = data.step.step_index;
  const logResponse = await apiFetch(`/session/${SID}/log/${stepIndex}`);
  $('#log').textContent = await logResponse.text();
  if (!opts.skipHistory) {
    await loadHistory();
  }
  return true;
}

function applyStateSnapshot(state) {
  const chips = Object.entries(state.current || {}).map(([key, value]) =>
    `<span class="chip">${esc(key)}: ${esc(value)}</span>`
  ).join(' ');
  $('#state').innerHTML = chips || '<span class="muted">no state</span>';
  const artifacts = Object.values(state.artifacts || {}).map(artifact =>
    `<div>${esc(artifact.name)} - <a href="#" class="art-download" data-art="${esc(artifact.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = artifacts || '<span class="muted">none</span>';
  window.__SESSION_STATE__ = state;
  updatePrimerSelects(state);
  wireDownloadLinks();
  updateFilteringFunnel(state);
  renderQcPlots(state);
}

async function refreshState() {
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

function syncPipelineFromSteps(steps) {
  PIPELINE = [];
  PIPELINE_SEQ = 0;
  (steps || []).forEach(step => {
    const unitId = step.unit || '';
    const meta = UNITS_META.find(unit => unit.id === unitId) || {};
    const label = meta.label || unitId;
    const card = document.querySelector(`.unit-card[data-unit="${cssEscape(unitId)}"]`);
    if (card) {
      applyParamsToCard(card, step.params);
    }
    PIPELINE.push({
      id: ++PIPELINE_SEQ,
      unit: unitId,
      label,
      card,
      params: step.params || {}
    });
  });
  drawFlow();
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
    const sidEl = $('#sid');
    if (sidEl) {
      sidEl.textContent = sid;
    }
    applyStateSnapshot(state);
    syncPipelineFromSteps(state.steps || []);
    pipeMsg('History loaded', 'muted');
  } catch (err) {
    pipeMsg('Unable to load history.', 'err');
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
      await startSession();
      pipeMsg('Session deleted. Started a new session.', 'ok');
      return;
    }
    await loadHistory();
    pipeMsg('Session deleted.', 'ok');
  } catch (err) {
    pipeMsg(err?.message || 'Unable to delete session.', 'err');
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
      const sidEl = $('#sid');
      if (sidEl) {
        sidEl.textContent = updatedSid;
      }
    }
    await loadHistory();
    pipeMsg(`Session renamed to ${updatedSid}.`, 'ok');
  } catch (err) {
    pipeMsg(err?.message || 'Unable to rename session.', 'err');
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
      return group !== 'sc';
    });
    const sorted = filtered.sort((a, b) => {
      const ta = Date.parse(a.updated_at || a.created_at || 0) || 0;
      const tb = Date.parse(b.updated_at || b.created_at || 0) || 0;
      return tb - ta;
    });
    if (!sorted.length) {
      list.innerHTML = '<div class="muted">No previous bulk runs yet.</div>';
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

/* -------------------- New grouped render -------------------- */
function makeUnitCard(u){
  const card = document.createElement('div');
  card.className = 'unit-card';
  card.dataset.unit = u.id;

  let paramsHTML = '<div class="params-body no-params">No parameters</div>';
  if (u.params_schema && Object.keys(u.params_schema).length){
    let inner = '';
    for (const [k,v] of Object.entries(u.params_schema||{})){
      const help = v.help ? ` <span class="muted">— ${esc(v.help)}</span>` : '';
      const label = `<label>${esc(k)}${help}</label>`;
      if (v.type === 'select') {
        const opts = (v.options||[]).map(o => {
          if (typeof o === 'string') {
            const selected = o === v.default ? 'selected' : '';
            return `<option value="${esc(o)}" ${selected}>${esc(o)}</option>`;
          }
          const value = o?.value ?? '';
          const labelText = o?.label ?? value;
          const selected = value === v.default ? 'selected' : '';
          return `<option value="${esc(value)}" ${selected}>${esc(labelText)}</option>`;
        }).join('');
        inner += `${label}<select name="${esc(k)}">${opts}</select>`;
      } else if (v.type === 'checkbox') {
        const checked = v.default ? 'checked' : '';
        inner += `<label class="checkbox-label"><input type="checkbox" name="${esc(k)}" ${checked}> ${esc(k)}${help}</label>`;
      } else if (v.type === 'file') {
        inner += `${label}<input name="${esc(k)}" placeholder="${esc(v.accept||'')}" />` +
                 `<div class="muted">Upload in section 1 → aux; I'll fill this automatically.</div>`;
      } else {
        const val = v.default ?? ''; const ph = v.placeholder ?? '';
        const typeAttr = (v.type === 'int') ? 'type="number"' : 'type="text"';
        inner += `${label}<input ${typeAttr} name="${esc(k)}" value="${esc(val)}" placeholder="${esc(ph)}">`;
      }
    }
    paramsHTML = `
      <details class="params">
        <summary>Parameters</summary>
        <div class="params-body">${inner}</div>
      </details>`;
  }

  card.innerHTML = `
    <div class="card-head">
      <h3>${esc(u.label)}</h3>
    </div>
    ${paramsHTML}
    <div class="card-actions row">
      <button class="run">Run</button>
      <button type="button" class="secondary pipe-add" data-unit-id="${esc(u.id)}" aria-pressed="false">Add to pipeline</button>
    </div>`;

  card.querySelector('.run').addEventListener('click', async (event) => {
    const button = event.currentTarget;
    setButtonRunning(button, true, 'Running...');
    try {
      await runUnit(card, u.id);
    } finally {
      setButtonRunning(button, false);
    }
  });
  const btn = card.querySelector('.pipe-add');
  btn.addEventListener('click', () => {
    const unitId = btn.dataset.unitId || card.dataset.unit;
    const meta = UNITS_META.find(m => m.id === unitId) || {};
    const label = meta.label || unitId;
    PIPELINE.push({ id: ++PIPELINE_SEQ, unit: unitId, label, card, params: collectParams(card) });
    drawFlow();
  });

  // searchable haystack
  card.dataset.haystack = [u.id, u.label, ...(u.requires||[]), unitCategory(u.id)].join(' ').toLowerCase();
  return card;
}

async function renderUnits(){
  let all = [];
  try {
    const res = await apiFetch(`/session/${SID}/units?group=bulk`);
    all = await res.json();
  } catch (e) {
    try {
      const res2 = await apiFetch(`/session/${SID}/units`);
      all = await res2.json();
    } catch (e2) { all = []; }
  }

  UNITS_META = (all || []).filter(u => {
    const id     = (u.id || '').toLowerCase();
    const label  = (u.label || '').toLowerCase();
    const group  = (u.group || '').toLowerCase(); // if backend sends it

    if (group) return group === 'bulk';          // preferred path
    if (id.startsWith('sc_')) return false;      // legacy naming
    if (label.startsWith('sc:')) return false;   // visible label hint
    return true;                                 // treat everything else as bulk
  });

  const byCat = {};
  UNITS_META.forEach(u => {
    const cat = unitCategory(u.id);
    if(!byCat[cat]) byCat[cat] = [];
    byCat[cat].push(makeUnitCard(u));
  });

  const mount = $('#unit-groups');
  if (!mount){ console.warn('unit-groups container not found'); return; }
  mount.innerHTML = '';

  Object.keys(CATEGORIES).concat(Object.keys(byCat).filter(c => !(c in CATEGORIES))).forEach(cat => {
    if(!byCat[cat] || byCat[cat].length === 0) return;
    const det = document.createElement('details');
    det.className = 'group';
    det.open = (cat === 'Filtering');

    const sum = document.createElement('summary');
    sum.innerHTML = `<span class="group-title">${esc(cat)}</span><span class="group-count">${byCat[cat].length}</span>`;
    const body = document.createElement('div'); body.className = 'group-body';
    byCat[cat].forEach(card => body.appendChild(card));
    const closeRow = document.createElement('div');
    closeRow.className = 'group-end-actions';
    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'group-close-btn';
    closeBtn.title = `Collapse ${cat}`;
    closeBtn.setAttribute('aria-label', `Collapse ${cat}`);
    closeBtn.innerHTML = '<span class="group-close-icon" aria-hidden="true"></span>';
    closeBtn.addEventListener('click', () => {
      det.open = false;
      sum.scrollIntoView({ block: 'nearest' });
    });
    closeRow.appendChild(closeBtn);
    body.appendChild(closeRow);

    det.appendChild(sum); det.appendChild(body);
    mount.appendChild(det);
  });

  // Search & bulk expand/collapse
  const q = $('#unit-search');
  if (q && !q.dataset.wired){
    q.dataset.wired = '1';
    q.addEventListener('input', () => {
      const needle = q.value.trim().toLowerCase();
      const groups = mount.querySelectorAll('details.group');
      groups.forEach(g => {
        const cards = g.querySelectorAll('.unit-card');
        let visible = 0;
        cards.forEach(c => {
          const hit = !needle || c.dataset.haystack.includes(needle);
          c.style.display = hit ? '' : 'none';
          if(hit) visible++;
        });
        g.style.display = visible ? '' : 'none';
        if(needle && visible) g.open = true;
        const countEl = g.querySelector('.group-count'); if(countEl) countEl.textContent = String(visible);
      });
    });
  }
  const expandBtn = $('#expand-all'); const collapseBtn = $('#collapse-all');
  if(expandBtn && !expandBtn.dataset.wired){
    expandBtn.dataset.wired = '1';
    expandBtn.addEventListener('click', ()=>mount.querySelectorAll('details.group').forEach(d=>d.open=true));
  }
  if(collapseBtn && !collapseBtn.dataset.wired){
    collapseBtn.dataset.wired = '1';
    collapseBtn.addEventListener('click', ()=>mount.querySelectorAll('details.group').forEach(d=>d.open=false));
  }

}

/* ===== Validation ===== */
function validateMaskPrimers(step){
  const paramsSnapshot = step.params || (step.card ? collectParams(step.card) : {});
  const unitId = (step.unit || '').toLowerCase();
  if(unitId === 'mask_primers_score' || unitId === 'mask_primers_align'){
    const primerValue = (paramsSnapshot.primer_fname || '').trim();
    const aux = (window.__SESSION_STATE__ && window.__SESSION_STATE__.aux) || {};
    const ok = primerValue.length > 0 || !!aux?.v_primers || !!aux?.c_primers;
    const label = unitId === 'mask_primers_align' ? 'MaskPrimers align' : 'MaskPrimers score';
    return ok ? {ok:true,msg:`${label}: OK`} : {ok:false,msg:`${label} needs a primer file (upload aux or fill filename).`};
  }
  if(unitId === 'mask_primers_extract'){
    return {ok:true,msg:'MaskPrimers extract: OK'};
  }
  return {ok:true,msg:'MaskPrimers: OK'};
}
function validateConsensus(step){
  return {ok:true,msg:'BuildConsensus: ensure BARCODE exists (MaskPrimers extract tag).'};
}

function validatePipeline(){
  const steps = selectedSteps();
  drawFlow();
  if(steps.length===0){ $('#validation').innerHTML = '<span class="warn">No steps selected.</span>'; return; }
  const msgs = [];
  let okAll = true;
  // Filter out any single-cell units that might have been added
  const bulkSteps = steps.filter(st => !(st.unit || '').startsWith('sc_'));
  if(bulkSteps.length !== steps.length){
    okAll = false;
    msgs.push(`<div class="err">• Single-cell units detected and removed from pipeline</div>`);
  }
  for(const st of bulkSteps){
    let res = {ok:true,msg:'OK'};
    if(st.unit === 'mask_primers_score' || st.unit === 'mask_primers_align' || st.unit === 'mask_primers_extract') res = validateMaskPrimers(st);
    else if(st.unit === 'build_consensus') res = validateConsensus(st);
    okAll = okAll && res.ok;
    msgs.push(`<div class="${res.ok?'ok':'err'}">• ${esc(st.label)}: ${esc(res.msg)}</div>`);
  }
  $('#validation').innerHTML = msgs.join('') || '—';
  pipeMsg(okAll ? 'Validation passed' : 'Validation found issues', okAll ? 'ok' : 'warn');
}

/* ===== Run Pipeline ===== */
async function runLinearPipeline(){
  const steps = selectedSteps();
  const bulkSteps = steps.filter(st => !(st.unit || '').startsWith('sc_'));
  if(bulkSteps.length === 0){ pipeMsg('No bulk steps selected','warn'); return; }
  STOP_REQUESTED = false;
  LAST_RUN_WAS_CANCELLED = false;
  PIPELINE_RUNNING = true;
  setStopButtonEnabled(true);
  if(bulkSteps.length !== steps.length){
    pipeMsg('Single-cell units removed from pipeline','warn');
  }
  validatePipeline(); // show current validation info
  setRunStatus('Starting…'); setProgress(0, bulkSteps.length);

  for(let i=0;i<bulkSteps.length;i++){
    if (STOP_REQUESTED) {
      setRunStatus(`Stopped (${i}/${bulkSteps.length})`);
      pipeMsg('Pipeline stopped','warn');
      return;
    }
    const s = bulkSteps[i];
    setRunStatus(`Running <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`);
    const ok = await runUnit(s.card, s.unit, s.params, { skipHistory: true });
    setProgress(i+1, bulkSteps.length);
    if(!ok){
      if (STOP_REQUESTED || LAST_RUN_WAS_CANCELLED) {
        setRunStatus(`Stopped at <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`);
        pipeMsg('Pipeline stopped','warn');
        return;
      }
      setRunStatus(`Failed at <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`);
      pipeMsg('Pipeline failed','err');
      return;
    }
  }
  setRunStatus('Finished ✅'); pipeMsg('Pipeline finished','ok');
}


async function runPipeline(){
  try {
    await runLinearPipeline();
    await loadHistory();
  } finally {
    PIPELINE_RUNNING = false;
    setStopButtonEnabled(false);
    STOP_REQUESTED = false;
  }
}

/* ===== Wire up ===== */
document.addEventListener('DOMContentLoaded', () => {
  if (window.Auth && !window.Auth.requireAuth()) {
    return;
  }
  startSession();
  $('#upload')?.addEventListener('click', uploadReads);
  $('#upload-aux')?.addEventListener('click', uploadAux);
  $('#session-new')?.addEventListener('click', startFreshSessionLikeRefresh);
  $('#history-refresh')?.addEventListener('click', loadHistory);
  $('#pipe-validate')?.addEventListener('click', validatePipeline);
  $('#pipe-run')?.addEventListener('click', async (event) => {
    const button = event.currentTarget;
    setButtonRunning(button, true, 'Running...');
    try {
      await runPipeline();
    } finally {
      setButtonRunning(button, false);
    }
  });
  $('#pipe-stop')?.addEventListener('click', requestStopCurrentRun);
  $('#pipe-clear')?.addEventListener('click', startFreshSessionLikeRefresh);
  setStopButtonEnabled(false);
});
