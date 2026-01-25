// Enhanced UI logic with right-side pipeline panel and click-ordered pipeline.
let SID = null;
let UNITS_META = [];
let PIPELINE = []; // keeps {id, unit, label, card, params} in the order of user clicks
let PIPELINE_SEQ = 0; // simple counter for unique pipeline entries

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
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
  const response = await fetch('/session/start', { method: 'POST' });
  const data = await response.json();
  SID = data.session_id;
  $('#sid').textContent = SID;
  PIPELINE = []; // reset
  PIPELINE_SEQ = 0;
  await renderUnits();
  drawFlow();
  await refreshState();
  $('#validation').textContent = '—';
  setRunStatus('—');
  setProgress(0, 1);
  setUploadReadsStatus('', 'info');
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
    const response = await fetch(`/session/${SID}/upload`, {
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
  const response = await fetch(`/session/${SID}/upload-aux`, {
    method: 'POST',
    body: formData
  });
  const data = await response.json();
  $('#aux-out').textContent = `Stored as: ${data.stored_as}` +
    (data.role && data.role !== 'other' ? ` (auto as ${data.role})` : '');
  await refreshState();
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

async function runUnit(card, unitId, forcedParams) {
  const params = forcedParams ? { ...forcedParams } : collectParams(card);
  const response = await fetch(`/session/${SID}/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ unit_id: unitId, params })
  });
  const data = await response.json();
  if (!response.ok) {
    const errorMsg = (data.detail && (data.detail.error || data.detail)) || response.statusText;
    alert(`Error: ${errorMsg}`);
    $('#log').textContent = (data.detail && data.detail.log_tail) ? data.detail.log_tail : '';
    return false;
  }
  await refreshState();
  const stepIndex = data.step.step_index;
  const logResponse = await fetch(`/session/${SID}/log/${stepIndex}`);
  $('#log').textContent = await logResponse.text();
  return true;
}

async function refreshState() {
  const response = await fetch(`/session/${SID}/state`);
  const state = await response.json();
  const chips = Object.entries(state.current || {}).map(([key, value]) =>
    `<span class="chip">${esc(key)}: ${esc(value)}</span>`
  ).join(' ');
  $('#state').innerHTML = chips || '<span class="muted">no state</span>';
  const artifacts = Object.values(state.artifacts || {}).map(artifact =>
    `<div>${esc(artifact.name)} — <a href="/session/${SID}/download/${encodeURIComponent(artifact.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = artifacts || '<span class="muted">none</span>';
  window.__SESSION_STATE__ = state;
  updatePrimerSelects(state);
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
    const res = await fetch(`/session/${SID}/units?group=bulk`);
    all = await res.json();
  } catch (e) {
    try {
      const res2 = await fetch(`/session/${SID}/units`);
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
  if(bulkSteps.length !== steps.length){
    pipeMsg('Single-cell units removed from pipeline','warn');
  }
  validatePipeline(); // show current validation info
  setRunStatus('Starting…'); setProgress(0, bulkSteps.length);

  for(let i=0;i<bulkSteps.length;i++){
    const s = bulkSteps[i];
    setRunStatus(`Running <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`);
    const ok = await runUnit(s.card, s.unit, s.params);
    setProgress(i+1, bulkSteps.length);
    if(!ok){ setRunStatus(`Failed at <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`); pipeMsg('Pipeline failed','err'); return; }
  }
  setRunStatus('Finished ✅'); pipeMsg('Pipeline finished','ok');
}


async function runPipeline(){
  await runLinearPipeline();
}

/* ===== Wire up ===== */
document.addEventListener('DOMContentLoaded', () => {
  startSession();
  $('#upload')?.addEventListener('click', uploadReads);
  $('#upload-aux')?.addEventListener('click', uploadAux);
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
  $('#pipe-clear')?.addEventListener('click', ()=>{
    PIPELINE = [];
    PIPELINE_SEQ = 0;
    drawFlow(); $('#validation').textContent='—'; pipeMsg('Pipeline cleared');
  });
});
