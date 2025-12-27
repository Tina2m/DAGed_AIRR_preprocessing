// Enhanced UI logic with right-side pipeline panel and click-ordered pipeline.
let SID = null;
let UNITS_META = [];
let PIPELINE = []; // keeps {unit, label, card} in the order of user clicks

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
/* ---------- Channel mapping for DAG metadata sync ---------- */
const CHANNEL_MAP = {
  filter_quality: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  filter_length: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  filter_missing: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  filter_repeats: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  filter_trimqual: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  filter_maskqual: { consumes: ['R1','R2'], produces: ['R1','R2'] },
  mask_primers: { consumes: ['R1','R2','PAIR1','PAIR2'], produces: ['R1','R2','PAIR1','PAIR2'] },
  pairseq: { consumes: ['R1','R2'], produces: ['PAIR1','PAIR2'] },
  assemble_align: { consumes: ['PAIR1','PAIR2'], produces: ['ASSEMBLED'] },
  assemble_join: { consumes: ['PAIR1','PAIR2'], produces: ['ASSEMBLED'] },
  assemble_sequential: { consumes: ['PAIR1','PAIR2'], produces: ['ASSEMBLED'] },
  collapse_seq: { consumes: ['ASSEMBLED'], produces: ['ASSEMBLED'] },
  build_consensus: { consumes: ['R1','R2','PAIR1','PAIR2'], produces: ['R1','R2','PAIR1','PAIR2'] },
};
const DAG_BRANCH_KEYS = ['R1','R2'];

/* ---------- Category config to reduce scrolling ---------- */
const CATEGORIES = {
  "Filtering and Quality Control": [
    "filter_quality","filter_length","filter_missing",
    "filter_repeats","filter_trimqual","filter_maskqual"
  ],
  "Tecnical processing(Primers)": [
    "mask_primers"
  ],
  "Pairing & Assembly": [
    "pairseq","assemble_align","assemble_join","assemble_sequential"
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
  // Return pipeline in click order, but drop items whose checkbox is no longer checked
  PIPELINE = PIPELINE.filter(step => step.card && step.card.querySelector('.pipe-add')?.checked);
  return [...PIPELINE];
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
    node.textContent = step.label;
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
function setBranchProgress(branch, completed, total, state){
  const key = (branch || '').toUpperCase();
  const bar = document.getElementById(`branch-bar-${key}`);
  const status = document.getElementById(`branch-status-${key}`);
  const row = document.querySelector(`.branch-progress-row[data-branch="${key}"]`);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  if(bar){
    bar.style.width = percent + '%';
  }
  if(status){
    let label = total ? `${completed}/${total}` : 'No nodes';
    if(state === 'error'){
      label += ' (error)';
    } else if(state === 'blocked'){
      label += ' (blocked)';
    }
    status.textContent = label;
    status.classList.remove('err','warn');
    if(state === 'error'){
      status.classList.add('err');
    } else if(state === 'blocked'){
      status.classList.add('warn');
    }
  }
  if(row){
    row.classList.toggle('empty', !total);
  }
}
function resetBranchProgress(){
  DAG_BRANCH_KEYS.forEach(branch => setBranchProgress(branch, 0, 0));
}
function hasDagPipeline(){
  const isPaired = (window.__BULK_MODE || '').toLowerCase() === 'paired' || document.body.classList.contains('mode-paired');
  const api = window.BulkDag;
  return isPaired && !!(api && typeof api.hasNodes === 'function' && api.hasNodes());
}

async function startSession() {
  const response = await fetch('/session/start', { method: 'POST' });
  const data = await response.json();
  SID = data.session_id;
  $('#sid').textContent = SID;
  PIPELINE = []; // reset
  await renderUnits();
  drawFlow();
  await refreshState();
  $('#validation').textContent = '—';
  setRunStatus('—');
  setProgress(0, 1);
  resetBranchProgress();
}

async function uploadReads() {
  const r1File = $('#r1f').files[0];
  if (!r1File) {
    alert('Choose R1');
    return;
  }
  const formData = new FormData();
  formData.append('r1', r1File);
  const r2File = $('#r2f').files[0];
  if (r2File) {
    formData.append('r2', r2File);
  }
  const response = await fetch(`/session/${SID}/upload`, {
    method: 'POST',
    body: formData
  });
  if (!response.ok) {
    alert('Upload failed');
    return;
  }
  await refreshState();
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
  // auto-fill in MaskPrimers cards
  if (data.role === 'v_primers' || data.role === 'other') {
    $$('.card[data-unit="mask_primers"] input[name="v_primers_fname"]').forEach(el => {
      if (!el.value) {
        el.value = data.stored_as;
      }
    });
  }
  if (data.role === 'c_primers') {
    $$('.card[data-unit="mask_primers"] input[name="c_primers_fname"]').forEach(el => {
      if (!el.value) {
        el.value = data.stored_as;
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
    if (element.classList.contains('pipe-add')) {
      return;
    }
    params[element.name] = (element.type === 'checkbox') ?
      (element.checked ? 'true' : 'false') : element.value;
  });
  return params;
}

function availableDagFiles(){
  const files = [];
  const seen = new Set();
  const state = window.__SESSION_STATE__ || {};
  const artifacts = state.artifacts || {};
  const regex = /\.(fastq|fq|fasta|fa)(\.gz)?$/i;

  const addArtifact = (artifactKey, entry, channelHint, prefixLabel) => {
    if(!artifactKey || seen.has(artifactKey)) return;
    const fileLabel = (entry && (entry.path || entry.name || artifactKey)) || artifactKey;
    const kind = (entry?.kind || '').toLowerCase();
    const looksLikeSeq = regex.test(fileLabel) || kind === 'fastq' || kind === 'fasta';
    if(!looksLikeSeq) return;
    const channel = (channelHint || entry?.channel || guessChannel(fileLabel) || '').toUpperCase();
    const labelParts = [];
    if(prefixLabel) labelParts.push(prefixLabel);
    if(channel) labelParts.push(channel);
    labelParts.push(fileLabel);
    files.push({ value: artifactKey, label: labelParts.filter(Boolean).join(' - '), channel });
    seen.add(artifactKey);
  };

  Object.entries(state.current || {}).forEach(([channelKey, artifactKey]) => {
    const entry = artifacts[artifactKey];
    addArtifact(artifactKey, entry, channelKey, `${channelKey} current`);
  });

  Object.entries(artifacts).forEach(([artifactKey, entry]) => {
    addArtifact(artifactKey, entry, entry?.channel || '', '');
  });
  return files;
}

function guessChannel(name){
  if(!name) return '';
  const upper = name.toUpperCase();
  if(upper.includes('TRA')) return 'TRA';
  if(upper.includes('TRB')) return 'TRB';
  if(upper.includes('R2')) return 'R2';
  if(upper.includes('MERGED')) return 'MERGED';
  if(upper.includes('R1')) return 'R1';
  return '';
}

function fillDagSelect(select, files){
  if(!select) return;
  const prev = select.value;
  const allowed = (select.dataset.channels || '').split(',').map(s=>s.trim()).filter(Boolean);
  const fixed = (select.dataset.channel || '').toUpperCase();
  select.innerHTML = '<option value="">Select file...</option>';
  files.forEach(file => {
    const channel = file.channel || guessChannel(file.value);
    if(fixed && channel && channel !== fixed) return;
    if(allowed.length && channel && !allowed.includes(channel)) return;
    const opt = document.createElement('option');
    opt.value = file.value;
    opt.textContent = file.label;
    opt.dataset.channel = channel || '';
    select.appendChild(opt);
  });
  if(prev && Array.from(select.options).some(opt => opt.value === prev)){
    select.value = prev;
  }
}

function refreshDagFileSelects(){
  const files = availableDagFiles();
  document.querySelectorAll('.dag-file-select').forEach(select => fillDagSelect(select, files));
}

function determineDagBranch(meta, selections){
  const firstSelectionChannel = (selections || [])
    .map(sel => (sel.channel || '').toUpperCase())
    .find(Boolean);
  if(meta.branchTarget) return meta.branchTarget;
  if(meta.dynamicBranch){
    return firstSelectionChannel || meta.branches?.[0] || 'R1';
  }
  if(firstSelectionChannel && meta.branches?.includes(firstSelectionChannel)){
    return firstSelectionChannel;
  }
  if(meta.branches && meta.branches.length){
    return meta.branches[0];
  }
  return firstSelectionChannel || 'R1';
}

function decorateDagControls(card, unit){
  const dagApi = window.BulkDag;
  if(!dagApi?.getUnitMeta || !dagApi?.createNode) return;
  const meta = dagApi.getUnitMeta(unit.id);
  if(!meta) return;
  const inputs = Array.isArray(meta.inputs) && meta.inputs.length
    ? meta.inputs
    : (meta.dynamicBranch ? [{ id: 'input', label: 'Input FASTQ', channels: meta.branches || ['R1','R2'] }] : []);
  if(!inputs.length) return;

  const block = document.createElement('div');
  block.className = 'dag-controls dag-only';
  block.innerHTML = '<div class="muted">Select FASTQ/FASTA files to add this unit as a DAG node.</div>';

  const selects = [];
  inputs.forEach(input => {
    const label = document.createElement('label');
    label.textContent = input.label || 'Input file';
    const select = document.createElement('select');
    select.className = 'dag-file-select';
    if(input.channels) select.dataset.channels = input.channels.map(ch=>ch.toUpperCase()).join(',');
    if(input.channel) select.dataset.channel = input.channel.toUpperCase();
    select.dataset.inputId = input.id || (input.label || unit.id);
    label.appendChild(select);
    block.appendChild(label);
    selects.push(select);
  });

  const addBtn = document.createElement('button');
  addBtn.type = 'button';
  addBtn.textContent = 'Add to DAG';
  addBtn.addEventListener('click', () => {
    const selections = [];
    let valid = true;
    selects.forEach(select => {
      const value = select.value;
      if(!value){
        valid = false;
        select.classList.add('input-error');
      } else {
        select.classList.remove('input-error');
      }
      const option = select.selectedOptions[0];
      selections.push({
        id: select.dataset.inputId,
        file: value,
        channel: select.dataset.channel || (option?.dataset.channel || '').toUpperCase(),
      });
    });
    if(!valid){
      alert('Select file(s) before adding this node.');
      return;
    }
    const branch = determineDagBranch(meta, selections);
    const params = collectParams(card);
    params.__files = selections.map(s => `${s.id}:${s.file}`).join(',');
    dagApi.createNode(unit.id, branch, {
      params,
      inputs: selections,
    });
  });
  block.appendChild(addBtn);

  const actions = card.querySelector('.actions');
  actions?.insertAdjacentElement('afterbegin', block);
  refreshDagFileSelects();
}
async function runUnit(card, unitId) {
  const params = collectParams(card);
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

async function runDagNode(node){
  const payload = {
    unit_id: node.unitId,
    params: node.params || {},
  };
  try{
    const res = await fetch(`/session/${SID}/run`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    let data = {};
    try{
      data = await res.json();
    }catch(e){
      data = {};
    }
    if(!res.ok){
      const logTail = data.detail?.log_tail || '';
      if(logTail) $('#log').textContent = logTail;
      return { ok:false, error: data.detail?.error || res.statusText };
    }
    await refreshState();
    if(data.step?.step_index !== undefined){
      const lr = await fetch(`/session/${SID}/log/${data.step.step_index}`);
      $('#log').textContent = await lr.text();
    }
    return { ok:true };
  }catch(err){
    return { ok:false, error: err.message };
  }
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
  refreshDagFileSelects();
}

/* -------------------- New grouped render -------------------- */
function makeUnitCard(u){
  const card = document.createElement('div');
  card.className = 'card compact';
  card.dataset.unit = u.id;

  const req = (u.requires||[]).map(x=>`<span class="chip">${esc(x)}</span>`).join(' ') || '<span class="muted">none</span>';
  let paramsHTML = '';
  if (u.params_schema && Object.keys(u.params_schema).length){
    let inner = '';
    for (const [k,v] of Object.entries(u.params_schema||{})){
      const help = v.help ? ` <span class="muted">— ${esc(v.help)}</span>` : '';
      const label = `<label>${esc(k)}${help}</label>`;
      if (v.type === 'select') {
        const opts = (v.options||[]).map(o => `<option value="${esc(o)}" ${o===v.default?'selected':''}>${esc(o)}</option>`).join('');
        inner += `${label}<select name="${esc(k)}">${opts}</select>`;
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
        <div class="param-wrap">${inner}</div>
      </details>`;
  }

  card.innerHTML = `
    <div class="card-head"><h3>${esc(u.label)}</h3></div>
    <div class="reqs">requires: ${req}</div>
    ${paramsHTML}
    <div class="actions">
      <button class="run">Run</button>
      <label class="row"><input type="checkbox" class="pipe-add" data-unit-id="${esc(u.id)}"> Add to pipeline</label>
    </div>`;

  card.querySelector('.run').addEventListener('click', ()=>runUnit(card,u.id));
  const chk = card.querySelector('.pipe-add');
  chk.addEventListener('change', () => {
    const unitId = chk.dataset.unitId || card.dataset.unit;
    const meta = UNITS_META.find(m => m.id === unitId) || {};
    const label = meta.label || unitId;

    if (chk.checked) {
      PIPELINE = PIPELINE.filter(s => s.unit !== unitId);
      PIPELINE.push({unit: unitId, label, card});
    } else {
      PIPELINE = PIPELINE.filter(s => s.unit !== unitId);
    }
    drawFlow();
  });

  // searchable haystack
  card.dataset.haystack = [u.id, u.label, ...(u.requires||[]), unitCategory(u.id)].join(' ').toLowerCase();
  decorateDagControls(card, u);
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

  syncDagMetaFromUnits(UNITS_META);

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
        const cards = g.querySelectorAll('.card');
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

  refreshDagFileSelects();
}

function syncDagMetaFromUnits(units) {
  const exporter = window.BulkDag && window.BulkDag.loadUnitMeta;
  if (!exporter) {
    return;
  }
  const metaMap = {};
  units.forEach(unit => {
    if (!(unit.id || '').startsWith('sc_')) { // keep bulk units only
      const dag = unit.dag_meta || {};
      const ch = dag.channels || {};
      const consumes = dag.consumes || dag.inputs || ch.consumes || ch.inputs || [];
      const produces = dag.produces || dag.outputs || ch.produces || ch.outputs || consumes || [];
      const rawBranches = dag.branches || dag.allowed_branches || ch.branches || dag.branch;
      const branches = Array.isArray(rawBranches) && rawBranches.length ? rawBranches : ['R1','R2'];
      const inputDefs = Array.isArray(dag.inputs) ? dag.inputs : [];
      
      // Use CHANNEL_MAP as fallback if no dag_meta
      const channelInfo = CHANNEL_MAP[unit.id] || { consumes: [], produces: [] };
      const finalConsumes = consumes.length ? consumes : channelInfo.consumes;
      const finalProduces = produces.length ? produces : channelInfo.produces;
      
      const meta = {
        label: unit.label || unit.id,
        consumes: Array.isArray(finalConsumes) && finalConsumes.length ? finalConsumes : ['R1'],
        produces: Array.isArray(finalProduces) && finalProduces.length ? finalProduces : (Array.isArray(finalConsumes) && finalConsumes.length ? finalConsumes : ['R1']),
        branches,
        params: Object.entries(unit.params_schema || {}).map(([key, val]) => ({
          key,
          label: val.label || key,
          type: val.type === 'select' ? 'select' : (val.type === 'number' || val.type === 'int' ? 'number' : 'text'),
          options: val.options || [],
          default: val.default,
          min: val.min,
          max: val.max,
          step: val.step,
          placeholder: val.placeholder,
          help: val.help,
        })),
      };
      if (dag.dynamicBranch) {
        meta.dynamicBranch = true;
      }
      if (dag.branch_target) {
        meta.branchTarget = dag.branch_target;
      }
      if (inputDefs.length) {
        meta.inputs = inputDefs.map(inp => ({
          id: inp.id,
          label: inp.label,
          channel: inp.channel,
          channels: inp.channels || inp.channelOptions,
        }));
      } else {
        const channels = meta.dynamicBranch
          ? (meta.branches && meta.branches.length ? meta.branches : ['R1','R2'])
          : (meta.consumes && meta.consumes.length ? meta.consumes : ['R1']);
        meta.inputs = [{
          id: 'input',
          label: 'Input FASTQ/FASTA',
          channels: channels.map(ch => ch.toUpperCase()),
        }];
      }
      metaMap[unit.id] = meta;
    }
  });
  exporter(metaMap);
}

/* ===== Validation ===== */
function validateDagPipeline(dagApi){
  const snapshot = dagApi.serialize?.();
  if(!snapshot || snapshot.nodes.length === 0){
    $('#validation').innerHTML = '<span class="warn">Add nodes to the DAG to validate.</span>';
    pipeMsg('No DAG nodes selected','warn');
    return false;
  }
  const order = dagApi.topoOrder?.();
  if(!order){
    $('#validation').innerHTML = '<div class="err">Cycle detected in DAG. Fix connections.</div>';
    pipeMsg('Cycle detected in DAG','err');
    return false;
  }
  const labelMap = {};
  snapshot.nodes.forEach(n => labelMap[n.id] = n.label);
  const rows = order.map((id, idx) => `<div>${idx+1}. ${esc(labelMap[id] || id)}</div>`).join('');
  $('#validation').innerHTML = rows;
  pipeMsg('DAG validation OK','ok');
  return true;
}

function validateMaskPrimers(step){
  const card = step.card;
  const variant = (card.querySelector('[name="variant"]')?.value || 'align').toLowerCase();
  if(variant === 'align' || variant === 'score'){
    const vbox = card.querySelector('input[name="v_primers_fname"]');
    const vFilled = vbox && vbox.value.trim().length > 0;
    const aux = (window.__SESSION_STATE__ && window.__SESSION_STATE__.aux) || {};
    const ok = vFilled || !!aux?.v_primers;
    return ok ? {ok:true,msg:'MaskPrimers primers: OK'} : {ok:false,msg:'MaskPrimers needs V primers (upload aux or fill filename).'};
  }
  return {ok:true,msg:'MaskPrimers extract: OK'};
}
function validateAssemble(step){
  const s = window.__SESSION_STATE__ || {};
  const needBoth = !!(step.unit.startsWith('assemble_'));
  if(needBoth){
    const haveR1 = !!(s.current && s.current.R1);
    const haveR2 = !!(s.current && s.current.R2);
    if(!(haveR1 && haveR2)){
      return {ok:false,msg:'AssemblePairs likely needs both R1 and R2 present (or run PairSeq).'};
    }
  }
  return {ok:true,msg:'AssemblePairs: basic check passed'};
}
function validateConsensus(step){
  return {ok:true,msg:'BuildConsensus: ensure BARCODE exists (MaskPrimers extract tag).'};
}

function validatePipeline(){
  const dagApi = window.BulkDag;
  if(dagApi?.hasNodes?.()){
    validateDagPipeline(dagApi);
    return;
  }
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
    if(st.unit === 'mask_primers') res = validateMaskPrimers(st);
    else if(st.unit.startsWith('assemble_')) res = validateAssemble(st);
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
    const ok = await runUnit(s.card, s.unit);
    setProgress(i+1, bulkSteps.length);
    if(!ok){ setRunStatus(`Failed at <b>${esc(s.label)}</b> (${i+1}/${bulkSteps.length})`); pipeMsg('Pipeline failed','err'); return; }
  }
  setRunStatus('Finished ✅'); pipeMsg('Pipeline finished','ok');
}


async function runPipeline(){
  if(hasDagPipeline()){
    await runDagPipeline();
  } else {
    await runLinearPipeline();
  }
}

async function runDagPipeline(){
  const dag = window.BulkDag;
  if(!dag){
    pipeMsg('DAG runtime unavailable','err');
    return;
  }
  const nodes = dag.getNodes?.() || [];
  if(nodes.length === 0){
    pipeMsg('Add nodes to the DAG first','warn');
    $('#validation').innerHTML = '<span class="warn">DAG is empty.</span>';
    return;
  }
  const order = dag.topoOrder?.();
  if(!order){
    pipeMsg('Cycle detected in DAG','err');
    $('#validation').innerHTML = '<div class="err">Cycle detected. Fix connections.</div>';
    return;
  }
  dag.resetStatuses?.();
  const edges = dag.getEdges?.() || [];
  const incoming = {};
  const dependents = {};
  edges.forEach(edge => {
    if(!incoming[edge.to]) incoming[edge.to] = [];
    incoming[edge.to].push(edge.from);
    if(!dependents[edge.from]) dependents[edge.from] = [];
    dependents[edge.from].push(edge.to);
  });
  const nodeMap = {};
  nodes.forEach(n => { nodeMap[n.id] = n; });
  const indegree = {};
  nodes.forEach(n => indegree[n.id] = (incoming[n.id] || []).length);
  const branchTotals = {};
  const branchCompleted = {};
  DAG_BRANCH_KEYS.forEach(branch => {
    branchTotals[branch] = 0;
    branchCompleted[branch] = 0;
  });
  nodes.forEach(node => {
    const key = (node.branch || '').toUpperCase();
    if(DAG_BRANCH_KEYS.includes(key)){
      branchTotals[key] = (branchTotals[key] || 0) + 1;
    }
  });
  DAG_BRANCH_KEYS.forEach(branch => setBranchProgress(branch, 0, branchTotals[branch]));

  const ready = Object.entries(indegree).filter(([,deg]) => deg === 0).map(([id]) => id);
  if(!ready.length){
    pipeMsg('No runnable nodes in DAG','err');
    return;
  }

  setRunStatus('Starting DAG pipeline');
  pipeMsg('Running DAG pipeline','muted');
  const total = nodes.length;
  setProgress(0, total);
  let completed = 0;
  const succeeded = new Set();
  const failed = new Set();
  const scheduled = new Set();
  let aborted = false;
  const activePromises = new Set();

  const recordBranchSuccess = (branchKey) => {
    if(!branchKey || !branchTotals[branchKey]) return;
    branchCompleted[branchKey] = Math.min(branchTotals[branchKey], (branchCompleted[branchKey] || 0) + 1);
    setBranchProgress(branchKey, branchCompleted[branchKey], branchTotals[branchKey]);
  };
  const markBranchState = (branchKey, state) => {
    if(!branchKey || !(branchKey in branchTotals)) return;
    setBranchProgress(branchKey, branchCompleted[branchKey] || 0, branchTotals[branchKey] || 0, state);
  };

  const scheduleNode = (nodeId) => {
    if(aborted || scheduled.has(nodeId) || failed.has(nodeId)) return;
    const node = nodeMap[nodeId];
    if(!node) return;
    scheduled.add(nodeId);
    const runnerPromise = (async () => {
      dag.setNodeStatus?.(nodeId,'running');
      setRunStatus(`Running <b>${esc(node.label)}</b> (${completed+1}/${total})`);
      const result = await runDagNode(node);
      if(result.ok){
        completed++;
        succeeded.add(nodeId);
        dag.setNodeStatus?.(nodeId,'done');
        setProgress(completed, total);
        const current = window.__SESSION_STATE__?.current || {};
        dag.recordChannelArtifacts?.(nodeId, current);
        const branchKey = (node.branch || '').toUpperCase();
        recordBranchSuccess(branchKey);
        (dependents[nodeId] || []).forEach(depId => {
          if(failed.has(depId)) return;
          indegree[depId] = Math.max(0, (indegree[depId] ?? 0) - 1);
          if(indegree[depId] === 0){
            scheduleNode(depId);
          }
        });
      } else {
        failed.add(nodeId);
        aborted = true;
        dag.setNodeStatus?.(nodeId,'error');
        pipeMsg(`Node ${node.label} failed: ${result.error || 'see log'}`,'err');
        setRunStatus(`Failed at <b>${esc(node.label)}</b>`);
        const branchKey = (node.branch || '').toUpperCase();
        markBranchState(branchKey, 'error');
      }
    })();
    activePromises.add(runnerPromise);
    runnerPromise.finally(() => activePromises.delete(runnerPromise));
  };

  ready.forEach(scheduleNode);

  while(activePromises.size){
    await Promise.all(Array.from(activePromises));
  }

  if(failed.size){
    const failedBranches = new Set();
    nodes.forEach(node => {
      if(!failed.has(node.id) && !succeeded.has(node.id)){
        dag.setNodeStatus?.(node.id,'blocked');
        const branchKey = (node.branch || '').toUpperCase();
        if(DAG_BRANCH_KEYS.includes(branchKey)){
          markBranchState(branchKey, 'blocked');
        }
      } else if(failed.has(node.id)){
        const branchKey = (node.branch || '').toUpperCase();
        if(DAG_BRANCH_KEYS.includes(branchKey)){
          failedBranches.add(branchKey);
        }
      }
    });
    failedBranches.forEach(branch => markBranchState(branch, 'error'));
    return;
  }
  pipeMsg('DAG pipeline finished','ok');
  setRunStatus('Finished DAG pipeline');
}

/* ===== Wire up ===== */
document.addEventListener('DOMContentLoaded', () => {
  startSession();
  $('#upload')?.addEventListener('click', uploadReads);
  $('#upload-aux')?.addEventListener('click', uploadAux);
  $('#pipe-validate')?.addEventListener('click', validatePipeline);
  $('#pipe-run')?.addEventListener('click', runPipeline);
  $('#pipe-clear')?.addEventListener('click', ()=>{
    $$('.pipe-add').forEach(c=>c.checked=false);
    PIPELINE = [];
    drawFlow(); $('#validation').textContent='—'; pipeMsg('Pipeline cleared');
    resetBranchProgress();
  });
});
