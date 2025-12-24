// Enhanced UI logic with right-side pipeline panel and click-ordered pipeline.
let SID = null;
let UNITS_META = [];
let PIPELINE = []; // keeps {unit, label, card} in the order of user clicks
let BULK_DAG = createEmptyBulkDag(); // structured graph (WIP)

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
// This page is BULK only
const UNIT_GROUP = 'bulk';

/* ---------- DAG foundation (for future branched pipelines) ---------- */
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

function createEmptyBulkDag(){
  return { nodes: {}, edges: [] };
}
function dagNodeId(){
  return `node_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`;
}
function unitChannels(unitId){
  return CHANNEL_MAP[unitId] || { consumes: [], produces: [] };
}
function dagAddNode(unitId, options = {}){
  const id = options.id || dagNodeId();
  BULK_DAG.nodes[id] = {
    id,
    unitId,
    label: options.label || unitId,
    params: options.params || {},
    channel: options.channel || null,
    meta: unitChannels(unitId),
  };
  return BULK_DAG.nodes[id];
}
function dagRemoveNode(nodeId){
  delete BULK_DAG.nodes[nodeId];
  BULK_DAG.edges = BULK_DAG.edges.filter(e => e.from !== nodeId && e.to !== nodeId);
}
function dagConnect(fromId, toId, channel = null){
  if(!BULK_DAG.nodes[fromId] || !BULK_DAG.nodes[toId]) return;
  BULK_DAG.edges.push({ from: fromId, to: toId, channel });
}
function dagDisconnect(fromId, toId){
  BULK_DAG.edges = BULK_DAG.edges.filter(e => !(e.from === fromId && e.to === toId));
}
function dagIncoming(nodeId){
  return BULK_DAG.edges.filter(e => e.to === nodeId).map(e => e.from);
}
function dagOutgoing(nodeId){
  return BULK_DAG.edges.filter(e => e.from === nodeId).map(e => e.to);
}
function serializeDag(){
  return {
    nodes: Object.values(BULK_DAG.nodes),
    edges: BULK_DAG.edges.slice(),
  };
}
function hydrateDag(payload){
  BULK_DAG = createEmptyBulkDag();
  if(!payload) return;
  (payload.nodes || []).forEach(node => {
    BULK_DAG.nodes[node.id] = {
      id: node.id,
      unitId: node.unitId,
      label: node.label,
      params: node.params || {},
      channel: node.channel || null,
      meta: unitChannels(node.unitId),
    };
  });
  BULK_DAG.edges = (payload.edges || []).filter(e => BULK_DAG.nodes[e.from] && BULK_DAG.nodes[e.to]);
}
function dagTopoOrder(){
  const indegree = {};
  Object.keys(BULK_DAG.nodes).forEach(id => indegree[id] = 0);
  BULK_DAG.edges.forEach(({to}) => { if(indegree[to] !== undefined) indegree[to]++; });
  const queue = Object.keys(indegree).filter(id => indegree[id] === 0);
  const order = [];
  while(queue.length){
    const node = queue.shift();
    order.push(node);
    dagOutgoing(node).forEach(next => {
      indegree[next]--;
      if(indegree[next] === 0) queue.push(next);
    });
  }
  if(order.length !== Object.keys(BULK_DAG.nodes).length){
    return null; // cycle detected
  }
  return order;
}

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
function unitCategory(id){ return CAT_BY_ID[id] || "Other"; }

function selectedSteps(){
  // Return pipeline in click order, but drop items whose checkbox is no longer checked
  PIPELINE = PIPELINE.filter(s => s.card && s.card.querySelector('.pipe-add')?.checked);
  return [...PIPELINE];
}

function drawFlow(){
  const steps = selectedSteps();
  const flow = $('#flow'); flow.innerHTML = '';
  if(steps.length === 0){ flow.innerHTML = '<span class="muted">no steps selected</span>'; return; }
  steps.forEach((s,i) => {
    const n = document.createElement('div'); n.className = 'node'; n.textContent = s.label;
    flow.appendChild(n);
    if(i < steps.length-1){ const a = document.createElement('div'); a.className = 'arrow'; a.textContent = '→'; flow.appendChild(a); }
  });
}

function pipeMsg(text, cls='muted'){ const p = $('#pipe-msg'); p.className = cls; p.textContent = text; }
function setRunStatus(text){ $('#run-status').innerHTML = text; }
function setProgress(i, n){ const pct = n ? Math.round((i/n)*100) : 0; $('#run-bar').style.width = pct + '%'; }
function hasDagPipeline(){
  const isPaired = (window.__BULK_MODE || '').toLowerCase() === 'paired' || document.body.classList.contains('mode-paired');
  const api = window.BulkDag;
  return isPaired && !!(api && typeof api.hasNodes === 'function' && api.hasNodes());
}

async function startSession(){
  const r = await fetch('/session/start',{method:'POST'});
  const j = await r.json();
  SID = j.session_id; $('#sid').textContent = SID;
  PIPELINE = []; // reset
  await renderUnits();
  drawFlow();
  await refreshState();
  $('#validation').textContent = '—';
  setRunStatus('—'); setProgress(0,1);
}

async function uploadReads(){
  const r1 = $('#r1f').files[0]; if(!r1){ alert('Choose R1'); return; }
  const fd = new FormData(); fd.append('r1', r1);
  const r2 = $('#r2f').files[0]; if(r2) fd.append('r2', r2);
  const r = await fetch(`/session/${SID}/upload`, {method:'POST', body:fd});
  if(!r.ok){ alert('Upload failed'); return; }
  await refreshState();
}

async function uploadAux(){
  const f = $('#auxf').files[0]; if(!f){ alert('Choose file'); return; }
  const fd = new FormData(); fd.append('file', f);
  const name = $('#auxname').value.trim(); if(name) fd.append('name', name);
  const r = await fetch(`/session/${SID}/upload-aux`, {method:'POST', body:fd});
  const j = await r.json();
  $('#aux-out').textContent = `Stored as: ${j.stored_as}` + (j.role && j.role!=='other' ? ` (auto as ${j.role})` : '');
  // auto-fill in MaskPrimers cards
  if(j.role === 'v_primers' || j.role === 'other'){
    $$('.card[data-unit="mask_primers"] input[name="v_primers_fname"]').forEach(el => { if(!el.value) el.value = j.stored_as; });
  }
  if(j.role === 'c_primers'){
    $$('.card[data-unit="mask_primers"] input[name="c_primers_fname"]').forEach(el => { if(!el.value) el.value = j.stored_as; });
  }
}

function collectParams(card){
  const params = {};
  card.querySelectorAll('input,select,textarea').forEach(el => {
    if(!el.name) return;
    if(el.type === 'file') return;
    if(el.classList.contains('pipe-add')) return;
    params[el.name] = (el.type === 'checkbox') ? (el.checked ? 'true' : 'false') : el.value;
  });
  return params;
}

async function runUnit(card, unitId){
  const params = collectParams(card);
  const r = await fetch(`/session/${SID}/run`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ unit_id: unitId, params })
  });
  const j = await r.json();
  if(!r.ok){
    alert(`Error: ${(j.detail && (j.detail.error||j.detail)) || r.statusText}`);
    $('#log').textContent = (j.detail && j.detail.log_tail) ? j.detail.log_tail : '';
    return false;
  }
  await refreshState();
  const stepIdx = j.step.step_index;
  const lr = await fetch(`/session/${SID}/log/${stepIdx}`);
  $('#log').textContent = await lr.text();
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

async function refreshState(){
  const r = await fetch(`/session/${SID}/state`);
  const s = await r.json();
  const chips = Object.entries(s.current||{}).map(([k,v]) => `<span class="chip">${esc(k)}: ${esc(v)}</span>`).join(' ');
  $('#state').innerHTML = chips || '<span class="muted">no state</span>';
  const arts = Object.values(s.artifacts||{}).map(a => `<div>${esc(a.name)} — <a href="/session/${SID}/download/${encodeURIComponent(a.name)}">download</a></div>`).join('');
  $('#arts').innerHTML = arts || '<span class="muted">none</span>';
  window.__SESSION_STATE__ = s;
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
  edges.forEach(edge => {
    if(!incoming[edge.to]) incoming[edge.to] = [];
    incoming[edge.to].push(edge.from);
  });
  const nodeMap = {};
  nodes.forEach(n => { nodeMap[n.id] = n; });

  setRunStatus('Starting DAG pipeline');
  pipeMsg('Running DAG pipeline','muted');
  const total = order.length;
  setProgress(0, total);
  let completed = 0;
  const succeeded = new Set();
  const failed = new Set();

  for(const nodeId of order){
    const node = nodeMap[nodeId];
    if(!node) continue;
    const deps = incoming[nodeId] || [];
    const blocked = deps.some(dep => failed.has(dep));
    if(blocked){
      dag.setNodeStatus?.(nodeId,'blocked');
      continue;
    }
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
    } else {
      failed.add(nodeId);
      dag.setNodeStatus?.(nodeId,'error');
      pipeMsg(`Node ${node.label} failed: ${result.error || 'see log'}`,'err');
      setRunStatus(`Failed at <b>${esc(node.label)}</b>`);
      break;
    }
  }

  if(failed.size){
    order.forEach(nodeId => {
      if(!failed.has(nodeId) && !succeeded.has(nodeId)){
        const deps = incoming[nodeId] || [];
        if(deps.some(dep => failed.has(dep))){
          dag.setNodeStatus?.(nodeId,'blocked');
        }
      }
    });
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
  });
});
