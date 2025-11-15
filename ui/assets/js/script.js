// Enhanced UI logic with right-side pipeline panel and click-ordered pipeline.
let SID = null;
let UNITS_META = [];
let PIPELINE = []; // keeps {unit, label, card} in the order of user clicks

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
// This page is BULK only
const UNIT_GROUP = 'bulk';

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

async function refreshState(){
  const r = await fetch(`/session/${SID}/state`);
  const s = await r.json();
  const chips = Object.entries(s.current||{}).map(([k,v]) => `<span class="chip">${esc(k)}: ${esc(v)}</span>`).join(' ');
  $('#state').innerHTML = chips || '<span class="muted">no state</span>';
  const arts = Object.values(s.artifacts||{}).map(a => `<div>${esc(a.name)} — <a href="/session/${SID}/download/${encodeURIComponent(a.name)}">download</a></div>`).join('');
  $('#arts').innerHTML = arts || '<span class="muted">none</span>';
  window.__SESSION_STATE__ = s;
}

async function renderUnits(){
  // Ask the backend for bulk-only units if it supports group filtering.
  // If it doesn't, we still filter on the client.
  let all = [];
  try {
    const res = await fetch(`/session/${SID}/units?group=bulk`);
    all = await res.json();
  } catch (e) {
    // Fallback to unfiltered endpoint, will filter below
    try {
      const res2 = await fetch(`/session/${SID}/units`);
      all = await res2.json();
    } catch (e2) { all = []; }
  }

  // Client-side filter (defensive) to make sure only BULK shows up
  UNITS_META = (all || []).filter(u => {
    const id     = (u.id || '').toLowerCase();
    const label  = (u.label || '').toLowerCase();
    const group  = (u.group || '').toLowerCase(); // if backend sends it

    if (group) return group === 'bulk';          // preferred path
    if (id.startsWith('sc_')) return false;      // legacy naming
    if (label.startsWith('sc:')) return false;   // visible label hint
    return true;                                 // treat everything else as bulk
  });

  const wrap = $('#units'); wrap.innerHTML = '';
  UNITS_META.forEach(u => {
    const card = document.createElement('div');
    card.className = 'card';
    card.dataset.unit = u.id;
    const req = (u.requires||[]).map(x=>`<span class="chip">${x}</span>`).join(' ') || '<span class="muted">none</span>';
    let paramsHTML = '';
    for (const [k,v] of Object.entries(u.params_schema||{})) {
      const help = v.help ? ` <span class="muted">— ${esc(v.help)}</span>` : '';
      const label = `<label>${esc(k)}${help}</label>`;
      if (v.type === 'select') {
        const opts = (v.options||[]).map(o => `<option value="${esc(o)}" ${o===v.default?'selected':''}>${esc(o)}</option>`).join('');
        paramsHTML += `${label}<select name="${esc(k)}">${opts}</select>`;
      } else if (v.type === 'file') {
        paramsHTML += `${label}<input name="${esc(k)}" placeholder="${esc(v.accept||'')}" />` +
                      `<div class="muted">Upload in section 1 → aux; I'll fill this automatically.</div>`;
      } else {
        const val = v.default ?? ''; const ph = v.placeholder ?? '';
        paramsHTML += `${label}<input name="${esc(k)}" value="${esc(val)}" placeholder="${esc(ph)}">`;
      }
    }

    card.innerHTML = `
      <h3>${esc(u.label)}</h3>
      <div class="muted">requires: ${req}</div>
      <div class="mt8">${paramsHTML}</div>
      <div class="row mt8">
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
        // keep click order
        PIPELINE = PIPELINE.filter(s => s.unit !== unitId);
        PIPELINE.push({unit: unitId, label, card});
      } else {
        PIPELINE = PIPELINE.filter(s => s.unit !== unitId);
      }
      drawFlow();
    });

    wrap.appendChild(card);
  });
}

// ===== Validation =====
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

// ===== Run Pipeline =====
async function runPipeline(){
  const steps = selectedSteps();
  // Filter out any single-cell units
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

// ===== Wire up =====
document.addEventListener('DOMContentLoaded', () => {
  $('#start').addEventListener('click', startSession);
  $('#upload').addEventListener('click', uploadReads);
  $('#upload-aux').addEventListener('click', uploadAux);
  $('#pipe-validate').addEventListener('click', validatePipeline);
  $('#pipe-run').addEventListener('click', runPipeline);
  $('#pipe-clear').addEventListener('click', ()=>{
    $$('.pipe-add').forEach(c=>c.checked=false);
    PIPELINE = [];
    drawFlow(); $('#validation').textContent='—'; pipeMsg('Pipeline cleared');
  });
  startSession();
});
