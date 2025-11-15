// Single-Cell page logic with flow builder/validator (module)
let SID = null;
let UNITS_META = [];
let FLOW = []; // [{unitId,label,params}]
let running = false;

const $  = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

// Persist session id (hidden from UI)
function persistSid(id){
  window.__SC_SID = id;                              // quick console access
  try { localStorage.setItem('sc_last_sid', id); } catch (_) {}
  console.info('%c[SC]%c session', 'color:#22d3ee', 'color:inherit', id);
}

// ---- session / uploads -------------------------------------------------------
async function startSession(){
  const r = await fetch('/session/start',{method:'POST'});
  const j = await r.json();
  SID = j.session_id;
  persistSid(SID);

  FLOW = [];
  renderFlow();
  await renderUnits();
  await refreshState();
  $('#log').textContent = '';
  $('#upload-msg').textContent = '';
  $('#uploaded-list').textContent = '';
  $('#validation').textContent = '—';
  $('#pstate').textContent = 'idle';
}

async function uploadSCFiles(){
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
  await refreshState();
  listUploaded(files);
}

function listUploaded(fileList){
  const names = Array.from(fileList).map(f => esc(f.name));
  $('#uploaded-list').innerHTML = names.length ? 'Uploaded: ' + names.join(', ') : '';
}

// ---- units rendering ---------------------------------------------------------
async function renderUnits(){
  const r = await fetch(`/session/${SID}/units`);
  const all = await r.json();
  UNITS_META = all.filter(u => (u.id || '').startsWith('sc_'));
  const wrap = $('#units'); wrap.innerHTML = '';
  if(UNITS_META.length === 0){
    wrap.innerHTML = '<div class="muted">No single-cell units registered yet.</div>';
    return;
  }
  UNITS_META.forEach(u => {
    const card = document.createElement('div');
    card.className = 'card';
    card.dataset.unit = u.id;
    let paramsHTML = '';
    for(const [k,v] of Object.entries(u.params_schema||{}) ){
      const help = v.help ? ` <span class="muted">— ${esc(v.help)}</span>` : '';
      const label = `<label>${esc(k)}${help}</label>`;
      if(v.type === 'select'){
        const opts = (v.options||[]).map(o => `<option value="${esc(o)}" ${o===v.default?'selected':''}>${esc(o)}</option>`).join('');
        paramsHTML += `${label}<select name="${esc(k)}">${opts}</select>`;
      }else{
        const val = v.default ?? ''; const ph = v.placeholder ?? '';
        paramsHTML += `${label}<input name="${esc(k)}" value="${esc(val)}" placeholder="${esc(ph)}">`;
      }
    }
    card.innerHTML = `
      <h3>${esc(u.label)}</h3>
      <div class="mt8">${paramsHTML}</div>
      <div class="row mt8">
        <button class="run">Run</button>
        <button class="secondary addflow">Add to flow</button>
      </div>`;
    card.querySelector('.run').addEventListener('click', ()=>runSingle(card, u.id, u.label));
    card.querySelector('.addflow').addEventListener('click', ()=>addToFlow(card, u.id, u.label));
    wrap.appendChild(card);
  });
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

async function runSingle(card, unitId, label){
  const params = collectParams(card);
  await runUnit({unitId, label, params});
}

// ---- flow builder / renderer -------------------------------------------------
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

// ---- validation --------------------------------------------------------------
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

// ---- run flow ---------------------------------------------------------------
async function runFlow(){
  if(running) return;
  const v = validateFlow();
  if(!v.ok){
    alert('Please fix flow issues and try again.');
    return;
  }
  if(FLOW.length === 0){
    alert('No steps to run.');
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

// ---- state / artifacts ------------------------------------------------------
async function refreshState(){
  const r = await fetch(`/session/${SID}/state`);
  const s = await r.json();
  const chips = Object.entries(s.current||{}).map(([k,v]) => `<span class="chip">${esc(k)}: ${esc(v)}</span>`).join(' ');
  $('#state').innerHTML = chips || '<span class="muted">no state</span>';
  const arts = Object.values(s.artifacts||{}).map(a =>
    `<div>${esc(a.name)} — <a href="/session/${SID}/download/${encodeURIComponent(a.name)}">download</a></div>`
  ).join('');
  $('#arts').innerHTML = arts || '<span class="muted">none</span>';
}

// ---- init -------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
  // auto-start a new session silently (no UI buttons)
  await startSession();

  $('#upload-sc').addEventListener('click', uploadSCFiles);
  $('#validate').addEventListener('click', validateFlow);
  $('#runflow').addEventListener('click', runFlow);
  $('#clearflow').addEventListener('click', ()=>{ FLOW=[]; renderFlow(); $('#validation').textContent='—'; $('#pstate').textContent='idle'; });
});
