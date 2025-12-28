// Bulk DAG builder for paired-end workflows with interactive editing
const DAG_STATE = {
  nodes: {},
  edges: [],
};
const CHANNEL_ARTIFACTS = {};
const STATUS_LABELS = {
  idle: 'Idle',
  running: 'Running',
  done: 'Done',
  error: 'Error',
  blocked: 'Blocked',
};

const DEFAULT_UNIT_META = {
  filterseq: {
    label: 'FilterSeq quality',
    consumes: ['R1'],
    produces: ['R1'],
    branches: ['R1', 'R2'],
    dynamicBranch: true,
    params: [
      { key: 'min_quality', label: 'Quality >=', type: 'number', default: 20, min: 0, max: 50, step: 1 },
    ],
    inputs: [
      { id: 'read', label: 'Input FASTQ', channels: ['R1','R2'] },
    ],
  },
  maskprimers: {
    label: 'MaskPrimers score',
    consumes: ['R1'],
    produces: ['R1'],
    branches: ['R1', 'R2'],
    dynamicBranch: true,
    params: [
      { key: 'max_error', label: 'Max error', type: 'number', default: 0.3, min: 0, max: 1, step: 0.05 },
    ],
    inputs: [
      { id: 'read', label: 'Input FASTQ', channels: ['R1','R2'] },
    ],
  },
  pairseq: {
    label: 'PairSeq',
    consumes: ['R1', 'R2'],
    produces: ['MERGED'],
    branches: ['MERGED'],
    params: [
      { key: 'coordinate', label: 'Coordinate mode', type: 'select', options: ['sra', 'presto'], default: 'sra' },
    ],
    inputs: [
      { id: 'read1', label: 'Read 1 FASTQ', channel: 'R1' },
      { id: 'read2', label: 'Read 2 FASTQ', channel: 'R2' },
    ],
    branchTarget: 'MERGED',
  },
  buildcons: {
    label: 'BuildConsensus',
    consumes: ['R1'],
    produces: ['R1'],
    branches: ['R1', 'R2'],
    dynamicBranch: true,
    params: [
      { key: 'min_reads', label: 'Min reads', type: 'number', default: 2, min: 1, step: 1 },
    ],
    inputs: [
      { id: 'read', label: 'Input FASTQ', channels: ['R1','R2'] },
    ],
  },
  assemble_pairs: {
    label: 'AssemblePairs sequential',
    consumes: ['MERGED'],
    produces: ['MERGED'],
    branches: ['MERGED'],
    params: [
      { key: 'aligner', label: 'Aligner', type: 'select', options: ['blastn', 'vsearch'], default: 'blastn' },
    ],
    inputs: [
      { id: 'merged', label: 'Merged reads FASTQ', channel: 'MERGED' },
    ],
    branchTarget: 'MERGED',
  },
};
let UNIT_META = {...DEFAULT_UNIT_META};

const branchLabels = {
  R1: 'Read 1 branch',
  R2: 'Read 2 branch',
  MERGED: 'Post-merge',
};

let NODE_COUNTER = 0;

function randomId(){
  return `node_${Math.random().toString(36).slice(2,8)}`;
}

function applyUnitMeta(metaMap){
  if(metaMap && Object.keys(metaMap).length){
    UNIT_META = metaMap;
  } else {
    UNIT_META = {...DEFAULT_UNIT_META};
  }
  renderPalette();
  renderGraph();
}

function defaultParams(meta){
  const params = {};
  (meta.params || []).forEach(p => {
    params[p.key] = p.default ?? '';
  });
  return params;
}

function ensureNodeDefaults(node){
  const meta = UNIT_META[node.unitId];
  if(!meta) return;
  if(!node.params) node.params = defaultParams(meta);
  else (meta.params || []).forEach(p => {
    if(node.params[p.key] === undefined){
      node.params[p.key] = p.default ?? '';
    }
  });
  if(meta.dynamicBranch){
    if(!meta.branches.includes(node.branch)){
      node.branch = meta.branches[0];
    }
    const branchChannel = node.branch === 'R2' ? 'R2' : 'R1';
    node.consumes = [branchChannel];
    node.produces = [branchChannel];
  } else {
    if(!node.consumes || !node.consumes.length){
      node.consumes = meta.consumes.slice();
    }
    if(!node.produces || !node.produces.length){
      node.produces = meta.produces.slice();
    }
  }
}

function cloneNode(node){
  return {
    id: node.id,
    unitId: node.unitId,
    label: node.label,
    branch: node.branch,
    consumes: (node.consumes || []).slice(),
    produces: (node.produces || []).slice(),
    params: {...(node.params || {})},
    status: node.status || 'idle',
    order: node.order ?? 0,
  };
}

function addNode(unitId, branch, options = {}){
  const meta = UNIT_META[unitId];
  if(!meta) return;
  const targetBranch = branch || meta.branchTarget || meta.branches?.[0] || 'R1';
  if(!meta.branches?.includes(targetBranch)){
    meta.branches = Array.from(new Set([...(meta.branches||[]), targetBranch]));
  }
  const existingBranchNodes = Object.values(DAG_STATE.nodes).filter(node => node.branch === targetBranch);
  const id = randomId();
  const baseParams = defaultParams(meta);
  const mergedParams = {...baseParams, ...(options.params || {})};
  const newNode = {
    id,
    unitId,
    label: meta.label,
    branch: targetBranch,
    consumes: meta.consumes.slice(),
    produces: meta.produces.slice(),
    params: mergedParams,
    incoming: [],
    status: 'idle',
    selectedInputs: options.inputs || [],
    order: NODE_COUNTER++,
  };
  DAG_STATE.nodes[id] = newNode;
  ensureNodeDefaults(newNode);
  const previous = existingBranchNodes[existingBranchNodes.length - 1];
  if(previous){
    ensureNodeDefaults(previous);
    connectNodes(previous.id, id);
  }
  renderGraph();
}

function removeNode(nodeId){
  const target = DAG_STATE.nodes[nodeId];
  let upstream = null;
  let downstream = null;
  if(target){
    const branch = target.branch;
    const branchIncoming = (target.incoming || [])
      .map(entry => DAG_STATE.nodes[entry.node])
      .filter(node => node && node.branch === branch)
      .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
    if(branchIncoming.length){
      upstream = branchIncoming[branchIncoming.length - 1];
    }
    const branchOutgoing = DAG_STATE.edges
      .filter(edge => edge.from === nodeId)
      .map(edge => DAG_STATE.nodes[edge.to])
      .filter(node => node && node.branch === branch)
      .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
    if(branchOutgoing.length){
      downstream = branchOutgoing[0];
    }
  }
  delete DAG_STATE.nodes[nodeId];
  DAG_STATE.edges = DAG_STATE.edges.filter(edge => edge.from !== nodeId && edge.to !== nodeId);
  Object.values(DAG_STATE.nodes).forEach(node => {
    node.incoming = node.incoming.filter(entry => entry.node !== nodeId);
  });
  Object.keys(CHANNEL_ARTIFACTS).forEach(channel => {
    if(CHANNEL_ARTIFACTS[channel]?.nodeId === nodeId){
      delete CHANNEL_ARTIFACTS[channel];
    }
  });
  if(upstream && downstream){
    ensureNodeDefaults(upstream);
    ensureNodeDefaults(downstream);
    connectNodes(upstream.id, downstream.id);
  }
  renderGraph();
}

function removeEdge(fromId, toId){
  DAG_STATE.edges = DAG_STATE.edges.filter(edge => !(edge.from === fromId && edge.to === toId));
  const target = DAG_STATE.nodes[toId];
  if(target){
    target.incoming = target.incoming.filter(entry => entry.node !== fromId);
  }
  renderGraph();
}

function connectNodes(fromId, toId){
  if(fromId === toId) return false;
  const source = DAG_STATE.nodes[fromId];
  const target = DAG_STATE.nodes[toId];
  if(!source || !target) return false;
  if(DAG_STATE.edges.some(edge => edge.from === fromId && edge.to === toId)) return false;

  const produced = source.produces || [];
  const targetMeta = UNIT_META[target.unitId] || {};
  const consumes = (target.consumes && target.consumes.length) ? target.consumes : (targetMeta.consumes || []);
  if(!consumes.length){
    alert(`${target.label} does not accept any inputs.`);
    return false;
  }
  const validChannels = produced.filter(ch => consumes.includes(ch));
  if(!validChannels.length){
    alert(`Cannot connect ${source.label} (${produced.join(', ') || 'no outputs'}) to ${target.label}. ${target.label} expects ${consumes.join(', ') || 'no inputs'}.`);
    return false;
  }
  DAG_STATE.edges.push({from: fromId, to: toId, channels: validChannels});

  const incomingMap = new Map(target.incoming.map(entry => [entry.node, entry]));
  incomingMap.set(fromId, {node: fromId, label: source.label, channels: validChannels});
  target.incoming = Array.from(incomingMap.values());
  return true;
}

function renderPalette(){
  const palette = document.querySelector('#bulk-palette');
  if(!palette) return;
  palette.innerHTML = '';
  Object.entries(UNIT_META).forEach(([unitId, meta]) => {
    const card = document.createElement('div');
    card.className = 'bulk-card';
    const title = document.createElement('h4');
    title.textContent = meta.label;
    card.appendChild(title);

    const consumes = document.createElement('div');
    consumes.className = 'muted';
    consumes.textContent = `Consumes: ${meta.consumes.join(', ') || '-'}`;
    card.appendChild(consumes);

    const produces = document.createElement('div');
    produces.className = 'muted';
    produces.textContent = `Produces: ${meta.produces.join(', ') || '-'}`;
    card.appendChild(produces);

    const branchInfo = document.createElement('div');
    branchInfo.className = 'muted';
    branchInfo.textContent = `Branches: ${meta.branches.join(', ')}`;
    card.appendChild(branchInfo);

    const buttonsRow = document.createElement('div');
    buttonsRow.className = 'branch-buttons';
    meta.branches.forEach(branch => {
      const btn = document.createElement('button');
      btn.textContent = `Add to ${branch}`;
      btn.addEventListener('click', () => addNode(unitId, branch));
      buttonsRow.appendChild(btn);
    });
    card.appendChild(buttonsRow);
    palette.appendChild(card);
  });
}

function buildParamField(node, def){
  const wrap = document.createElement('div');
  wrap.className = 'param-field';
  const label = document.createElement('label');
  label.textContent = def.label;
  wrap.appendChild(label);

  let input;
  if(def.type === 'select'){
    input = document.createElement('select');
    (def.options || []).forEach(opt => {
      const option = document.createElement('option');
      if(typeof opt === 'string'){
        option.value = opt;
        option.textContent = opt;
      } else {
        option.value = opt.value;
        option.textContent = opt.label || opt.value;
      }
      input.appendChild(option);
    });
  } else {
    input = document.createElement('input');
    input.type = def.type === 'number' ? 'number' : 'text';
    if(def.step !== undefined) input.step = def.step;
    if(def.min !== undefined) input.min = def.min;
    if(def.max !== undefined) input.max = def.max;
    if(def.placeholder) input.placeholder = def.placeholder;
  }
  input.className = 'param-input';
  input.dataset.param = def.key;
  const currentVal = node.params?.[def.key];
  input.value = currentVal !== undefined ? currentVal : (def.default ?? '');
  const updateValue = () => { node.params[def.key] = input.value; };
  input.addEventListener(def.type === 'select' ? 'change' : 'input', updateValue);
  wrap.appendChild(input);

  if(def.help){
    const hint = document.createElement('small');
    hint.textContent = def.help;
    wrap.appendChild(hint);
  }
  return wrap;
}

function buildParamSection(node, meta){
  if(!meta.params || !meta.params.length) return null;
  const details = document.createElement('details');
  details.className = 'node-params';
  details.open = meta.params.length <= 2;
  const summary = document.createElement('summary');
  summary.textContent = 'Parameters';
  details.appendChild(summary);
  const body = document.createElement('div');
  body.className = 'params-body';
  meta.params.forEach(def => body.appendChild(buildParamField(node, def)));
  details.appendChild(body);
  return details;
}

function renderGraph(){
  const cols = {
    R1: document.querySelector('#graph-r1'),
    R2: document.querySelector('#graph-r2'),
    MERGED: document.querySelector('#graph-merge'),
  };
  Object.entries(cols).forEach(([branch, col]) => {
    if(col){
      col.innerHTML = '';
      const heading = document.createElement('h3');
      heading.textContent = branchLabels[branch];
      col.appendChild(heading);
    }
  });

  Object.values(DAG_STATE.nodes).forEach(node => {
    ensureNodeDefaults(node);
    const meta = UNIT_META[node.unitId] || {};
    const card = document.createElement('div');
    card.className = 'graph-node';
    const mergeInputs = (meta.consumes || []).filter(ch => ch === 'R1' || ch === 'R2').length > 1 || (meta.consumes || []).includes('MERGED');
    if(mergeInputs){
      card.classList.add('merge-node');
    }
    const status = node.status || 'idle';
    card.classList.add(`status-${status}`);

    const head = document.createElement('div');
    head.className = 'node-head';
    const title = document.createElement('div');
    title.className = 'node-title';
    title.textContent = node.label;
    const idSpan = document.createElement('span');
    idSpan.className = 'node-id muted';
    idSpan.textContent = ` (${node.id})`;
    title.appendChild(idSpan);
    const removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.className = 'node-close';
    removeBtn.setAttribute('aria-label', `Remove ${node.label}`);
    removeBtn.innerHTML = '&times;';
    removeBtn.addEventListener('click', () => removeNode(node.id));
    head.appendChild(title);
    head.appendChild(removeBtn);
    card.appendChild(head);

    const statusBadge = document.createElement('div');
    statusBadge.className = `status-badge status-${status}`;
    statusBadge.textContent = STATUS_LABELS[status] || status;
    card.appendChild(statusBadge);

    const bucket = cols[node.branch] || cols.MERGED;
    bucket?.appendChild(card);
  });

  renderEdges();
}

function branchSequence(branch){
  const branchNodes = Object.values(DAG_STATE.nodes)
    .filter(node => node.branch === branch)
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  if(branchNodes.length === 0){
    return [];
  }
  const branchEdges = DAG_STATE.edges.filter(edge => {
    const fromNode = DAG_STATE.nodes[edge.from];
    const toNode = DAG_STATE.nodes[edge.to];
    return fromNode?.branch === branch && toNode?.branch === branch;
  });
  if(branchEdges.length === 0){
    return branchNodes;
  }
  const incomingCount = {};
  branchNodes.forEach(node => incomingCount[node.id] = 0);
  branchEdges.forEach(edge => {
    if(incomingCount[edge.to] !== undefined){
      incomingCount[edge.to]++;
    }
  });
  let current = branchNodes.find(node => incomingCount[node.id] === 0) || branchNodes[0];
  const visited = new Set();
  const sequence = [];
  while(current && !visited.has(current.id)){
    sequence.push(current);
    visited.add(current.id);
    const nextEdge = branchEdges.find(edge => edge.from === current.id);
    current = (nextEdge && DAG_STATE.nodes[nextEdge.to]) || null;
  }
  const leftovers = branchNodes.filter(node => !visited.has(node.id));
  leftovers.forEach(node => sequence.push(node));
  return sequence;
}

function renderEdges(){
  const out = document.querySelector('#graph-edges');
  if(!out) return;
  out.innerHTML = '';

  const connHeader = document.createElement('h4');
  connHeader.textContent = 'Connections';
  out.appendChild(connHeader);

  ['R1', 'R2'].forEach(branch => {
    const row = document.createElement('div');
    row.className = 'edge-row branch-summary';
    const label = document.createElement('span');
    label.className = 'edge-branch-label';
    label.textContent = branchLabels[branch] || branch;
    const summarySpan = document.createElement('span');
    summarySpan.className = 'edge-branch-summary';
    const sequence = branchSequence(branch).map(node => node.label);
    summarySpan.textContent = sequence.length ? sequence.join(' -> ') : 'No nodes';
    row.appendChild(label);
    row.appendChild(summarySpan);
    out.appendChild(row);
  });

  const artHeader = document.createElement('h4');
  artHeader.textContent = 'Artifacts by channel';
  out.appendChild(artHeader);

  const keys = Object.keys(CHANNEL_ARTIFACTS);
  if(keys.length === 0){
    const none = document.createElement('div');
    none.className = 'muted';
    none.textContent = 'Run nodes to capture channel outputs';
    out.appendChild(none);
  } else {
    keys.forEach(channel => {
      const info = CHANNEL_ARTIFACTS[channel];
      const row = document.createElement('div');
      row.className = 'channel-row';
      const left = document.createElement('span');
      left.textContent = channel;
      const right = document.createElement('span');
      const value = document.createElement('span');
      value.textContent = info.value || 'n/a';
      const label = document.createElement('span');
      label.className = 'muted';
      label.textContent = ` (${info.label})`;
      right.appendChild(value);
      right.appendChild(label);
      row.appendChild(left);
      row.appendChild(right);
      out.appendChild(row);
    });
  }
}

function topoOrder(){
  const indegree = {};
  Object.keys(DAG_STATE.nodes).forEach(id => indegree[id] = 0);
  DAG_STATE.edges.forEach(({to}) => { if(indegree[to] !== undefined) indegree[to]++; });
  const queue = Object.keys(indegree).filter(id => indegree[id] === 0);
  const order = [];
  while(queue.length){
    const node = queue.shift();
    order.push(node);
    DAG_STATE.edges.forEach(edge => {
      if(edge.from === node){
        indegree[edge.to]--;
        if(indegree[edge.to] === 0) queue.push(edge.to);
      }
    });
  }
  if(order.length !== Object.keys(DAG_STATE.nodes).length){
    return null;
  }
  return order;
}

function setNodeStatus(id, status){
  if(DAG_STATE.nodes[id]){
    DAG_STATE.nodes[id].status = status;
    renderGraph();
  }
}

function resetStatuses(){
  Object.values(DAG_STATE.nodes).forEach(node => node.status = 'idle');
  renderGraph();
}

function recordChannelArtifacts(nodeId, currentState){
  const node = DAG_STATE.nodes[nodeId];
  if(!node || !currentState) return;
  (node.produces || []).forEach(ch => {
    if(currentState[ch]){
      CHANNEL_ARTIFACTS[ch] = { nodeId, label: node.label, value: currentState[ch] };
    }
  });
  renderEdges();
}

document.addEventListener('DOMContentLoaded', () => {
  setupModeToggle();
  renderPalette();
  renderGraph();
});

window.BulkDag = {
  serialize: () => ({
    nodes: Object.values(DAG_STATE.nodes).map(cloneNode),
    edges: DAG_STATE.edges.map(edge => ({...edge})),
  }),
  topoOrder,
  getNodes: () => Object.values(DAG_STATE.nodes).map(cloneNode),
  getEdges: () => DAG_STATE.edges.map(edge => ({...edge})),
  getNode: id => (DAG_STATE.nodes[id] ? cloneNode(DAG_STATE.nodes[id]) : null),
  hasNodes: () => Object.keys(DAG_STATE.nodes).length > 0,
  setNodeStatus,
  resetStatuses,
  recordChannelArtifacts,
  getChannelArtifacts: () => ({...CHANNEL_ARTIFACTS}),
  loadUnitMeta: applyUnitMeta,
  createNode: (unitId, branch, opts) => addNode(unitId, branch, opts),
  getUnitMeta: id => UNIT_META[id] ? {...UNIT_META[id]} : null,
};

function setupModeToggle(){
  const radios = document.querySelectorAll('input[name="bulk-mode"]');
  if(!radios.length) return;
  const notes = {
    single: 'Single-end: build a linear flow with Add to pipeline.',
    paired: 'Paired-end: use the DAG builder for R1/R2 inputs.',
  };
  const apply = (mode) => {
    const body = document.body;
    if(!body) return;
    body.classList.toggle('mode-single', mode === 'single');
    body.classList.toggle('mode-paired', mode === 'paired');
    window.__BULK_MODE = mode;
    const note = document.getElementById('mode-note');
    if(note){
      note.textContent = notes[mode] || '';
    }
  };
  radios.forEach(radio => {
    radio.addEventListener('change', () => {
      if(radio.checked){
        apply(radio.value);
      }
    });
  });
  const initial = Array.from(radios).find(r => r.checked)?.value || 'single';
  apply(initial);
}
