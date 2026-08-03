const OUTCOMES = [
  "success",
  "timeout_after_effect",
  "slow",
  "fail_before",
  "custom_ok",
  "custom_fail",
];
const CLASSES = [
  "read",
  "idempotent_mutate",
  "keyed_mutate",
  "non_idempotent_mutate",
  "irreversible",
];
const DEFAULT_CLASS = {
  search_docs: "read",
  lookup_order: "read",
  set_status: "idempotent_mutate",
  charge_keyed: "keyed_mutate",
  charge: "non_idempotent_mutate",
  refund: "non_idempotent_mutate",
  send_email: "non_idempotent_mutate",
  ship_order: "non_idempotent_mutate",
  create_ticket: "non_idempotent_mutate",
  post_slack: "non_idempotent_mutate",
  delete_account: "irreversible",
};

const state = {
  nodes: [], // {id, tool, x, y, outcome, side_effect_class, tool_call_id}
  edges: [], // {from, to}
  seq: 1,
  connectFrom: null,
  drag: null,
  wire: null, // { fromId, x2, y2 } while dragging a connection
};

function wouldCreateCycle(fromId, toId) {
  // Walk forward from toId; if we reach fromId, from→to would loop.
  const adj = {};
  for (const e of state.edges) {
    (adj[e.from] || (adj[e.from] = [])).push(e.to);
  }
  const q = [toId];
  const seen = new Set();
  while (q.length) {
    const id = q.shift();
    if (id === fromId) return true;
    if (seen.has(id)) continue;
    seen.add(id);
    for (const nxt of adj[id] || []) q.push(nxt);
  }
  return false;
}

function connectNodes(fromId, toId) {
  if (!fromId || !toId || fromId === toId) return false;
  if (!nodeById(fromId) || !nodeById(toId)) return false;
  if (wouldCreateCycle(fromId, toId)) return false;
  // One out + one in per node → simple agent pipeline (no fan-out triangles)
  state.edges = state.edges.filter((e) => e.to !== toId && e.from !== fromId);
  state.edges.push({ from: fromId, to: toId });
  state.connectFrom = null;
  state.wire = null;
  connectHint.hidden = true;
  return true;
}

function cancelConnect() {
  state.connectFrom = null;
  state.wire = null;
  connectHint.hidden = true;
  canvas.querySelectorAll(".node.selected, .port.hot").forEach((el) => {
    el.classList.remove("selected", "hot");
  });
  drawEdges();
}

function canvasPoint(clientX, clientY) {
  const rect = canvas.getBoundingClientRect();
  return {
    x: clientX - rect.left,
    y: clientY - rect.top,
  };
}

function nodeIdFromPoint(clientX, clientY) {
  const el = document.elementFromPoint(clientX, clientY);
  const node = el?.closest?.(".node");
  return node?.dataset?.id || null;
}

const canvas = document.getElementById("canvas");
const edgesSvg = document.getElementById("edges");
const emptyEl = document.getElementById("canvasEmpty");
const connectHint = document.getElementById("connectHint");

function uid(prefix) {
  return `${prefix}_${state.seq++}`;
}

function addNode(tool, x = 80, y = 80) {
  const id = uid("n");
  state.nodes.push({
    id,
    tool,
    x,
    y,
    outcome: "success",
    side_effect_class: DEFAULT_CLASS[tool] || "non_idempotent_mutate",
    tool_call_id: uid("call"),
  });
  render();
  return id;
}

function removeNode(id) {
  state.nodes = state.nodes.filter((n) => n.id !== id);
  state.edges = state.edges.filter((e) => e.from !== id && e.to !== id);
  render();
}

function nodeById(id) {
  return state.nodes.find((n) => n.id === id);
}

function topoPlan() {
  const ids = state.nodes.map((n) => n.id);
  const indeg = Object.fromEntries(ids.map((id) => [id, 0]));
  const adj = Object.fromEntries(ids.map((id) => [id, []]));
  for (const e of state.edges) {
    if (!adj[e.from] || indeg[e.to] === undefined) continue;
    adj[e.from].push(e.to);
    indeg[e.to] += 1;
  }
  const q = ids.filter((id) => indeg[id] === 0);
  // stable: left-to-right among roots
  q.sort((a, b) => nodeById(a).x - nodeById(b).x);
  const ordered = [];
  while (q.length) {
    const id = q.shift();
    ordered.push(id);
    for (const nxt of adj[id]) {
      indeg[nxt] -= 1;
      if (indeg[nxt] === 0) {
        q.push(nxt);
        q.sort((a, b) => nodeById(a).x - nodeById(b).x);
      }
    }
  }
  // cycles / leftovers: append by x
  if (ordered.length < ids.length) {
    const left = ids
      .filter((id) => !ordered.includes(id))
      .sort((a, b) => nodeById(a).x - nodeById(b).x);
    ordered.push(...left);
  }
  return ordered.map((id) => {
    const n = nodeById(id);
    return {
      id: n.id,
      tool: n.tool,
      tool_call_id: n.tool_call_id,
      outcome: n.outcome,
      side_effect_class: n.side_effect_class,
    };
  });
}

/** Visual center of a port, in SVG/canvas pixel space. */
function portCenter(nodeEl, side) {
  const canvasRect = canvas.getBoundingClientRect();
  const port = nodeEl.querySelector(`.port.${side}`);
  if (port) {
    const r = port.getBoundingClientRect();
    return {
      x: r.left + r.width / 2 - canvasRect.left,
      y: r.top + r.height / 2 - canvasRect.top,
    };
  }
  const n = nodeEl.getBoundingClientRect();
  return {
    x: (side === "out" ? n.right : n.left) - canvasRect.left,
    y: n.top + n.height / 2 - canvasRect.top,
  };
}

function drawEdges() {
  const canvasRect = canvas.getBoundingClientRect();
  const w = Math.max(canvasRect.width, 1);
  const h = Math.max(canvasRect.height, 1);
  // Keep SVG pixel space identical to getBoundingClientRect space
  edgesSvg.setAttribute("width", String(w));
  edgesSvg.setAttribute("height", String(h));
  edgesSvg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  edgesSvg.setAttribute("preserveAspectRatio", "none");
  edgesSvg.style.width = `${w}px`;
  edgesSvg.style.height = `${h}px`;
  edgesSvg.innerHTML = "";

  for (const e of state.edges) {
    const a = canvas.querySelector(`.node[data-id="${e.from}"]`);
    const b = canvas.querySelector(`.node[data-id="${e.to}"]`);
    if (!a || !b) continue;
    const p1 = portCenter(a, "out");
    const p2 = portCenter(b, "in");
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(p1.x));
    line.setAttribute("y1", String(p1.y));
    line.setAttribute("x2", String(p2.x));
    line.setAttribute("y2", String(p2.y));
    edgesSvg.appendChild(line);
  }
  if (state.wire) {
    const a = canvas.querySelector(`.node[data-id="${state.wire.fromId}"]`);
    if (a) {
      const p1 = portCenter(a, "out");
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", String(p1.x));
      line.setAttribute("y1", String(p1.y));
      line.setAttribute("x2", String(state.wire.x2));
      line.setAttribute("y2", String(state.wire.y2));
      line.classList.add("wire-temp");
      edgesSvg.appendChild(line);
    }
  }
}

function render() {
  emptyEl.hidden = state.nodes.length > 0;
  // remove old nodes
  canvas.querySelectorAll(".node").forEach((n) => n.remove());

  for (const n of state.nodes) {
    const node = document.createElement("div");
    node.className = "node";
    node.dataset.id = n.id;
    node.style.left = `${n.x}px`;
    node.style.top = `${n.y}px`;

    const head = document.createElement("div");
    head.className = "node-head";
    head.innerHTML = `<strong>${n.tool}</strong>`;
    const del = document.createElement("button");
    del.type = "button";
    del.className = "node-del";
    del.title = "Remove";
    del.textContent = "×";
    del.addEventListener("click", (ev) => {
      ev.stopPropagation();
      removeNode(n.id);
    });
    head.appendChild(del);

    head.addEventListener("pointerdown", (ev) => {
      if (ev.target.closest("button, select")) return;
      const startX = ev.clientX;
      const startY = ev.clientY;
      const origX = n.x;
      const origY = n.y;
      state.drag = { id: n.id, startX, startY, origX, origY };
      head.setPointerCapture(ev.pointerId);
    });
    head.addEventListener("pointermove", (ev) => {
      if (!state.drag || state.drag.id !== n.id) return;
      n.x = Math.max(8, state.drag.origX + (ev.clientX - state.drag.startX));
      n.y = Math.max(8, state.drag.origY + (ev.clientY - state.drag.startY));
      node.style.left = `${n.x}px`;
      node.style.top = `${n.y}px`;
      drawEdges();
    });
    head.addEventListener("pointerup", () => {
      state.drag = null;
    });

    const outLabel = document.createElement("label");
    outLabel.textContent = "outcome (free)";
    const outSel = document.createElement("select");
    for (const o of OUTCOMES) {
      const opt = document.createElement("option");
      opt.value = o;
      opt.textContent = o;
      if (o === n.outcome) opt.selected = true;
      outSel.appendChild(opt);
    }
    outSel.addEventListener("change", () => {
      n.outcome = outSel.value;
    });

    const classLabel = document.createElement("label");
    classLabel.textContent = "side_effect_class";
    const classSel = document.createElement("select");
    for (const c of CLASSES) {
      const opt = document.createElement("option");
      opt.value = c;
      opt.textContent = c;
      if (c === n.side_effect_class) opt.selected = true;
      classSel.appendChild(opt);
    }
    classSel.addEventListener("change", () => {
      n.side_effect_class = classSel.value;
    });

    const pin = document.createElement("div");
    pin.className = "port in";
    pin.title = "Drop a connection here (left port)";
    pin.addEventListener("pointerup", (ev) => {
      ev.stopPropagation();
      if (!state.connectFrom || state.connectFrom === n.id) return;
      if (connectNodes(state.connectFrom, n.id)) render();
    });
    pin.addEventListener("click", (ev) => {
      ev.stopPropagation();
      if (!state.connectFrom || state.connectFrom === n.id) return;
      if (connectNodes(state.connectFrom, n.id)) render();
    });

    const pout = document.createElement("div");
    pout.className = "port out";
    pout.title = "Drag to another tool to connect (right port)";
    pout.addEventListener("pointerdown", (ev) => {
      ev.stopPropagation();
      ev.preventDefault();
      state.connectFrom = n.id;
      connectHint.hidden = false;
      const pt = canvasPoint(ev.clientX, ev.clientY);
      state.wire = { fromId: n.id, x2: pt.x, y2: pt.y };
      canvas.querySelectorAll(".node").forEach((el) => {
        el.classList.toggle("selected", el.dataset.id === n.id);
      });
      pout.classList.add("hot");
      pout.setPointerCapture(ev.pointerId);
      drawEdges();
    });
    pout.addEventListener("pointermove", (ev) => {
      if (state.connectFrom !== n.id || !state.wire) return;
      const pt = canvasPoint(ev.clientX, ev.clientY);
      state.wire.x2 = pt.x;
      state.wire.y2 = pt.y;
      const overId = nodeIdFromPoint(ev.clientX, ev.clientY);
      canvas.querySelectorAll(".port.in").forEach((el) => {
        const id = el.closest(".node")?.dataset?.id;
        el.classList.toggle("hot", Boolean(overId && id === overId && id !== n.id));
      });
      drawEdges();
    });
    pout.addEventListener("pointerup", (ev) => {
      ev.stopPropagation();
      if (state.connectFrom !== n.id) return;
      const toId = nodeIdFromPoint(ev.clientX, ev.clientY);
      if (toId && toId !== n.id && connectNodes(n.id, toId)) {
        render();
        return;
      }
      // Keep click-to-connect armed if they released on empty canvas
      state.wire = null;
      drawEdges();
    });

    // Clicking the card while a wire is armed finishes the link
    node.addEventListener("click", (ev) => {
      if (ev.target.closest("button, select, .port")) return;
      if (!state.connectFrom || state.connectFrom === n.id) return;
      if (connectNodes(state.connectFrom, n.id)) render();
    });

    node.append(head, outLabel, outSel, classLabel, classSel, pin, pout);
    canvas.appendChild(node);
  }
  requestAnimationFrame(drawEdges);
}

function formatResult(r) {
  const lines = [];
  if (r.gate) lines.push(`gate: ${r.gate}`);
  lines.push(`executions: ${JSON.stringify(r.executions)}`);
  if (r.error) lines.push(`error: ${r.error}`);
  lines.push("");
  for (const e of r.events || []) {
    const call = e.detail?.call ? ` @${e.detail.call}` : "";
    lines.push(`[${e.kind}] ${e.message}${call}`);
  }
  return lines.join("\n") || "-";
}

function money(amount) {
  const n = Number(amount);
  if (!Number.isFinite(n)) return "$10";
  return Number.isInteger(n) ? `$${n}` : `$${n.toFixed(2)}`;
}

function timesWord(n) {
  if (n === 1) return "once";
  if (n === 2) return "twice";
  return `${n} times`;
}

const EFFECT_PRIORITY = [
  "delete_account",
  "charge",
  "charge_keyed",
  "refund",
  "ship_order",
  "create_ticket",
  "post_slack",
  "send_email",
  "set_status",
  "search_docs",
  "lookup_order",
];

function primaryEffect(plan, executions) {
  const exec = executions || {};
  for (const tool of EFFECT_PRIORITY) {
    const count = exec[tool] || 0;
    if (count > 0) {
      const step = (plan || []).find((p) => p.tool === tool);
      return {
        kind: tool,
        count,
        amount: step?.amount ?? 10,
        order_id: step?.order_id || "ord_1001",
        channel: step?.channel || "#ops",
      };
    }
  }
  const first = (plan || [])[0];
  if (first) {
    return {
      kind: first.tool,
      count: 0,
      amount: first.amount ?? 10,
      order_id: first.order_id || "ord_1001",
      channel: first.channel || "#ops",
    };
  }
  return { kind: "generic", count: 0 };
}

function injectorSub(inj, noun) {
  if (inj === "redispatch") return `A retry ran ${noun} again.`;
  if (inj === "crash_hard_block") return `After a crash, a blind retry ran ${noun} again.`;
  if (inj === "peer_slow") return `Two agent workers both completed ${noun}.`;
  return `The same effect ran more than once.`;
}

function guardedSub(inj, g, good) {
  if (inj === "redispatch" || g.includes("RETURN")) {
    return `Mycelium recognized the duplicate retry and returned the first result - ${good}.`;
  }
  if (inj === "peer_slow" || g.includes("POLL")) {
    return `A second worker tried the same action; Mycelium made it wait instead.`;
  }
  if (inj === "crash_hard_block" || g.includes("HARD_BLOCK")) {
    return `After a crash, Mycelium blocked a blind retry - ${good}.`;
  }
  return `Mycelium prevented a duplicate - ${good}.`;
}

/** Plain-English outcome for non-technical readers. */
function humanStory({ guarded, executions, gate, injector, plan, error }) {
  const effect = primaryEffect(plan, executions || {});
  const inj = injector || "none";
  const g = String(gate || "");
  const n = effect.count || 0;

  if (error && !n) {
    return {
      badge: guarded ? "Blocked" : "Failed",
      lead: guarded
        ? "Mycelium stopped an unsafe retry."
        : "The agent hit an error and may have left side effects half-done.",
      sub: String(error),
    };
  }

  const stories = {
    charge: {
      noun: "the payment",
      badBadge: "Customer overcharged",
      badLead: (c) => `The card was charged ${money(effect.amount)} ${timesWord(c)}.`,
      goodBadge: "Charged once - safe",
      goodLead: () => `The card was charged ${money(effect.amount)} only once.`,
      goodExtra: "no second debit",
      onceLead: () => `The card was charged ${money(effect.amount)} once.`,
    },
    refund: {
      noun: "the refund",
      badBadge: "Double refund",
      badLead: (c) =>
        `The customer was refunded ${money(effect.amount)} ${timesWord(c)}.`,
      goodBadge: "Refunded once - safe",
      goodLead: () => `Refunded ${money(effect.amount)} only once.`,
      goodExtra: "no second payout",
      onceLead: () => `Refunded ${money(effect.amount)} once.`,
    },
    ship_order: {
      noun: "the shipment",
      badBadge: "Shipped twice",
      badLead: (c) =>
        `Order ${effect.order_id} was shipped ${timesWord(c)} - duplicate packages.`,
      goodBadge: "Shipped once - safe",
      goodLead: () => `Order ${effect.order_id} shipped only once.`,
      goodExtra: "no second box left the warehouse",
      onceLead: () => `Order ${effect.order_id} shipped once.`,
    },
    create_ticket: {
      noun: "ticket creation",
      badBadge: "Duplicate tickets",
      badLead: (c) => `The same support ticket was opened ${timesWord(c)}.`,
      goodBadge: "One ticket - safe",
      goodLead: () => "Only one support ticket was created.",
      goodExtra: "no duplicate ticket spam",
      onceLead: () => "One support ticket was created.",
    },
    post_slack: {
      noun: "the Slack post",
      badBadge: "Channel spammed",
      badLead: (c) =>
        `${effect.channel} got the same alert ${timesWord(c)}.`,
      goodBadge: "Posted once - safe",
      goodLead: () => `Posted to ${effect.channel} only once.`,
      goodExtra: "no duplicate noise",
      onceLead: () => `Posted to ${effect.channel} once.`,
    },
    send_email: {
      noun: "the email",
      badBadge: "Duplicate email",
      badLead: (c) => `The same email was sent ${timesWord(c)}.`,
      goodBadge: "Email sent once",
      goodLead: () => "The email went out only once.",
      goodExtra: "no duplicate send",
      onceLead: () => "The email was sent once.",
    },
    set_status: {
      noun: "the status write",
      badBadge: "Status written again",
      badLead: (c) => `Order status was written ${timesWord(c)}.`,
      goodBadge: "Status write guarded",
      goodLead: () => "Status was applied once (safe to repeat).",
      goodExtra: "no conflicting writes from a blind retry",
      onceLead: () => "Order status was updated once.",
    },
    charge_keyed: {
      noun: "the keyed payment",
      badBadge: "Keyed charge retried",
      badLead: (c) =>
        `Payment with the same key ran ${timesWord(c)} - provider key should collapse this.`,
      goodBadge: "Keyed charge once",
      goodLead: () => "Charged once under the idempotency key.",
      goodExtra: "same key did not debit again",
      onceLead: () => "Keyed payment ran once.",
    },
    delete_account: {
      noun: "account deletion",
      badBadge: "Delete ran again",
      badLead: (c) =>
        `Account purge ran ${timesWord(c)} - irreversible work should not retry blind.`,
      goodBadge: "Delete guarded",
      goodLead: () => "Account delete happened once - retry was blocked.",
      goodExtra: "no second irreversible purge",
      onceLead: () => "Account delete ran once.",
    },
    search_docs: {
      read: true,
      label: "Docs search",
    },
    lookup_order: {
      read: true,
      label: "Order lookup",
    },
  };

  const s = stories[effect.kind];
  if (s?.read) {
    return {
      badge: guarded ? "Read allowed" : "Lookup ran",
      lead: `${s.label} ran ${timesWord(n || 1)}.`,
      sub: "Reads are usually safe to repeat - charges, refunds, and shipments are not.",
    };
  }

  if (s) {
    if (!guarded) {
      if (n >= 2) {
        return {
          badge: s.badBadge,
          lead: s.badLead(n),
          sub: injectorSub(inj, s.noun),
        };
      }
      return {
        badge: `${effect.kind} once (unguarded)`,
        lead: s.onceLead(),
        sub: "Nothing stopped a duplicate if the agent retried.",
      };
    }
    if (n === 1 && /RETURN|POLL|HARD_BLOCK|PROTECTED/i.test(g + inj)) {
      return {
        badge: s.goodBadge,
        lead: s.goodLead(),
        sub: guardedSub(inj, g, s.goodExtra),
      };
    }
    if (n >= 2) {
      return {
        badge: "Unexpected duplicate",
        lead: s.badLead(n),
        sub: "That shouldn’t happen under Mycelium - check the technical log.",
      };
    }
    if (n === 0 && g.includes("HARD_BLOCK")) {
      return {
        badge: "Retry blocked",
        lead: "No second side effect went through.",
        sub: "Mycelium refused an unsafe retry after a crash.",
      };
    }
    return {
      badge: "Protected",
      lead: n ? s.onceLead() : "No duplicate side effect.",
      sub: g ? `Gate: ${g}` : "Mycelium guarded this side effect.",
    };
  }

  return {
    badge: guarded ? "With Mycelium" : "Without Mycelium",
    lead: "Run finished.",
    sub: g ? `Gate: ${g}` : "See technical detail for the event log.",
  };
}

function renderStory(elId, story) {
  const el = document.getElementById(elId);
  el.innerHTML = "";
  const badge = document.createElement("div");
  badge.className = "story-badge";
  badge.textContent = story.badge;
  const lead = document.createElement("p");
  lead.className = "story-lead";
  lead.textContent = story.lead;
  const sub = document.createElement("p");
  sub.className = "story-sub";
  sub.textContent = story.sub;
  el.append(badge, lead, sub);
}

function clearChips() {
  document.querySelectorAll(".col .chip").forEach((c) => c.remove());
}

function setCmd(elId, text) {
  const el = document.getElementById(elId);
  if (!text) {
    el.hidden = true;
    el.textContent = "";
    return;
  }
  el.hidden = false;
  el.textContent = `$ ${text}`;
}

async function run() {
  const btn = document.getElementById("btnRun");
  const plan = topoPlan();
  if (!plan.length) {
    alert("Add at least one tool node.");
    return;
  }
  const injector = document.getElementById("injector").value;
  btn.disabled = true;
  document.getElementById("compareTitle").textContent = "What happened?";
  document.getElementById("compareHint").textContent =
    "Same agent graph twice - once unprotected, once with Mycelium. Plain English first; open technical detail if you want gates.";
  document.getElementById("titleWithout").textContent = "Without Mycelium";
  document.getElementById("titleWith").textContent = "With Mycelium";
  setCmd("cmdWithout", "");
  setCmd("cmdWith", "");
  clearChips();
  try {
    const res = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        plan,
        tools: [],
        injector,
        mode: "both",
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      document.getElementById("logWithout").textContent = data.detail || "error";
      document.getElementById("logWith").textContent = "-";
      renderStory("storyWithout", {
        badge: "Error",
        lead: "Couldn’t run this scenario.",
        sub: String(data.detail || "error"),
      });
      renderStory("storyWith", {
        badge: "-",
        lead: "No result.",
        sub: "Fix the error on the left and try again.",
      });
      return;
    }
    document.getElementById("yamlBox").textContent = data.yaml_preview || "";
    const without = data.results.find((r) => r.mode === "without");
    const withM = data.results.find((r) => r.mode === "with");
    document.getElementById("logWithout").textContent = without
      ? formatResult(without)
      : "-";
    document.getElementById("logWith").textContent = withM ? formatResult(withM) : "-";

    renderStory(
      "storyWithout",
      humanStory({
        guarded: false,
        executions: without?.executions,
        gate: without?.gate,
        injector,
        plan,
        error: without?.error,
      }),
    );
    renderStory(
      "storyWith",
      humanStory({
        guarded: true,
        executions: withM?.executions,
        gate: withM?.gate,
        injector,
        plan,
        error: withM?.error,
      }),
    );
  } finally {
    btn.disabled = false;
  }
}

// Palette
for (const btn of document.querySelectorAll(".palette-item")) {
  btn.addEventListener("click", () => {
    const tool = btn.dataset.tool;
    const n = state.nodes.length;
    addNode(tool, 60 + (n % 3) * 230, 50 + Math.floor(n / 3) * 200);
  });
  btn.addEventListener("dragstart", (ev) => {
    ev.dataTransfer.setData("text/tool", btn.dataset.tool);
  });
}

canvas.addEventListener("dragover", (ev) => ev.preventDefault());
canvas.addEventListener("drop", (ev) => {
  ev.preventDefault();
  const tool = ev.dataTransfer.getData("text/tool");
  if (!tool) return;
  const rect = canvas.getBoundingClientRect();
  addNode(tool, ev.clientX - rect.left - 100, ev.clientY - rect.top - 20);
});

canvas.addEventListener("click", (ev) => {
  if (ev.target.closest(".node, .port")) return;
  if (state.connectFrom) cancelConnect();
});

window.addEventListener("keydown", (ev) => {
  if (ev.key === "Escape" && state.connectFrom) cancelConnect();
});

document.getElementById("btnRun").addEventListener("click", () => {
  run().catch(console.error);
});

window.addEventListener("resize", () => drawEdges());

// Seed a linear agent: charge → ship_order → send_email (same Y, straight wires)
(function seed() {
  const y = 72;
  const gap = 252;
  const tools = ["charge", "ship_order", "send_email"];
  const ids = tools.map((tool, i) => {
    const id = uid("n");
    state.nodes.push({
      id,
      tool,
      x: 48 + i * gap,
      y,
      outcome: "success",
      side_effect_class: DEFAULT_CLASS[tool] || "non_idempotent_mutate",
      tool_call_id: uid("call"),
    });
    return id;
  });
  state.edges = [
    { from: ids[0], to: ids[1] },
    { from: ids[1], to: ids[2] },
  ];
  render();
  requestAnimationFrame(() => requestAnimationFrame(drawEdges));
})();
