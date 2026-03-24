const graphElement = document.getElementById("graph");
const nodeDetails = document.getElementById("nodeDetails");
const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const resetBtn = document.getElementById("resetBtn");
const expandBtn = document.getElementById("expandBtn");
const chatInput = document.getElementById("chatInput");
const chatSend = document.getElementById("chatSend");
const chatLog = document.getElementById("chatLog");

let cy = null;
let selectedNodeId = null;

function addChatMessage(text, role) {
  const div = document.createElement("div");
  div.className = `chat-msg ${role}`;
  div.textContent = text;
  chatLog.appendChild(div);
  chatLog.scrollTop = chatLog.scrollHeight;
}

function initGraph() {
  cy = cytoscape({
    container: graphElement,
    elements: [],
    layout: { name: "cose", padding: 30 },
    style: [
      {
        selector: "node",
        style: {
          "background-color": "#c2543d",
          "label": "data(label)",
          "font-size": 10,
          "text-wrap": "wrap",
          "text-max-width": 80,
          "color": "#1d1d1b",
          "text-outline-width": 1,
          "text-outline-color": "#f4f1ec",
        },
      },
      {
        selector: "edge",
        style: {
          "curve-style": "bezier",
          "target-arrow-shape": "triangle",
          "line-color": "#9c8d7f",
          "target-arrow-color": "#9c8d7f",
          "width": 1.2,
        },
      },
      {
        selector: "node[type = 'SalesOrder']",
        style: { "background-color": "#f2a154" },
      },
      {
        selector: "node[type = 'Delivery']",
        style: { "background-color": "#a3b18a" },
      },
      {
        selector: "node[type = 'Billing']",
        style: { "background-color": "#c2543d" },
      },
      {
        selector: "node[type = 'Payment']",
        style: { "background-color": "#5c5470" },
      },
      {
        selector: "node:selected",
        style: { "border-width": 3, "border-color": "#1d1d1b" },
      },
    ],
  });

  cy.on("tap", "node", (evt) => {
    const node = evt.target;
    selectedNodeId = node.id();
    nodeDetails.textContent = JSON.stringify(node.data(), null, 2);
  });
}

function upsertGraph(data) {
  const nodes = data.nodes || [];
  const edges = data.edges || [];

  nodes.forEach((node) => {
    if (cy.getElementById(node.id).length === 0) {
      cy.add({
        group: "nodes",
        data: {
          id: node.id,
          label: node.label || node.id,
          type: node.type,
          props: node.props,
        },
      });
    }
  });

  edges.forEach((edge) => {
    if (cy.getElementById(edge.id).length === 0) {
      cy.add({
        group: "edges",
        data: {
          id: edge.id,
          source: edge.source_id,
          target: edge.target_id,
          type: edge.type,
        },
      });
    }
  });

  cy.layout({ name: "cose", padding: 30, animate: true }).run();
}

async function loadOverview() {
  const resp = await fetch("/api/graph/overview");
  const data = await resp.json();
  upsertGraph(data);
}

async function expandSelected() {
  if (!selectedNodeId) {
    addChatMessage("Select a node to expand.", "system");
    return;
  }
  const resp = await fetch("/api/graph/expand", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ node_id: selectedNodeId, limit: 400 }),
  });
  const data = await resp.json();
  upsertGraph(data);
}

async function searchNodes() {
  const query = searchInput.value.trim();
  if (!query) return;
  const resp = await fetch("/api/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  const data = await resp.json();
  if (!data.nodes.length) {
    addChatMessage("No matching nodes found.", "system");
    return;
  }
  upsertGraph({ nodes: data.nodes, edges: [] });
  const first = data.nodes[0];
  selectedNodeId = first.id;
  const ele = cy.getElementById(first.id);
  cy.animate({ center: { eles: ele }, zoom: 1.2 });
  nodeDetails.textContent = JSON.stringify(first, null, 2);
}

async function sendChat() {
  const message = chatInput.value.trim();
  if (!message) return;
  addChatMessage(message, "user");
  chatInput.value = "";
  const resp = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message }),
  });
  if (!resp.ok) {
    const err = await resp.json();
    addChatMessage(`Error: ${err.detail}`, "system");
    return;
  }
  const data = await resp.json();
  addChatMessage(data.answer, "system");
}

resetBtn.addEventListener("click", () => {
  cy.fit();
});

expandBtn.addEventListener("click", expandSelected);
searchBtn.addEventListener("click", searchNodes);
chatSend.addEventListener("click", sendChat);

chatInput.addEventListener("keydown", (evt) => {
  if (evt.key === "Enter") {
    sendChat();
  }
});

initGraph();
loadOverview();
