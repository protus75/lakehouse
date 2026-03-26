"""FastAPI service with HTML UI for RAG queries and entry browsing."""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
import sys
sys.path.insert(0, "/workspace")
from dlt.lib.duckdb_reader import get_reader

from query_tabletop_rules import ask, get_toc, DEFAULT_MODEL
app = FastAPI(title="Lakehouse RAG API", version="3.0.0")


# ── API Models ───────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    model: str = DEFAULT_MODEL

class QueryResponse(BaseModel):
    answer: str
    question: str


# ── API Endpoints ────────────────────────────────────────────────

@app.post("/ask", response_model=QueryResponse)
def ask_question(req: QueryRequest):
    try:
        answer = ask(req.question, model=req.model)
        return QueryResponse(answer=answer, question=req.question)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/categories")
def get_categories():
    """Get available ToC sections grouped by book."""
    conn = get_reader()
    rows = conn.execute("""
        SELECT t.toc_id, t.title, t.page_start, t.page_end, t.source_file,
               f.document_title, COUNT(c.chunk_id) as chunk_count
        FROM documents_tabletop_rules.toc t
        LEFT JOIN documents_tabletop_rules.files f ON t.source_file = f.source_file
        LEFT JOIN documents_tabletop_rules.chunks c ON t.toc_id = c.toc_id
        WHERE NOT t.is_excluded
        GROUP BY t.toc_id, t.title, t.page_start, t.page_end, t.source_file, f.document_title
        ORDER BY t.source_file, t.page_start
    """).fetchall()
    conn.close()

    books = {}
    for r in rows:
        book = r[5] or r[4]
        if book not in books:
            books[book] = []
        books[book].append({
            "toc_id": r[0], "title": r[1], "entries": r[6],
        })
    return books


@app.get("/api/entries")
def get_entries(toc_id: int, search: Optional[str] = None):
    """Get entry titles within a ToC section, optionally filtered by search."""
    conn = get_reader()
    query = """
        SELECT DISTINCT entry_title
        FROM documents_tabletop_rules.chunks
        WHERE toc_id = ? AND entry_title IS NOT NULL
    """
    params = [toc_id]
    if search:
        query += " AND LOWER(entry_title) LIKE ?"
        params.append(f"%{search.lower()}%")
    query += " ORDER BY entry_title"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [r[0] for r in rows]


@app.get("/api/entry")
def get_entry(toc_id: int, title: str):
    """Get full content of a specific entry."""
    conn = get_reader()
    rows = conn.execute("""
        SELECT c.entry_title, c.section_title, c.content, c.page_numbers,
               t.title as toc_title, f.document_title
        FROM documents_tabletop_rules.chunks c
        JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
        LEFT JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
        WHERE c.toc_id = ? AND c.entry_title = ?
        ORDER BY c.chunk_id
    """, [toc_id, title]).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Entry not found")

    # Combine chunks, keep longest when duplicates
    chunks = [r[2] for r in rows]
    chunks.sort(key=len, reverse=True)
    kept = []
    for chunk in chunks:
        sig = chunk[:100]
        if not any(sig in k for k in kept):
            kept.append(chunk)
    content = "\n\n".join(kept)

    return {
        "title": rows[0][0],
        "section": rows[0][1],
        "toc_title": rows[0][4],
        "source": rows[0][5],
        "content": content,
    }


@app.get("/api/search")
def search_all(q: str, limit: int = 20):
    """Search entries across all books."""
    conn = get_reader()
    rows = conn.execute("""
        SELECT DISTINCT c.entry_title, c.toc_id, t.title as toc_title, f.document_title
        FROM documents_tabletop_rules.chunks c
        JOIN documents_tabletop_rules.toc t ON c.toc_id = t.toc_id
        LEFT JOIN documents_tabletop_rules.files f ON c.source_file = f.source_file
        WHERE c.entry_title IS NOT NULL
        AND LOWER(c.entry_title) LIKE ?
        ORDER BY c.entry_title
        LIMIT ?
    """, [f"%{q.lower()}%", limit]).fetchall()
    conn.close()
    return [{"title": r[0], "toc_id": r[1], "toc_title": r[2], "source": r[3]} for r in rows]


@app.get("/health")
def health():
    return {"status": "ok"}


# ── Web UI ───────────────────────────────────────────────────────

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Rules Reference</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #1a1a2e; color: #e0e0e0; min-height: 100vh; }

/* Tabs */
.tabs { display: flex; background: #16213e; border-bottom: 2px solid #0f3460; }
.tab { padding: 12px 24px; cursor: pointer; color: #888; font-weight: 600; border-bottom: 2px solid transparent; margin-bottom: -2px; }
.tab.active { color: #e94560; border-bottom-color: #e94560; }

/* Panels */
.panel { display: none; flex: 1; flex-direction: column; overflow: hidden; }
.panel.active { display: flex; }

/* Browse panel */
.browse-layout { display: flex; flex: 1; overflow: hidden; }
.sidebar { width: 280px; background: #16213e; border-right: 1px solid #0f3460; display: flex; flex-direction: column; overflow: hidden; }
.sidebar-search { padding: 10px; border-bottom: 1px solid #0f3460; }
.sidebar-search input { width: 100%; background: #1a1a2e; border: 1px solid #0f3460; border-radius: 6px; color: #e0e0e0; padding: 8px 12px; font-size: 14px; outline: none; }
.sidebar-search input:focus { border-color: #e94560; }
.category-list { overflow-y: auto; flex: 1; }
.category-header { padding: 8px 12px; font-size: 11px; color: #e94560; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; background: #0f3460; position: sticky; top: 0; }
.category-item { padding: 6px 12px; cursor: pointer; font-size: 13px; border-bottom: 1px solid #0f346030; }
.category-item:hover { background: #0f3460; }
.category-item.active { background: #e94560; color: #fff; }
.entry-list { overflow-y: auto; flex: 1; border-top: 1px solid #0f3460; }
.entry-item { padding: 6px 12px; cursor: pointer; font-size: 13px; border-bottom: 1px solid #0f346030; }
.entry-item:hover { background: #0f3460; }
.entry-item.active { background: #e94560; color: #fff; }

.content-area { flex: 1; overflow-y: auto; padding: 20px; }
.content-area h1 { color: #e94560; font-size: 22px; margin-bottom: 4px; }
.content-area .source { color: #888; font-size: 12px; margin-bottom: 16px; }
.content-area .meta-table { width: 100%; border-collapse: collapse; margin: 12px 0; }
.content-area .meta-table td { padding: 4px 10px; border: 1px solid #0f3460; font-size: 13px; }
.content-area .meta-table td:first-child { color: #e94560; font-weight: 600; width: 140px; background: #16213e; }
.content-area .description { margin-top: 16px; line-height: 1.6; white-space: pre-wrap; }
.content-area .empty { color: #666; font-style: italic; margin-top: 40px; text-align: center; }

/* Chat panel */
.chat-container { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; }
.message { max-width: 90%; padding: 12px 16px; border-radius: 12px; line-height: 1.5; font-size: 15px; word-wrap: break-word; }
.message.user { align-self: flex-end; background: #0f3460; color: #fff; border-bottom-right-radius: 4px; }
.message.assistant { align-self: flex-start; background: #16213e; border: 1px solid #0f3460; border-bottom-left-radius: 4px; }
.message.assistant strong { color: #fff; }
.typing { align-self: flex-start; color: #888; font-style: italic; padding: 12px 16px; }
.input-area { background: #16213e; border-top: 1px solid #0f3460; padding: 12px 16px; display: flex; gap: 8px; }
.input-area textarea { flex: 1; background: #1a1a2e; border: 1px solid #0f3460; border-radius: 8px; color: #e0e0e0; padding: 10px 14px; font-size: 15px; font-family: inherit; resize: none; min-height: 44px; max-height: 120px; outline: none; }
.input-area textarea:focus { border-color: #e94560; }
.input-area button { background: #e94560; color: #fff; border: none; border-radius: 8px; padding: 10px 20px; font-size: 15px; font-weight: 600; cursor: pointer; }
.input-area button:disabled { opacity: 0.5; }

@media (max-width: 768px) {
  .browse-layout { flex-direction: column; }
  .sidebar { width: 100%; max-height: 40vh; }
  .message { max-width: 95%; }
}
</style>
</head>
<body>

<div class="tabs">
  <div class="tab active" onclick="switchTab('browse')">Browse</div>
  <div class="tab" onclick="switchTab('chat')">Ask</div>
</div>

<div class="panel active" id="panel-browse">
  <div class="browse-layout">
    <div class="sidebar">
      <div class="sidebar-search">
        <input type="text" id="globalSearch" placeholder="Search all entries..." oninput="globalSearchDebounce()">
      </div>
      <div class="category-list" id="categoryList"></div>
      <div class="entry-list" id="entryList"></div>
    </div>
    <div class="content-area" id="contentArea">
      <div class="empty">Select a category and entry to view</div>
    </div>
  </div>
</div>

<div class="panel" id="panel-chat">
  <div class="chat-container" id="chat"></div>
  <div class="input-area">
    <textarea id="input" rows="1" placeholder="Ask about rules, spells, abilities..."
      onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendQuery()}"></textarea>
    <button id="sendBtn" onclick="sendQuery()">Ask</button>
  </div>
</div>

<script>
// ── Tab switching ──
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`.tab[onclick*="${name}"]`).classList.add('active');
  document.getElementById('panel-' + name).classList.add('active');
}

// ── Browse ──
let categories = {};
let activeCategory = null;
let searchTimer = null;

async function loadCategories() {
  const resp = await fetch('/api/categories');
  categories = await resp.json();
  renderCategories();
}

function renderCategories() {
  const el = document.getElementById('categoryList');
  let html = '';
  for (const [book, sections] of Object.entries(categories)) {
    html += `<div class="category-header">${book}</div>`;
    for (const s of sections) {
      html += `<div class="category-item" data-id="${s.toc_id}" onclick="selectCategory(${s.toc_id}, this)">${s.title} <small style="color:#666">(${s.entries})</small></div>`;
    }
  }
  el.innerHTML = html;
}

async function selectCategory(tocId, el) {
  document.querySelectorAll('.category-item').forEach(i => i.classList.remove('active'));
  el.classList.add('active');
  activeCategory = tocId;
  const resp = await fetch(`/api/entries?toc_id=${tocId}`);
  const entries = await resp.json();
  renderEntries(entries);
}

function renderEntries(entries) {
  const el = document.getElementById('entryList');
  if (!entries.length) {
    el.innerHTML = '<div style="padding:12px;color:#666;font-size:13px">No entries</div>';
    return;
  }
  el.innerHTML = entries.map(e =>
    `<div class="entry-item" onclick="selectEntry(${activeCategory}, '${e.replace(/'/g,"\\'")}', this)">${e}</div>`
  ).join('');
}

async function selectEntry(tocId, title, el) {
  document.querySelectorAll('.entry-item').forEach(i => i.classList.remove('active'));
  if (el) el.classList.add('active');
  const resp = await fetch(`/api/entry?toc_id=${tocId}&title=${encodeURIComponent(title)}`);
  const data = await resp.json();
  renderContent(data);
}

function renderContent(data) {
  const area = document.getElementById('contentArea');
  const META_FIELDS = ['Sphere', 'School', 'Range', 'Components', 'Duration',
    'Casting Time', 'Area of Effect', 'Saving Throw', 'Power Score',
    'Initial Cost', 'Maintenance Cost', 'Preparation Time', 'Prerequisites'];

  // Parse metadata from content
  let content = data.content;
  const meta = {};
  for (const field of META_FIELDS) {
    const re = new RegExp(field + '\\\\s*:\\\\s*(.+?)(?:\\\\n|$)', 'i');
    const m = content.match(re);
    if (m) meta[field] = m[1].trim();
  }
  // Check reversible
  meta['Reversible'] = /reversible/i.test(content) ? 'Yes' : 'No';

  // Strip heading and metadata lines from description
  let desc = content;
  desc = desc.replace(/^#{1,4}\\s+.+$/gm, '');
  for (const field of META_FIELDS) {
    desc = desc.replace(new RegExp('^' + field + '\\\\s*:.+$', 'gmi'), '');
  }
  desc = desc.replace(/^\\(.*\\)\\s*(?:Reversible)?\\s*$/gm, '');
  desc = desc.replace(/^Reversible\\s*$/gm, '');
  desc = desc.trim();

  // Build HTML
  let metaHtml = '';
  if (Object.keys(meta).length > 1) {
    metaHtml = '<table class="meta-table">';
    for (const [k,v] of Object.entries(meta)) {
      metaHtml += `<tr><td>${k}</td><td>${v}</td></tr>`;
    }
    metaHtml += '</table>';
  }

  area.innerHTML = `
    <h1>${data.title}</h1>
    <div class="source">${data.source} — ${data.toc_title}</div>
    ${metaHtml}
    <div class="description">${desc}</div>
  `;
}

function globalSearchDebounce() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(globalSearch, 300);
}

async function globalSearch() {
  const q = document.getElementById('globalSearch').value.trim();
  if (q.length < 2) { loadCategories(); return; }
  const resp = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
  const results = await resp.json();
  const el = document.getElementById('entryList');
  el.innerHTML = results.map(r =>
    `<div class="entry-item" onclick="selectEntry(${r.toc_id}, '${r.title.replace(/'/g,"\\'")}', this)">
      <div>${r.title}</div>
      <small style="color:#666">${r.source} — ${r.toc_title}</small>
    </div>`
  ).join('');
  document.getElementById('categoryList').innerHTML = `<div class="category-header">${results.length} results for "${q}"</div>`;
}

// ── Chat ──
const chat = document.getElementById('chat');
const input = document.getElementById('input');
const sendBtn = document.getElementById('sendBtn');

input.addEventListener('input', () => {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 120) + 'px';
});

function addMessage(text, role) {
  const div = document.createElement('div');
  div.className = 'message ' + role;
  if (role === 'assistant') {
    div.innerHTML = text.replace(/\\n/g, '<br>').replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>');
  } else {
    div.textContent = text;
  }
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
}

async function sendQuery() {
  const q = input.value.trim();
  if (!q) return;
  input.value = '';
  input.style.height = 'auto';
  sendBtn.disabled = true;
  addMessage(q, 'user');
  const typing = document.createElement('div');
  typing.className = 'typing';
  typing.textContent = 'Looking up rules...';
  chat.appendChild(typing);
  chat.scrollTop = chat.scrollHeight;

  try {
    const resp = await fetch('/ask', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({question: q})
    });
    typing.remove();
    if (!resp.ok) {
      const err = await resp.json();
      addMessage('Error: ' + (err.detail || resp.statusText), 'assistant');
    } else {
      const data = await resp.json();
      addMessage(data.answer, 'assistant');
    }
  } catch (e) {
    typing.remove();
    addMessage('Connection error: ' + e.message, 'assistant');
  }
  sendBtn.disabled = false;
  input.focus();
}

// Init
loadCategories();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTML_PAGE
