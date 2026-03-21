"""FastAPI service with HTML UI for RAG queries over HTTP.
Serves both the API endpoints and a mobile-friendly web UI."""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from query_tabletop_rules import ask, search_section, get_toc, route_to_toc, DEFAULT_MODEL

app = FastAPI(title="Lakehouse RAG API", version="2.0.0")


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
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #1a1a2e;
    color: #e0e0e0;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
  }
  header {
    background: #16213e;
    padding: 12px 16px;
    border-bottom: 1px solid #0f3460;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  header h1 {
    font-size: 18px;
    color: #e94560;
    font-weight: 600;
  }
  .chat-container {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .message {
    max-width: 90%;
    padding: 12px 16px;
    border-radius: 12px;
    line-height: 1.5;
    font-size: 15px;
    word-wrap: break-word;
  }
  .message.user {
    align-self: flex-end;
    background: #0f3460;
    color: #fff;
    border-bottom-right-radius: 4px;
  }
  .message.assistant {
    align-self: flex-start;
    background: #16213e;
    border: 1px solid #0f3460;
    border-bottom-left-radius: 4px;
  }
  .message.assistant h2, .message.assistant h3 {
    color: #e94560;
    margin: 12px 0 6px 0;
    font-size: 16px;
  }
  .message.assistant h2:first-child, .message.assistant h3:first-child {
    margin-top: 0;
  }
  .message.assistant strong { color: #fff; }
  .message.assistant ul, .message.assistant ol {
    margin: 6px 0 6px 20px;
  }
  .message.assistant li { margin: 3px 0; }
  .message.assistant code {
    background: #0f3460;
    padding: 2px 5px;
    border-radius: 3px;
    font-size: 13px;
  }
  .message.assistant hr {
    border: none;
    border-top: 1px solid #0f3460;
    margin: 10px 0;
  }
  .message .sources {
    margin-top: 10px;
    padding-top: 8px;
    border-top: 1px solid #0f3460;
    font-size: 12px;
    color: #888;
  }
  .message .routing {
    font-size: 12px;
    color: #e94560;
    margin-bottom: 8px;
    font-style: italic;
  }
  .typing {
    align-self: flex-start;
    color: #888;
    font-style: italic;
    padding: 12px 16px;
  }
  .input-area {
    background: #16213e;
    border-top: 1px solid #0f3460;
    padding: 12px 16px;
    display: flex;
    gap: 8px;
  }
  .input-area textarea {
    flex: 1;
    background: #1a1a2e;
    border: 1px solid #0f3460;
    border-radius: 8px;
    color: #e0e0e0;
    padding: 10px 14px;
    font-size: 15px;
    font-family: inherit;
    resize: none;
    min-height: 44px;
    max-height: 120px;
    outline: none;
  }
  .input-area textarea:focus {
    border-color: #e94560;
  }
  .input-area button {
    background: #e94560;
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 10px 20px;
    font-size: 15px;
    font-weight: 600;
    cursor: pointer;
    white-space: nowrap;
  }
  .input-area button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  .input-area button:active:not(:disabled) {
    background: #c73e54;
  }
  @media (max-width: 600px) {
    .message { max-width: 95%; font-size: 14px; }
    header h1 { font-size: 16px; }
  }
</style>
</head>
<body>
<header>
  <h1>Rules Reference</h1>
</header>

<div class="chat-container" id="chat"></div>

<div class="input-area">
  <textarea id="input" rows="1" placeholder="Ask about rules, spells, abilities..."
    onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendQuery()}"></textarea>
  <button id="sendBtn" onclick="sendQuery()">Ask</button>
</div>

<script>
const chat = document.getElementById('chat');
const input = document.getElementById('input');
const sendBtn = document.getElementById('sendBtn');

// Auto-resize textarea
input.addEventListener('input', () => {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 120) + 'px';
});

// Simple markdown to HTML
function md(text) {
  let html = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>')
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/^---$/gm, '<hr>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\\n/g, '<br>');
  // Wrap consecutive <li> in <ul>
  html = html.replace(/((?:<li>.*?<\\/li><br>?)+)/g, '<ul>$1</ul>');
  return html;
}

function addMessage(text, role, extra) {
  const div = document.createElement('div');
  div.className = 'message ' + role;
  if (role === 'assistant') {
    let content = '';
    if (extra && extra.routing) {
      content += '<div class="routing">' + extra.routing + '</div>';
    }
    // Split sources from answer
    const parts = text.split('\\nSources:');
    content += md(parts[0]);
    if (parts[1]) {
      content += '<div class="sources">Sources: ' + parts[1].trim() + '</div>';
    }
    div.innerHTML = content;
  } else {
    div.textContent = text;
  }
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
  return div;
}

function showTyping() {
  const div = document.createElement('div');
  div.className = 'typing';
  div.id = 'typing';
  div.textContent = 'Looking up rules...';
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
}

function hideTyping() {
  const el = document.getElementById('typing');
  if (el) el.remove();
}

async function sendQuery() {
  const q = input.value.trim();
  if (!q) return;

  input.value = '';
  input.style.height = 'auto';
  sendBtn.disabled = true;
  addMessage(q, 'user');
  showTyping();

  try {
    const resp = await fetch('/ask', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({question: q})
    });

    hideTyping();

    if (!resp.ok) {
      const err = await resp.json();
      addMessage('Error: ' + (err.detail || resp.statusText), 'assistant');
    } else {
      const data = await resp.json();
      addMessage(data.answer, 'assistant');
    }
  } catch (e) {
    hideTyping();
    addMessage('Connection error: ' + e.message, 'assistant');
  }

  sendBtn.disabled = false;
  input.focus();
}

input.focus();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTML_PAGE
