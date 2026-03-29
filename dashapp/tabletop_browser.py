"""Tabletop Rules Browser — full scrollable book with ToC sidebar navigation.

Dash app serving on port 8000. Renders the entire book as a single HTML
document with proper anchor links from the sidebar ToC.
"""
import sys
import time
sys.path.insert(0, "/workspace")

from dash import Dash, html, dcc, callback, Input, Output
from dlt.lib.duckdb_reader import get_reader


# ── Cached DuckDB connection + query results ────────────────

_conn = None
_cache = {}  # key -> (timestamp, result)


def _get_conn():
    """Return a shared DuckDB connection, creating it once."""
    global _conn
    if _conn is None:
        _conn = get_reader(namespaces=["gold_tabletop"])
    return _conn


_rendered_cache = {}  # source_file -> (sidebar, content)


def _invalidate():
    """Drop cached connection and query results (call after pipeline runs)."""
    global _conn, _cache, _rendered_cache
    _conn = None
    _cache = {}
    _rendered_cache = {}


def _query(sql, params=None):
    """Execute SQL and return list of dicts. Results are cached by (sql, params)."""
    key = (sql, tuple(params) if params else ())
    if key in _cache:
        return _cache[key]
    conn = _get_conn()
    if params:
        df = conn.execute(sql, params).fetchdf()
    else:
        df = conn.execute(sql).fetchdf()
    result = df.to_dict("records")
    _cache[key] = result
    return result


def _get_books():
    return [r["source_file"] for r in _query(
        "SELECT source_file FROM gold_tabletop.gold_files ORDER BY source_file"
    )]


def _get_toc(source_file):
    return _query(
        "SELECT toc_id, title, sort_order, depth, is_chapter, is_table, parent_title "
        "FROM gold_tabletop.gold_toc "
        "WHERE source_file = ? AND is_excluded = false AND is_table = false "
        "ORDER BY sort_order",
        [source_file],
    )


def _get_full_book(source_file):
    """Get all gold entries in ToC reading order for the full book view."""
    return _query(
        "SELECT entry_id, toc_id, toc_title, section_title, entry_title, "
        "content, char_count, spell_class, spell_level, sort_order, "
        "depth, is_chapter, is_table "
        "FROM gold_tabletop.gold_entries "
        "WHERE source_file = ? AND is_table = false "
        "ORDER BY sort_order, entry_title",
        [source_file],
    )


def _get_entry_index(source_file):
    """Return {entry_id: {entry_id, entry_title, entry_type, ...}} for all entries."""
    rows = _query(
        "SELECT entry_id, entry_title, entry_type, spell_level, spell_class, "
        "school, sphere "
        "FROM gold_tabletop.gold_entry_index WHERE source_file = ?",
        [source_file],
    )
    return {r["entry_id"]: r for r in rows}


def _get_tables(source_file):
    """Get all parsed tables with their rows, keyed by toc_title."""
    rows = _query(
        "SELECT toc_title, toc_id, table_number, table_title, sort_order, row_index, cells "
        "FROM gold_tabletop.gold_tables "
        "WHERE source_file = ? "
        "ORDER BY sort_order, table_number, row_index",
        [source_file],
    )
    import json as _json
    tables = {}
    for r in rows:
        key = r["toc_title"]
        if key not in tables:
            tables[key] = {"toc_title": key, "toc_id": r["toc_id"],
                           "table_number": r["table_number"],
                           "table_title": r["table_title"],
                           "sort_order": r["sort_order"], "rows": []}
        try:
            cells = _json.loads(r["cells"]) if isinstance(r["cells"], str) else r["cells"]
        except Exception:
            cells = [r["cells"]]
        tables[key]["rows"].append(cells)
    return tables


def _get_summaries():
    """Return {entry_id: summary}. Empty dict if table doesn't exist yet."""
    try:
        rows = _query(
            "SELECT entry_id, content as summary FROM gold_tabletop.gold_entry_descriptions "
            "WHERE description_type = 'summary'"
        )
        return {r["entry_id"]: r["summary"] for r in rows}
    except Exception:
        # Fall back to legacy table
        try:
            rows = _query("SELECT entry_id, summary FROM gold_tabletop.gold_ai_summaries")
            return {r["entry_id"]: r["summary"] for r in rows}
        except Exception:
            return {}


def _get_descriptions():
    """Return {entry_id: content} for original clean descriptions."""
    try:
        rows = _query(
            "SELECT entry_id, content FROM gold_tabletop.gold_entry_descriptions "
            "WHERE description_type = 'original'"
        )
        return {r["entry_id"]: r["content"] for r in rows}
    except Exception:
        return {}


def _get_annotations():
    """Return {entry_id: {is_combat, is_popular}}. Empty dict if table doesn't exist yet."""
    try:
        rows = _query("SELECT entry_id, is_combat, is_popular FROM gold_tabletop.gold_ai_annotations")
        return {r["entry_id"]: r for r in rows}
    except Exception:
        return {}


# ── Build sidebar ToC ────────────────────────────────────────

def build_toc_sidebar(toc_rows, anchor_map):
    """Build sidebar ToC. anchor_map = {toc_id: actual_anchor_id}."""
    items = []
    # Build parent chapter lookup for fallback
    parent_chapter_anchor = {}
    current_chapter_toc_id = None
    for row in toc_rows:
        if row["is_chapter"]:
            current_chapter_toc_id = row["toc_id"]
        parent_chapter_anchor[row["toc_id"]] = current_chapter_toc_id

    for row in toc_rows:
        depth = row["depth"]
        is_chapter = row["is_chapter"]
        is_table = row["is_table"]
        toc_id = row["toc_id"]

        # Use direct anchor if available, else fall back to parent chapter
        if toc_id in anchor_map:
            anchor = anchor_map[toc_id]
        else:
            parent_id = parent_chapter_anchor.get(toc_id)
            anchor = f"toc-{parent_id}" if parent_id else f"toc-{toc_id}"

        style = {"paddingLeft": f"{depth * 1.2}rem", "lineHeight": "1.6"}
        cls = "toc-chapter" if is_chapter else ("toc-table" if is_table else "toc-section")

        items.append(
            html.Div(
                html.A(row["title"], href=f"#{anchor}", className="toc-link"),
                className=cls,
                style=style,
            )
        )
    return items


# ── Build book content ───────────────────────────────────────

_MATERIAL_PATTERNS = [
    "The material component",
    "The material components",
    "The materials for",
    "The spell requires",
    "The spell's material",
    "Material component:",
    "Material components:",
]


def _split_material_components(content):
    """Split material component sentences from the end of spell content.

    Returns (main_content, component_text). component_text is empty string
    if no material component text is found.
    """
    lines = content.rstrip().split("\n")
    # Search from the end for material component text
    for i in range(len(lines) - 1, max(len(lines) - 8, -1), -1):
        stripped = lines[i].strip()
        for pat in _MATERIAL_PATTERNS:
            if stripped.lower().startswith(pat.lower()):
                main = "\n".join(lines[:i]).rstrip()
                comp = "\n".join(lines[i:]).strip()
                return main, comp
    return content, ""


def _linkify_table_refs(text, toc_table_num_to_id):
    """Replace 'Table N' references in text with anchor links."""
    import re
    def _replace(m):
        num = int(m.group(1))
        toc_id = toc_table_num_to_id.get(num)
        if toc_id:
            return f'<a href="#toc-{toc_id}" class="table-ref">{m.group(0)}</a>'
        return m.group(0)
    return re.sub(r"\bTable\s+(\d+)\b", _replace, text)


def _render_table(table_data):
    """Render a parsed table as an HTML table element."""
    if not table_data or not table_data.get("rows"):
        return html.Div("(table data not available)", className="table-missing")
    rows = table_data["rows"]
    # First row is header
    header = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []
    return html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(c, className="table-header-cell") for c in header])),
            html.Tbody([
                html.Tr([html.Td(c, className="table-cell") for c in row])
                for row in data_rows
            ]),
        ], className="data-table"),
    ], className="table-container")


def build_book_content(book_data, toc_rows, entry_index, summaries, annotations, descriptions=None):
    """Returns (elements, anchor_map) where anchor_map = {toc_id: anchor_id}."""
    import re
    elements = []
    toc_title_to_id = {r["title"]: r["toc_id"] for r in toc_rows}
    # Also map by table number for fuzzy table matching
    toc_table_num_to_id = {}
    for r in toc_rows:
        if r["is_table"]:
            m = re.search(r"Table\s+(\d+)", r["title"])
            if m:
                toc_table_num_to_id[int(m.group(1))] = r["toc_id"]
    anchor_map = {}  # toc_id -> actual anchor string
    # Chapters always have anchors
    for r in toc_rows:
        if r["is_chapter"]:
            anchor_map[r["toc_id"]] = f"toc-{r['toc_id']}"
    import math

    def _content_category(toc_title, entry_title, content):
        """Return CSS class for optional rule categories."""
        classes = []
        toc_lower = (toc_title or "").lower()
        entry_lower = (entry_title or "").lower()
        if "proficienc" in toc_lower or "proficienc" in entry_lower:
            classes.append("cat-proficiencies")
        if "spell component" in toc_lower or "spell component" in entry_lower:
            classes.append("cat-spell-components")
        if "component" in entry_lower and "spell" in toc_lower:
            classes.append("cat-spell-components")
        if "encumbrance" in toc_lower or "encumbrance" in entry_lower:
            classes.append("cat-encumbrance")
        return " ".join(classes)

    current_toc_id = None

    for row in book_data:
        toc_id = row["toc_id"]
        toc_title = row["toc_title"]
        entry_title = row["entry_title"]
        is_chapter = row["is_chapter"]
        depth = row["depth"]
        sort_order = row.get("sort_order", 0)

        cat = _content_category(toc_title, entry_title, row.get("content", ""))

        # New ToC section — render heading with anchor
        if toc_id != current_toc_id:
            current_toc_id = toc_id
            anchor_id = f"toc-{toc_id}"
            anchor_map[toc_id] = anchor_id
            level = min(depth + 1, 4)
            tag = [html.H1, html.H2, html.H3, html.H4][level - 1]
            cls = "chapter" if is_chapter else "section"
            elements.append(html.Div(id=anchor_id, className=f"{cls}-anchor {cat}"))
            elements.append(tag(toc_title, className=f"{cls}-heading {cat}"))

        # Entry heading + metadata
        entry_els = []

        # Anchor for ToC sub-section matching this entry title
        if entry_title:
            matched_toc = toc_title_to_id.get(entry_title)
            if matched_toc and matched_toc not in anchor_map:
                anchor_id = f"toc-{matched_toc}"
                entry_els.append(html.Div(id=anchor_id))
                anchor_map[matched_toc] = anchor_id

        # Entry title — skip if same as section heading (already rendered)
        entry_id = row.get("entry_id")
        idx = entry_index.get(entry_id) if entry_id else None
        entry_anchor = f"entry-{entry_id}" if entry_id else ""

        if entry_title and entry_title != toc_title:
            entry_els.append(html.Div(entry_title, className="entry-title", id=entry_anchor))

        # Badges — only show meaningful spell/annotation info
        if idx:
            badges = []
            sl = idx.get("spell_level")
            if sl is not None and not (isinstance(sl, float) and math.isnan(sl)):
                badges.append(f"Level {int(sl)}")
            for field in ("spell_class", "school", "sphere"):
                val = idx.get(field)
                if val and not (isinstance(val, float) and math.isnan(val)):
                    badges.append(str(val))
            if entry_id:
                ann = annotations.get(entry_id)
                if ann:
                    if ann.get("is_combat"):
                        badges.append("Combat")
                    if ann.get("is_popular"):
                        badges.append("Popular")
            if badges:
                entry_els.append(html.Div(" · ".join(badges), className="entry-badges"))

        # Summary (shown when AI Summary toggle is on — spells only)
        is_spell = toc_title and "level spells" in toc_title.lower()
        if entry_id and is_spell:
            summary = summaries.get(entry_id)
            summary_text = summary if summary else "(AI summary not yet generated)"
            summary_cls = "entry-summary" if summary else "entry-summary entry-summary-empty"
            entry_els.append(html.Div(summary_text, className=summary_cls))

        if entry_els:
            elements.append(html.Div(entry_els, className=f"entry-block {cat}"))

        # Full entry content — for spells with descriptions, use clean description
        content_cls = "entry-content has-summary" if (entry_id and is_spell) else "entry-content"
        if row["content"]:
            # Use clean description for spells if available (metadata stripped)
            if entry_id and is_spell and descriptions and entry_id in descriptions:
                content_text = descriptions[entry_id]
            else:
                content_text = row["content"]
            main_content, component_text = _split_material_components(content_text)
            linked_main = _linkify_table_refs(main_content, toc_table_num_to_id)
            elements.append(html.Div(
                dcc.Markdown(linked_main, dangerously_allow_html=True),
                className=f"{content_cls} {cat}",
            ))
            if component_text:
                linked_comp = _linkify_table_refs(component_text, toc_table_num_to_id)
                elements.append(html.Div(
                    dcc.Markdown(linked_comp, dangerously_allow_html=True),
                    className=f"{content_cls} cat-spell-components {cat}",
                ))

    return elements, anchor_map


# ── App ──────────────────────────────────────────────────────

app = Dash(__name__)

books = _get_books()
default_book = books[0] if books else None

app.layout = html.Div([
    # Sidebar
    html.Div([
        html.H3("Rules Browser"),
        dcc.Dropdown(
            id="book-selector",
            options=[{"label": b.replace(".pdf", ""), "value": b} for b in books],
            value=default_book,
            clearable=False,
            style={"marginBottom": "0.5rem"},
        ),
        html.Div([
            dcc.Checklist(
                id="display-toggles",
                options=[
                    {"label": " AI Summary", "value": "summary"},
                    {"label": " Entry Details", "value": "meta"},
                ],
                value=["summary", "meta"],
                style={"fontSize": "0.85rem"},
            ),
        ], style={"marginBottom": "0.5rem"}),
        html.Div([
            html.Div("Optional Rules", style={"fontWeight": "600", "fontSize": "0.85rem",
                                               "marginBottom": "0.2rem"}),
            dcc.Checklist(
                id="optional-rules",
                options=[
                    {"label": " Proficiencies", "value": "proficiencies"},
                    {"label": " Spell Components", "value": "spell_components"},
                    {"label": " Encumbrance", "value": "encumbrance"},
                ],
                value=["proficiencies", "spell_components", "encumbrance"],
                style={"fontSize": "0.85rem"},
            ),
        ], style={"marginBottom": "0.5rem"}),
        html.Hr(),
        html.Div(id="toc-nav", style={"overflowY": "auto", "flex": "1"}),
        html.Hr(),
        html.Button("Refresh Data", id="refresh-btn",
                     style={"width": "100%", "padding": "0.4rem", "cursor": "pointer",
                            "background": "#21262d", "color": "#fafafa",
                            "border": "1px solid #30363d", "borderRadius": "4px",
                            "fontSize": "0.85rem"}),
        html.Div(id="refresh-status", style={"fontSize": "0.8rem", "color": "#8b949e",
                                              "marginTop": "0.3rem", "textAlign": "center"}),
    ], id="sidebar"),

    # Main content
    html.Div([
        html.Div(id="book-content"),
    ], id="main-content"),

    html.Div(id="toggle-dummy", style={"display": "none"}),
    html.Div(id="optional-dummy", style={"display": "none"}),
], id="app-container")


@callback(
    Output("toc-nav", "children"),
    Output("book-content", "children"),
    Input("book-selector", "value"),
)
def update_book(source_file):
    if not source_file:
        return [], [html.P("Select a book.")]

    if source_file in _rendered_cache:
        sidebar, content = _rendered_cache[source_file]
        return sidebar, content

    t0 = time.time()
    toc = _get_toc(source_file)
    book_data = _get_full_book(source_file)
    entry_index = _get_entry_index(source_file)
    summaries = _get_summaries()
    annotations = _get_annotations()
    descriptions = _get_descriptions()
    t1 = time.time()

    content, anchor_map = build_book_content(book_data, toc, entry_index, summaries, annotations, descriptions)
    sidebar = build_toc_sidebar(toc, anchor_map)
    t2 = time.time()

    book_name = source_file.replace(".pdf", "").replace("_", " ")
    header = [html.H1(book_name)]
    result_content = header + content

    print(f"[browser] {source_file}: queries={t1-t0:.1f}s, render={t2-t1:.1f}s")

    _rendered_cache[source_file] = (sidebar, result_content)
    return sidebar, result_content


# Clientside callback — toggles visibility without re-rendering
app.clientside_callback(
    """
    function(toggles) {
        window._displayToggles = toggles || [];
        window._applyDisplayToggles();
        return '';
    }
    """,
    Output("toggle-dummy", "children"),
    Input("display-toggles", "value"),
)

# Clientside callback — toggle optional rule sections
app.clientside_callback(
    """
    function(toggles) {
        var cats = {
            'proficiencies': 'cat-proficiencies',
            'spell_components': 'cat-spell-components',
            'encumbrance': 'cat-encumbrance'
        };
        Object.keys(cats).forEach(function(key) {
            var show = (toggles || []).indexOf(key) >= 0;
            document.querySelectorAll('.' + cats[key]).forEach(function(el) {
                el.style.display = show ? '' : 'none';
            });
        });
        return '';
    }
    """,
    Output("optional-dummy", "children"),
    Input("optional-rules", "value"),
)


@callback(
    Output("refresh-status", "children"),
    Output("book-selector", "options"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_data(n_clicks):
    _invalidate()
    new_books = _get_books()
    options = [{"label": b.replace(".pdf", ""), "value": b} for b in new_books]
    return f"Refreshed ({time.strftime('%H:%M:%S')})", options


# ── CSS ──────────────────────────────────────────────────────

app.index_string = '''
<!DOCTYPE html>
<html>
<head>
    {%metas%}
    <title>Tabletop Rules Browser</title>
    {%css%}
    <style>
        body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
               background: #0e1117; color: #fafafa; }
        #app-container { display: flex; height: 100vh; }
        #sidebar { width: 320px; min-width: 320px; background: #161b22; padding: 1rem;
                   overflow-y: auto; border-right: 1px solid #30363d;
                   display: flex; flex-direction: column;
                   position: fixed; top: 0; left: 0; height: 100vh; z-index: 100;
                   transform: translateX(-308px); transition: transform 0.2s ease; }
        #sidebar:hover { transform: translateX(0); }
        #sidebar::after { content: ''; position: absolute; top: 0; right: 0; width: 12px;
                          height: 100%; background: linear-gradient(to right, transparent, #30363d);
                          cursor: pointer; }
        #main-content { flex: 1; overflow-y: auto; padding: 2rem 3rem; margin-left: 12px; }

        /* ToC */
        .toc-chapter { font-weight: 600; font-size: 0.95rem; margin-top: 0.2rem; }
        .toc-section { font-size: 0.85rem; }
        .toc-table { font-size: 0.85rem; font-style: italic; }
        .toc-link { color: #4a9eff; text-decoration: none; }
        .toc-link:hover { text-decoration: underline; }

        /* Book content */
        .chapter-anchor { padding-top: 1rem; }
        .chapter-heading { margin-top: 0.5rem; border-top: 2px solid #30363d; padding-top: 1rem; }
        .section-anchor { padding-top: 0.8rem; }
        .section-heading { margin-top: 0.3rem; border-top: 1px solid #21262d; padding-top: 0.5rem; }
        .entry-block { margin-top: 1.2rem; }
        .entry-title { font-weight: 600; font-size: 1.1rem; color: #4a9eff;
                       padding-top: 0.5rem; }
        .entry-badges { font-size: 0.8rem; color: #8b949e; margin: 0.1rem 0; }
        .entry-summary { background: #1a1a2e; border-left: 3px solid #4a9eff;
                         padding: 0.5rem 0.8rem; margin: 0.4rem 0; font-size: 0.9rem; }
        .entry-summary-empty { color: #484f58; font-style: italic; }
        .entry-content { margin-top: 0.3rem; line-height: 1.6; }
        .entry-content p { margin: 0.4rem 0; }
        .table-ref { color: #4a9eff; text-decoration: none; }
        .table-ref:hover { text-decoration: underline; }

        /* Default: AI Summary toggle is ON — hide spell content, show summary */
        .entry-content.has-summary { display: none; }

        /* Dropdown styling */
        .Select-control { background: #21262d !important; border-color: #30363d !important; }
        .Select-value-label { color: #fafafa !important; }
        .Select-menu-outer { background: #21262d !important; }

        /* Instant scroll on anchor click */
        #main-content { scroll-behavior: auto; }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
    <script>
        // Shared toggle logic — called by clientside callback AND after content renders
        window._displayToggles = ['summary', 'meta'];
        window._applyDisplayToggles = function() {
            var toggles = window._displayToggles;
            var showSummary = toggles.indexOf('summary') >= 0;
            var showMeta = toggles.indexOf('meta') >= 0;
            document.querySelectorAll('.entry-summary').forEach(function(el) {
                el.style.display = showSummary ? 'block' : 'none';
            });
            document.querySelectorAll('.entry-content.has-summary').forEach(function(el) {
                el.style.display = showSummary ? 'none' : 'block';
            });
            document.querySelectorAll('.entry-badges').forEach(function(el) {
                el.style.display = showMeta ? 'block' : 'none';
            });
        };
        // Re-apply toggles whenever book content changes
        var obs = new MutationObserver(function() { window._applyDisplayToggles(); });
        document.addEventListener('DOMContentLoaded', function() {
            var mc = document.getElementById('book-content');
            if (mc) obs.observe(mc, {childList: true, subtree: true});
        });

        // Intercept anchor clicks and scroll within #main-content div
        document.addEventListener('click', function(e) {
            var link = e.target.closest('a[href^="#"]');
            if (!link) return;
            var id = link.getAttribute('href').substring(1);
            var target = document.getElementById(id);
            console.log('[scroll]', id, 'found:', !!target);
            if (!target) return;
            e.preventDefault();
            target.scrollIntoView({ behavior: 'instant', block: 'start' });
        });
    </script>
</body>
</html>
'''


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
