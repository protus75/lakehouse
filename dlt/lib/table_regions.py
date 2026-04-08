"""Font-switch table region detection + char-offset span maps.

Two related features for table extraction:

1. detect_table_regions(page, cfg) — returns TableRegion bboxes built from
   PyMuPDF span (font, size, bold) metadata. See below for the algorithm.

2. extract_page_text_with_span_map(page) — returns (text, span_map) where
   text matches PyMuPDF's default page.get_text() output and span_map records
   the (char_start, char_end, bbox) of every span. Used to convert region
   bboxes into character ranges that silver can mask.

Body text uses one dominant style; table headers use a different style
(per-book, configured in YAML).

Algorithm:
  1. Walk page.get_text("dict") spans, identify body style from histogram.
  2. Find header spans matching cfg (header_font, header_size, header_bold).
  3. Cluster header spans into header rows by y-position, then group adjacent
     header rows with the same x-range into header clusters.
  4. For each cluster, extract column x0 positions from the first row.
  5. Trace down: include lines below where >=2 words sit at matching column x0s.
     Stop when alignment breaks for 2+ lines or another header cluster begins.
  6. Output {bbox, header_bbox, span_indices, row_count, col_count, columns}.

No text matching, no LLM. Deterministic, fast (~ms/page).
"""
from dataclasses import dataclass, field


@dataclass
class SpanCharRange:
    char_start: int
    char_end: int
    bbox: tuple   # (x0, y0, x1, y1)


def extract_page_text_with_span_map(page) -> tuple:
    """Build a page-text string and a per-span char-offset map.

    The text output is *substring-equivalent* to `page.get_text("text")` —
    same words in the same order with the same line breaks — but may differ
    by an occasional newline in pages with unusual block structures (verified
    319/322 byte-identical on PHB; the 3 mismatches are 2 art-only pages and
    1 page with one extra newline in a bullet list). Silver string matching
    is substring/find-based and tolerates this.

    The span_map is a list of SpanCharRange entries — one per text-bearing
    span — recording where that span sits in the assembled text and its bbox.
    Used by Phase 2 to convert table region bboxes into char ranges for
    page_text_masks.
    """
    d = page.get_text("dict")
    pieces = []
    span_map = []
    cursor = 0

    for block in d.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = span.get("text", "")
                pieces.append(text)
                if text:
                    span_map.append(SpanCharRange(
                        char_start=cursor,
                        char_end=cursor + len(text),
                        bbox=tuple(round(x, 2) for x in span.get("bbox", (0, 0, 0, 0))),
                    ))
                cursor += len(text)
            # Line terminator
            pieces.append("\n")
            cursor += 1

    text = "".join(pieces)
    return text, span_map


def region_char_ranges(region, span_map: list) -> list:
    """Convert a TableRegion bbox into a list of (char_start, char_end) ranges
    in the page text.

    A span belongs to the region if its bbox center sits inside the region
    bbox. Adjacent matching spans (separated by 0 or 1 chars) get coalesced
    into a single range so silver can mask each table region as one or two
    contiguous slices instead of dozens of tiny ones.
    """
    rx0, ry0, rx1, ry1 = region.bbox
    ranges = []
    for sm in span_map:
        sx0, sy0, sx1, sy1 = sm.bbox
        cx = (sx0 + sx1) / 2
        cy = (sy0 + sy1) / 2
        if rx0 <= cx <= rx1 and ry0 <= cy <= ry1:
            ranges.append((sm.char_start, sm.char_end))
    if not ranges:
        return []
    ranges.sort()
    coalesced = [ranges[0]]
    for s, e in ranges[1:]:
        prev_s, prev_e = coalesced[-1]
        if s <= prev_e + 1:
            coalesced[-1] = (prev_s, max(prev_e, e))
        else:
            coalesced.append((s, e))
    return coalesced


@dataclass
class TableRegion:
    bbox: tuple              # (x0, y0, x1, y1) full region
    header_bbox: tuple       # (x0, y0, x1, y1) header rows only
    columns: list            # [x0, x0, ...] column anchor positions
    row_count: int           # total rows including header
    col_count: int           # number of columns
    header_row_count: int    # number of header rows
    span_indices: list       # indices into the flat span list


def _flatten_spans(page_dict: dict) -> list:
    """Flatten dict blocks/lines/spans into a single list with bbox + style.
    Returns list of {idx, font, size, bold, bbox, text, line_y, block_idx}.
    """
    spans = []
    for bi, block in enumerate(page_dict.get("blocks", [])):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                if not span.get("text", "").strip():
                    continue
                spans.append({
                    "idx": len(spans),
                    "font": span.get("font", ""),
                    "size": round(span.get("size", 0), 1),
                    "bold": bool(span.get("flags", 0) & 16),
                    "bbox": tuple(round(x, 1) for x in span.get("bbox", (0, 0, 0, 0))),
                    "text": span.get("text", ""),
                    "block_idx": bi,
                })
    return spans


def _is_header_span(s: dict, cfg: dict) -> bool:
    """Check if a span matches the configured header style."""
    if cfg.get("header_font") and s["font"] != cfg["header_font"]:
        return False
    hsize = cfg.get("header_size")
    if hsize is not None and abs(s["size"] - hsize) > 0.1:
        return False
    hbold = cfg.get("header_bold")
    if hbold is not None and s["bold"] != hbold:
        return False
    return True


def _group_into_rows(spans: list, y_tol: float) -> list:
    """Group spans by y0 (within y_tol) into rows. Sorted top-to-bottom.
    Returns list of [span, ...] rows."""
    if not spans:
        return []
    sorted_spans = sorted(spans, key=lambda s: (s["bbox"][1], s["bbox"][0]))
    rows = []
    current = [sorted_spans[0]]
    current_y = sorted_spans[0]["bbox"][1]
    for s in sorted_spans[1:]:
        if abs(s["bbox"][1] - current_y) <= y_tol:
            current.append(s)
        else:
            rows.append(sorted(current, key=lambda x: x["bbox"][0]))
            current = [s]
            current_y = s["bbox"][1]
    rows.append(sorted(current, key=lambda x: x["bbox"][0]))
    return rows


def _row_x_range(row: list) -> tuple:
    """Min x0 and max x1 across a row of spans."""
    return (
        min(s["bbox"][0] for s in row),
        max(s["bbox"][2] for s in row),
    )


def _row_y(row: list) -> float:
    return min(s["bbox"][1] for s in row)


def _row_y_bottom(row: list) -> float:
    return max(s["bbox"][3] for s in row)


def _x_overlap(a: tuple, b: tuple) -> bool:
    """Two x-ranges overlap (have common x-extent)."""
    return not (a[1] < b[0] or b[1] < a[0])


def _cluster_header_rows(header_rows: list, cfg: dict) -> list:
    """Group header rows into clusters by vertical adjacency + x-range overlap.

    Two rows belong to the same cluster if:
      - vertically close (gap <= cluster_y_gap)
      - x-ranges overlap
      - their span counts are similar (max/min <= 2.0) OR one row is a single
        wide span (a label row above the main column header row)

    Mismatched span counts (e.g. a 1-span definition term followed by a 5-span
    header row) get split into separate clusters so glossary entries don't
    coalesce into one giant fake header.
    """
    if not header_rows:
        return []
    gap = cfg.get("cluster_y_gap", 15.0)
    clusters = []
    current = [header_rows[0]]
    for row in header_rows[1:]:
        prev = current[-1]
        prev_bottom = _row_y_bottom(prev)
        row_top = _row_y(row)
        prev_x = _row_x_range(prev)
        row_x = _row_x_range(row)
        adjacent = row_top - prev_bottom <= gap and _x_overlap(prev_x, row_x)
        # Span-count compatibility:
        #   - similar counts (max/min <= 2.0), OR
        #   - prev is a single-span LABEL row (e.g. "Table 1: Strength") and
        #     current is the actual multi-span column header. Only allowed
        #     when current has >=2 spans, otherwise sequential single-span
        #     rows (glossary terms) coalesce into a fake giant header.
        a, b = len(prev), len(row)
        if a == 1 and b >= 2:
            compatible = True
        elif a >= 2 and b == 1:
            compatible = True  # trailing single-span continuation row
        elif a == 1 and b == 1:
            compatible = False  # don't chain sequential singletons
        else:
            compatible = max(a, b) / min(a, b) <= 2.0
        if adjacent and compatible:
            current.append(row)
        else:
            clusters.append(current)
            current = [row]
    clusters.append(current)
    return clusters


def _column_ranges_from_cluster(cluster: list, x_tol: float,
                                outer_margin: float = 80.0) -> list:
    """Extract column (x0, x1) ranges from a header cluster.

    Uses the row with the MOST DISTINCT x-positions in the cluster as the
    column template. For multi-row headers where one row has wider/merged
    spans (e.g. "10 11 12" as one cell) and another has finer granularity,
    the finer-grained row is the correct column structure.

    For multi-span headers, columns are MIDPOINT ZONES between header span
    centers, with `outer_margin` extension on the leftmost/rightmost edges.
    Single-span headers return the raw bbox extent and will fail min_cols=2
    downstream (correctly — one header isn't enough evidence of a table).
    """
    if not cluster:
        return []

    # Score each row by distinct x0 positions (after merging overlapping spans).
    def _merge_row(row):
        spans = sorted(row, key=lambda s: s["bbox"][0])
        merged = [dict(spans[0])]
        for s in spans[1:]:
            prev = merged[-1]
            if s["bbox"][0] <= prev["bbox"][2]:
                merged[-1]["bbox"] = (
                    prev["bbox"][0], prev["bbox"][1],
                    max(prev["bbox"][2], s["bbox"][2]),
                    max(prev["bbox"][3], s["bbox"][3]),
                )
            else:
                merged.append(dict(s))
        return merged

    best_merged = max((_merge_row(r) for r in cluster), key=len)

    if len(best_merged) == 1:
        b = best_merged[0]["bbox"]
        return [(b[0], b[2])]

    centers = [(s["bbox"][0] + s["bbox"][2]) / 2 for s in best_merged]
    boundaries = [best_merged[0]["bbox"][0] - outer_margin]
    for i in range(len(centers) - 1):
        boundaries.append((centers[i] + centers[i + 1]) / 2)
    boundaries.append(best_merged[-1]["bbox"][2] + outer_margin)

    return [(boundaries[i], boundaries[i + 1]) for i in range(len(best_merged))]


def _row_matches_columns(row: list, columns: list) -> int:
    """Count how many columns contain at least one word from this row.

    A word matches a column if its x-center falls within the column's (x0, x1)
    range. Column ranges already include outer margins via midpoint zones.
    """
    if not row:
        return 0
    matched = 0
    for col in columns:
        c0, c1 = col
        for s in row:
            cx = (s["bbox"][0] + s["bbox"][2]) / 2
            if c0 <= cx <= c1:
                matched += 1
                break
    return matched


def _trace_data_rows(all_rows: list, header_end_idx: int, header_x_range: tuple,
                     columns: list, cfg: dict) -> tuple:
    """Walk down all_rows from header_end_idx, including rows that match column
    structure within the header's horizontal extent.

    On 2-column pages, all_rows is sorted by y0 which interleaves left and right
    column content. Rows that don't horizontally overlap the header belong to a
    different column and are SKIPPED (not counted as misses). Only rows that DO
    overlap the header column are evaluated; misses count toward the 2-strike
    stop condition.

    Returns (last_data_row_idx, included_count).
    """
    min_cols = cfg.get("min_columns", 2)
    misses = 0
    last_good = header_end_idx
    included = 0
    for i in range(header_end_idx + 1, len(all_rows)):
        row = all_rows[i]
        row_x = _row_x_range(row)
        # Other-column row — skip without penalty
        if not _x_overlap(row_x, header_x_range):
            continue
        matched = _row_matches_columns(row, columns)
        if matched >= min_cols:
            last_good = i
            included += 1
            misses = 0
        else:
            misses += 1
            if misses >= 2:
                break
    return last_good, included


def _try_headerless_columns(label_cluster: list, all_rows: list,
                            label_end_idx: int, cfg: dict,
                            outer_margin: float = 80.0) -> tuple | None:
    """For a single-span LABEL cluster (e.g. "Armor *", "Table 13:"), look in
    the rows immediately below for a column-aligned body-font block, and use
    its first row as the column template.

    A headerless table is recognized when:
      - the cluster is a single Formata-Regular span (a label/title)
      - within `headerless_lookahead` points below the label, there is a
        body-font row whose horizontal extent contains the label's x-center
        and which has at least min_columns spans
      - that template row's column structure repeats on at least
        `headerless_min_data_rows` consecutive rows below it

    Returns (columns, header_x_range, data_start_idx) on success, else None.
    """
    if len(label_cluster) != 1 or len(label_cluster[0]) != 1:
        return None
    label_span = label_cluster[0][0]
    label_bottom = label_span["bbox"][3]
    label_x_center = (label_span["bbox"][0] + label_span["bbox"][2]) / 2

    look_ahead = cfg.get("headerless_lookahead", 30.0)
    min_cols = cfg.get("min_columns", 2)
    min_data = cfg.get("headerless_min_data_rows", 2)

    # Locate the first body-font row that horizontally contains the label.
    template_idx = None
    template_row = None
    for i in range(label_end_idx + 1, len(all_rows)):
        row = all_rows[i]
        ry = _row_y(row)
        if ry > label_bottom + look_ahead:
            break
        rx = _row_x_range(row)
        if not (rx[0] <= label_x_center <= rx[1]):
            continue
        body_spans = [s for s in row if not _is_header_span(s, cfg)]
        if len(body_spans) < min_cols:
            continue
        template_idx = i
        template_row = sorted(body_spans, key=lambda s: s["bbox"][0])
        break

    if template_idx is None:
        return None

    # Build columns from the template row using the same midpoint logic.
    centers = [(s["bbox"][0] + s["bbox"][2]) / 2 for s in template_row]
    boundaries = [template_row[0]["bbox"][0] - outer_margin]
    for j in range(len(centers) - 1):
        boundaries.append((centers[j] + centers[j + 1]) / 2)
    boundaries.append(template_row[-1]["bbox"][2] + outer_margin)
    columns = [(boundaries[j], boundaries[j + 1]) for j in range(len(template_row))]
    header_x_range = (template_row[0]["bbox"][0], template_row[-1]["bbox"][2])

    # Verify min_data consecutive rows match (the template row counts as one).
    consec = 1
    for i in range(template_idx + 1, len(all_rows)):
        row = all_rows[i]
        rx = _row_x_range(row)
        if not _x_overlap(rx, header_x_range):
            continue
        if _row_matches_columns(row, columns) >= min_cols:
            consec += 1
            if consec >= min_data:
                break
        else:
            break
    if consec < min_data:
        return None

    return columns, header_x_range, template_idx


def detect_table_regions(page, cfg: dict) -> list:
    """Detect table regions on a PyMuPDF page using font-switch + column tracing.

    Args:
        page: pymupdf.Page object
        cfg: per-book table_detection config dict

    Returns:
        list of TableRegion
    """
    if not cfg or not cfg.get("enabled", False):
        return []

    page_dict = page.get_text("dict")
    spans = _flatten_spans(page_dict)
    if not spans:
        return []

    # Group all spans into rows (used for column tracing)
    y_tol = cfg.get("row_y_tolerance", 3.0)
    all_rows = _group_into_rows(spans, y_tol)

    # Find header spans, group into header rows, cluster
    header_spans = [s for s in spans if _is_header_span(s, cfg)]
    if not header_spans:
        return []
    header_rows = _group_into_rows(header_spans, y_tol)
    clusters = _cluster_header_rows(header_rows, cfg)

    x_tol = cfg.get("column_x_tolerance", 5.0)
    min_cols = cfg.get("min_columns", 2)
    min_rows = cfg.get("min_rows", 2)

    regions = []
    used_row_indices = set()

    for cluster in clusters:
        columns = _column_ranges_from_cluster(cluster, x_tol)

        # Header bbox + label info (used for both normal and headerless paths)
        all_header_spans = [s for row in cluster for s in row]
        hx0 = min(s["bbox"][0] for s in all_header_spans)
        hy0 = min(s["bbox"][1] for s in all_header_spans)
        hx1 = max(s["bbox"][2] for s in all_header_spans)
        hy1 = max(s["bbox"][3] for s in all_header_spans)
        header_bbox = (hx0, hy0, hx1, hy1)
        header_x_range = (hx0, hx1)

        # Find the index of the last header row in all_rows
        last_header_y = _row_y(cluster[-1])
        header_end_idx = None
        for i, row in enumerate(all_rows):
            if abs(_row_y(row) - last_header_y) <= y_tol:
                header_end_idx = i
        if header_end_idx is None:
            continue

        # Headerless fallback: single-span label cluster — derive columns from
        # the first column-aligned body row directly below.
        if len(columns) < min_cols:
            fallback = _try_headerless_columns(cluster, all_rows, header_end_idx, cfg)
            if fallback is None:
                continue
            columns, header_x_range, template_idx = fallback
            # Move header_end_idx so tracing starts AT the template row (it's
            # data, not header — we want it included in the region).
            header_end_idx = template_idx - 1

        # Trace data rows
        last_data_idx, data_count = _trace_data_rows(
            all_rows, header_end_idx, header_x_range, columns, cfg
        )

        total_rows = len(cluster) + data_count
        if total_rows < min_rows or data_count < 1:
            continue

        # Compute full region bbox
        rx0 = hx0
        ry0 = hy0
        rx1 = hx1
        ry1 = hy1
        for i in range(header_end_idx, last_data_idx + 1):
            for s in all_rows[i]:
                rx0 = min(rx0, s["bbox"][0])
                ry0 = min(ry0, s["bbox"][1])
                rx1 = max(rx1, s["bbox"][2])
                ry1 = max(ry1, s["bbox"][3])

        # Span indices belonging to this region
        region_spans = []
        for i in range(header_end_idx - len(cluster) + 1, last_data_idx + 1):
            if 0 <= i < len(all_rows):
                if i in used_row_indices:
                    continue
                for s in all_rows[i]:
                    if _x_overlap((s["bbox"][0], s["bbox"][2]), header_x_range):
                        region_spans.append(s["idx"])
                used_row_indices.add(i)

        regions.append(TableRegion(
            bbox=(rx0, ry0, rx1, ry1),
            header_bbox=header_bbox,
            columns=columns,
            row_count=total_rows,
            col_count=len(columns),
            header_row_count=len(cluster),
            span_indices=region_spans,
        ))

    return regions
