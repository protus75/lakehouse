"""Font-switch table region detection.

Detects table bounding boxes on a PDF page using span (font, size, bold)
metadata from PyMuPDF. Body text uses one dominant style; table headers
use a different style (per-book, configured in YAML).

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
    Returns list of [header_row, ...] clusters."""
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
        if row_top - prev_bottom <= gap and _x_overlap(prev_x, row_x):
            current.append(row)
        else:
            clusters.append(current)
            current = [row]
    clusters.append(current)
    return clusters


def _column_ranges_from_cluster(cluster: list, x_tol: float, margin: float = 8.0) -> list:
    """Extract column (x0, x1) ranges from a header cluster.

    Uses the row with the most spans as the column template. Each header span
    becomes a column whose extent is the span's bbox x0..x1, padded by `margin`
    on each side to absorb right-aligned numeric cells that overhang slightly.
    Adjacent columns whose padded ranges overlap are merged.
    """
    if not cluster:
        return []
    best_row = max(cluster, key=lambda r: len(r))
    raw = sorted(((s["bbox"][0] - margin, s["bbox"][2] + margin) for s in best_row),
                 key=lambda r: r[0])
    merged = [raw[0]]
    for r in raw[1:]:
        if r[0] <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], r[1]))
        else:
            merged.append(r)
    return merged


def _row_matches_columns(row: list, columns: list, x_tol: float) -> int:
    """Count how many columns contain at least one word from this row.

    A word matches a column if its x-center falls within the column's (x0, x1)
    range. This handles left-, center-, and right-aligned cell content.
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
    x_tol = cfg.get("column_x_tolerance", 5.0)
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
        matched = _row_matches_columns(row, columns, x_tol)
        if matched >= min_cols:
            last_good = i
            included += 1
            misses = 0
        else:
            misses += 1
            if misses >= 2:
                break
    return last_good, included


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
        if len(columns) < min_cols:
            continue

        # Header bbox
        all_header_spans = [s for row in cluster for s in row]
        hx0 = min(s["bbox"][0] for s in all_header_spans)
        hy0 = min(s["bbox"][1] for s in all_header_spans)
        hx1 = max(s["bbox"][2] for s in all_header_spans)
        hy1 = max(s["bbox"][3] for s in all_header_spans)
        header_bbox = (hx0, hy0, hx1, hy1)
        header_x_range = (hx0, hx1)

        # Find the index of the last header row in all_rows
        # (header_rows last entry's y matches some all_rows entry)
        last_header_y = _row_y(cluster[-1])
        header_end_idx = None
        for i, row in enumerate(all_rows):
            if abs(_row_y(row) - last_header_y) <= y_tol:
                header_end_idx = i
        if header_end_idx is None:
            continue

        # Trace data rows
        last_data_idx, data_count = _trace_data_rows(
            all_rows, header_end_idx, header_x_range, columns, cfg
        )

        total_rows = len(cluster) + data_count
        if total_rows < min_rows:
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
