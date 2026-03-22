---
name: Always use printed page numbers from PDF
description: Never calculate page offsets - read page numbers directly from page text
type: feedback
---

When a PDF has a table of contents with page numbers, always read the actual printed page number from each page's text content to map content to chapters. Never calculate offsets between PDF page indices and printed page numbers.

**Why:** PDFs have unnumbered pages (covers, full-page artwork, inserts) that make offset math unreliable. The printed page number is on the page itself — just read it.

**How to apply:** Use pymupdf to extract text from each page and find the standalone page number (typically near the bottom or top). Use that number to look up which ToC chapter the page belongs to. Fall back to PDF page index only if no printed number is found. Always read page numbers when there is a ToC to reference.
