from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass
class PdfPageText:
    page_number: int
    text: str
    score: int = 0


HERWANTO_TERMS = [
    "herwanto",
    "muhammad herwanto",
    "muhammad_herwanto",
    "muhammad_herwanto_johari",
    "m. herwanto",
    "md herwanto",
    "t. mtl muhammad herwanto johari",
]

TIMETABLE_TERMS = [
    "timetable",
    "time table",
    "teacher",
    "teachers",
    "period",
    "mon",
    "tue",
    "wed",
    "thu",
    "fri",
    "odd",
    "even",
    "ml",
    "mtl",
    "malay",
    "bahasa",
    "class",
    "room",
]


def extract_pdf_pages(file_bytes: bytes) -> tuple[int, list[PdfPageText]]:
    """Extract text from a PDF, preserving page numbers."""
    try:
        import fitz
    except Exception as exc:
        raise RuntimeError("PyMuPDF is not installed. Add pymupdf to requirements.") from exc

    pages: list[PdfPageText] = []
    with fitz.open(stream=file_bytes, filetype="pdf") as pdf:
        page_count = pdf.page_count
        for idx, page in enumerate(pdf, start=1):
            text = page.get_text("text") or ""
            clean = "\n".join(line.rstrip() for line in text.splitlines() if line.strip())
            pages.append(PdfPageText(page_number=idx, text=clean))
    return page_count, pages


def _normalise_words(value: str) -> list[str]:
    return re.findall(r"[a-z0-9_.@-]+", (value or "").lower())


def score_pages(pages: Iterable[PdfPageText], extra_terms: Iterable[str] = ()) -> list[PdfPageText]:
    terms = [t.lower() for t in [*HERWANTO_TERMS, *TIMETABLE_TERMS, *extra_terms] if str(t).strip()]
    scored = []
    for page in pages:
        text = page.text.lower()
        score = 0
        for term in terms:
            if term in text:
                score += 8 if term in HERWANTO_TERMS else 2
        score += min(text.count("\n"), 80) // 8
        score += len(re.findall(r"\b(?:p(?:eriod)?\s*)?\d{1,2}\b", text)) // 3
        scored.append(PdfPageText(page.page_number, page.text, score))
    return sorted(scored, key=lambda item: (item.score, len(item.text)), reverse=True)


def build_pdf_excerpt(
    pages: list[PdfPageText],
    caption: str = "",
    max_pages: int = 12,
    max_chars_per_page: int = 3500,
) -> tuple[str, list[int], int]:
    """Return a compact text packet of the most relevant pages."""
    extra_terms = _normalise_words(caption)
    text_pages = [p for p in pages if p.text.strip()]
    ranked = score_pages(text_pages, extra_terms=extra_terms)
    selected = ranked[:max_pages]
    selected = sorted(selected, key=lambda item: item.page_number)

    chunks = []
    for page in selected:
        text = page.text.strip()
        if len(text) > max_chars_per_page:
            text = text[:max_chars_per_page].rstrip() + "\n...[page excerpt clipped]"
        chunks.append(f"--- Page {page.page_number} (relevance {page.score}) ---\n{text}")
    return "\n\n".join(chunks), [p.page_number for p in selected], len(text_pages)


def format_pdf_index(page_count: int, text_pages: int, selected_pages: list[int]) -> str:
    if selected_pages:
        selected = ", ".join(str(p) for p in selected_pages)
        return f"PDF has {page_count} pages; text found on {text_pages}; analysed pages: {selected}."
    return f"PDF has {page_count} pages; no extractable text found."
