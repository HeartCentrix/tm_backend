"""PDF rendering via WeasyPrint. Uses the same HTML template; pulls styles.css
from the templates dir as base_url so relative assets resolve."""
from pathlib import Path
from datetime import datetime
from weasyprint import HTML, CSS
from workers.chat_export_worker.render.html_renderer import render_thread_html, render_message_html

_TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
_STYLES_PATH = _TEMPLATES / "styles.css"


def render_thread_pdf(messages, thread_name: str, exported_at: datetime | None = None) -> bytes:
    html = render_thread_html(messages, thread_name, exported_at)
    return HTML(string=html, base_url=str(_TEMPLATES)).write_pdf(
        stylesheets=[CSS(filename=str(_STYLES_PATH))],
    )


def render_message_pdf(message, thread_name: str, exported_at: datetime | None = None) -> bytes:
    html = render_message_html(message, thread_name, exported_at)
    return HTML(string=html, base_url=str(_TEMPLATES)).write_pdf(
        stylesheets=[CSS(filename=str(_STYLES_PATH))],
    )
