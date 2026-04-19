"""Render RenderMessage list to HTML."""
from datetime import datetime, timezone
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape

_TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES)),
    autoescape=select_autoescape(["html", "j2"]),
    trim_blocks=True, lstrip_blocks=True,
)


def render_thread_html(messages, thread_name: str, exported_at: datetime | None = None) -> str:
    tpl = _env.get_template("thread.html.j2")
    return tpl.render(
        messages=messages,
        thread_name=thread_name,
        exported_at=(exported_at or datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M UTC"),
    )


def render_message_html(message, thread_name: str, exported_at: datetime | None = None) -> str:
    tpl = _env.get_template("message.html.j2")
    return tpl.render(
        msg=message,
        thread_name=thread_name,
        exported_at=(exported_at or datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M UTC"),
    )
