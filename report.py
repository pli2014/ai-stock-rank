
from datetime import datetime
from pathlib import Path
from typing import Iterable

from jinja2 import Environment, FileSystemLoader, select_autoescape

from src.analyzer import StockTrend


env = Environment(
    loader=FileSystemLoader(searchpath=Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
)


def render_report(trends: Iterable[StockTrend], output_path: str = "report.html") -> None:
    template = env.get_template("report.html")
    html = template.render(stocks=list(trends), generated_at=datetime.now())
    Path(output_path).write_text(html, encoding="utf-8")
