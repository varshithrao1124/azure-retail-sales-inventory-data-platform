"""Render a captured terminal transcript to a PNG. The text file is the source of truth."""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def render(text: str, destination: Path, title: str) -> None:
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
    font = ImageFont.truetype(font_path, 14)
    title_font = ImageFont.truetype(font_path, 16)
    lines = text.replace("\t", "    ").splitlines() or [""]
    width = max(title_font.getlength(title), *(font.getlength(line) for line in lines))
    line_h = 18
    image = Image.new("RGB", (int(width) + 36, 48 + line_h * len(lines) + 16), "#0f1720")
    draw = ImageDraw.Draw(image)
    draw.text((16, 12), title, fill="#f8fafc", font=title_font)
    y = 40
    for line in lines:
        draw.text((16, y), line, fill="#d6f5d6", font=font)
        y += line_h
    destination.parent.mkdir(parents=True, exist_ok=True)
    image.save(destination)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--title", default="local execution")
    args = parser.parse_args()
    render(args.source.read_text(encoding="utf-8"), args.destination, args.title)


if __name__ == "__main__":
    main()
