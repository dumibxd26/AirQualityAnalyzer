"""Convert RAPORT.md to PDF by rendering HTML with headless Microsoft Edge.

xhtml2pdf cannot embed @font-face fonts reliably (Romanian diacritics render as
black boxes). Edge/Chromium renders HTML with native Windows fonts (Segoe UI),
which cover Romanian diacritics fully, and prints to PDF deterministically.
"""
import os
import subprocess
import sys
import time

import markdown

HERE = os.path.dirname(os.path.abspath(__file__))
MD_PATH = os.path.join(HERE, "RAPORT.md")
HTML_PATH = os.path.join(HERE, "_raport.html")
PDF_PATH = os.path.join(HERE, "RAPORT.pdf")

EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
if not os.path.isfile(EDGE):
    EDGE = r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"

CSS = """
@page { size: A4; margin: 14mm 15mm; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10pt; color: #1a1a1a; line-height: 1.4; }
h1 { font-size: 19pt; color: #14324f; margin: 0 0 4pt 0; }
h2 { font-size: 14pt; color: #14324f; border-bottom: 1px solid #c9d3dc; padding-bottom: 3px; margin-top: 18px; }
h3 { font-size: 11.5pt; color: #1f4763; margin-top: 12px; }
p { margin: 5px 0; text-align: justify; }
a { color: #1763a6; text-decoration: none; }
table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 9pt; }
th { background-color: #14324f; color: #ffffff; padding: 5px 7px; text-align: left; }
td { border: 0.5pt solid #c9d3dc; padding: 4px 7px; }
tr:nth-child(even) td { background-color: #f2f5f8; }
code { font-family: 'Cascadia Code', Consolas, monospace; font-size: 8.5pt; background-color: #eef1f4; padding: 0 2px; }
pre { background-color: #f2f5f8; border: 0.5pt solid #d4dbe2; padding: 8px; font-family: Consolas, monospace; font-size: 8pt; overflow: hidden; }
pre code { background: none; }
img { width: 62%; display: block; margin: 6px auto; }
hr { border: none; border-top: 1px solid #c9d3dc; }
h2 { page-break-after: avoid; }
table, pre, img { page-break-inside: avoid; }
"""


def main():
    with open(MD_PATH, encoding="utf-8") as f:
        text = f.read()

    body = markdown.markdown(text, extensions=["tables", "fenced_code"])
    html = (
        "<!DOCTYPE html><html lang='ro'><head><meta charset='utf-8'>"
        f"<style>{CSS}</style></head><body>{body}</body></html>"
    )
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    if os.path.exists(PDF_PATH):
        os.remove(PDF_PATH)

    subprocess.run(
        [
            EDGE,
            "--headless",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={PDF_PATH}",
            "file:///" + HTML_PATH.replace("\\", "/"),
        ],
        check=True,
        timeout=120,
    )

    for _ in range(40):
        if os.path.exists(PDF_PATH) and os.path.getsize(PDF_PATH) > 0:
            break
        time.sleep(0.25)

    if not os.path.exists(PDF_PATH):
        print("[pdf] Edge did not produce a PDF", file=sys.stderr)
        sys.exit(1)
    print(f"[pdf] wrote {PDF_PATH} ({os.path.getsize(PDF_PATH):,} bytes)")


if __name__ == "__main__":
    main()
