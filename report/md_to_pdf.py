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
body { font-family: 'Times New Roman', Times, serif; font-size: 11pt; color: #000000; line-height: 1.4; }
h1 { font-size: 19pt; color: #000000; margin: 0 0 4pt 0; }
h2 { font-size: 14pt; color: #000000; border-bottom: 1px solid #000000; padding-bottom: 3px; margin-top: 18px; }
h3 { font-size: 12pt; color: #000000; margin-top: 12px; }
p { margin: 5px 0; text-align: justify; }
a { color: #000000; text-decoration: underline; }
table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 10pt; }
th { background-color: #ffffff; color: #000000; padding: 5px 7px; text-align: left; border: 0.5pt solid #000000; }
td { border: 0.5pt solid #000000; padding: 4px 7px; }
tr:nth-child(even) td { background-color: #ffffff; }
code { font-family: 'Times New Roman', Times, serif; font-size: 10pt; background-color: #ffffff; padding: 0 2px; }
pre { background-color: #ffffff; border: 0.5pt solid #000000; padding: 8px; font-family: 'Courier New', Courier, monospace; font-size: 8.5pt; overflow: hidden; color: #000000; }
pre code { background: none; font-family: 'Courier New', Courier, monospace; }
img { width: 62%; display: block; margin: 6px auto; }
hr { border: none; border-top: 1px solid #000000; }
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
