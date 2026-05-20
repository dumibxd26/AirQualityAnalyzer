"""Generate AirQualityAnalyzer.pptx -- a polished slide deck for the project.

Run inside a python:3.11 container with python-pptx installed:
  docker run --rm -v ${PWD}:/w -w /w python:3.11-slim sh -c \
    "pip install --quiet python-pptx==1.0.2 && python make_deck.py"
"""
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

# -------------------- Theme --------------------
BG        = RGBColor(0x0B, 0x12, 0x1A)   # deep navy
PANEL     = RGBColor(0x12, 0x1C, 0x2A)
ACCENT    = RGBColor(0x4D, 0xC4, 0xFF)   # cyan
ACCENT_2  = RGBColor(0xFF, 0x7A, 0x59)   # orange
GOOD      = RGBColor(0x4A, 0xD3, 0x95)
MODERATE  = RGBColor(0xF4, 0xC3, 0x4B)
HIGH      = RGBColor(0xFF, 0x9F, 0x43)
CRITICAL  = RGBColor(0xE7, 0x4C, 0x3C)
TEXT      = RGBColor(0xEA, 0xF2, 0xFA)
MUTED     = RGBColor(0x9A, 0xAB, 0xBE)

SLIDE_W, SLIDE_H = Inches(13.333), Inches(7.5)

prs = Presentation()
prs.slide_width  = SLIDE_W
prs.slide_height = SLIDE_H

BLANK = prs.slide_layouts[6]


# -------------------- Helpers --------------------
def add_bg(slide, color=BG):
    bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, SLIDE_W, SLIDE_H)
    bg.line.fill.background()
    bg.fill.solid(); bg.fill.fore_color.rgb = color
    bg.shadow.inherit = False
    return bg


def add_text(slide, x, y, w, h, text, *, size=18, bold=False,
             color=TEXT, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, font="Segoe UI"):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(0.05)
    tf.vertical_anchor = anchor
    lines = text.split("\n") if isinstance(text, str) else text
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        r = p.add_run(); r.text = line
        r.font.name = font
        r.font.size = Pt(size)
        r.font.bold = bold
        r.font.color.rgb = color
    return tb


def add_bullets(slide, x, y, w, h, items, *, size=16, color=TEXT, font="Segoe UI"):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.LEFT
        p.space_after = Pt(6)
        r = p.add_run(); r.text = f"\u2022  {item}"
        r.font.name = font
        r.font.size = Pt(size)
        r.font.color.rgb = color
    return tb


def add_panel(slide, x, y, w, h, *, fill=PANEL, line=ACCENT):
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    sh.adjustments[0] = 0.06
    sh.fill.solid(); sh.fill.fore_color.rgb = fill
    sh.line.color.rgb = line
    sh.line.width = Pt(0.75)
    sh.shadow.inherit = False
    return sh


def add_chip(slide, x, y, w, h, text, *, fill=ACCENT, fg=BG):
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    sh.adjustments[0] = 0.5
    sh.fill.solid(); sh.fill.fore_color.rgb = fill
    sh.line.fill.background()
    sh.shadow.inherit = False
    tf = sh.text_frame
    tf.margin_left = tf.margin_right = Inches(0.05)
    tf.margin_top = tf.margin_bottom = Inches(0.02)
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    p = tf.paragraphs[0]; p.alignment = PP_ALIGN.CENTER
    r = p.add_run(); r.text = text
    r.font.name = "Segoe UI"; r.font.size = Pt(11); r.font.bold = True
    r.font.color.rgb = fg
    return sh


def add_arrow(slide, x1, y1, x2, y2, color=ACCENT):
    conn = slide.shapes.add_connector(2, x1, y1, x2, y2)  # 2 = straight
    conn.line.color.rgb = color
    conn.line.width = Pt(2.25)
    conn.shadow.inherit = False
    from pptx.oxml.ns import qn
    from lxml import etree
    line_el = conn.line._get_or_add_ln()
    tail = etree.SubElement(line_el, qn('a:tailEnd'))
    tail.set('type', 'triangle')
    tail.set('w', 'med'); tail.set('len', 'med')
    return conn


def slide_header(slide, title, subtitle=None):
    add_bg(slide)
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(0.18), SLIDE_H)
    bar.fill.solid(); bar.fill.fore_color.rgb = ACCENT
    bar.line.fill.background(); bar.shadow.inherit = False
    add_text(slide, Inches(0.5), Inches(0.35), Inches(12), Inches(0.7),
             title, size=30, bold=True, color=TEXT)
    if subtitle:
        add_text(slide, Inches(0.5), Inches(1.0), Inches(12), Inches(0.45),
                 subtitle, size=15, color=MUTED)
    div = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.5), Inches(1.55),
                                 Inches(12.3), Emu(12700))
    div.fill.solid(); div.fill.fore_color.rgb = ACCENT
    div.line.fill.background(); div.shadow.inherit = False


def footer(slide, page_num):
    add_text(slide, Inches(0.5), Inches(7.05), Inches(6), Inches(0.3),
             "AirQualityAnalyzer  \u00b7  Kafka \u2192 Flink \u2192 WebSocket \u2192 Browser",
             size=10, color=MUTED)
    add_text(slide, Inches(12.0), Inches(7.05), Inches(0.8), Inches(0.3),
             f"{page_num:02d}", size=10, color=MUTED, align=PP_ALIGN.RIGHT)


# -------------------- Slide 1: Title --------------------
s = prs.slides.add_slide(BLANK)
add_bg(s)
add_text(s, Inches(0.7), Inches(2.6), Inches(12), Inches(1.2),
         "AirQualityAnalyzer", size=64, bold=True, color=TEXT)
add_text(s, Inches(0.7), Inches(3.7), Inches(12), Inches(0.6),
         "Real-time global air-quality intelligence",
         size=24, color=ACCENT)
add_text(s, Inches(0.7), Inches(4.4), Inches(12), Inches(0.5),
         "OpenAQ  +  Open-Meteo   \u2192   Kafka   \u2192   Apache Flink   \u2192   WebSocket   \u2192   Live Dashboard",
         size=14, color=MUTED)

chips = ["Apache Kafka", "Apache Flink (PyFlink)", "FastAPI", "Leaflet + Chart.js", "Docker Compose"]
x = Inches(0.7)
for c in chips:
    w = Inches(1.9)
    add_chip(s, x, Inches(5.6), w, Inches(0.4), c)
    x += w + Inches(0.15)

add_text(s, Inches(0.7), Inches(6.6), Inches(12), Inches(0.3),
         "Streaming data pipeline \u00b7 windowed analytics \u00b7 alert escalation",
         size=12, color=MUTED)


# -------------------- Slide 2: Problem & Goals --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "The problem",
             "Air-quality data is plentiful but rarely turned into actionable, live intelligence.")
add_bullets(s, Inches(0.6), Inches(1.9), Inches(6.2), Inches(5), [
    "Thousands of global sensors publish pollutant readings (pm25, pm10, no2, o3, so2, co).",
    "Raw data is hourly, noisy, geographically scattered, and lacks weather context.",
    "Operators need: live map, rolling averages, threshold alerts, escalation of sustained breaches.",
    "Requires a fault-tolerant streaming backbone, not a polling REST app.",
], size=17)

add_panel(s, Inches(7.1), Inches(1.9), Inches(5.6), Inches(4.6))
add_text(s, Inches(7.35), Inches(2.05), Inches(5.2), Inches(0.4),
         "Project goals", size=18, bold=True, color=ACCENT)
add_bullets(s, Inches(7.35), Inches(2.55), Inches(5.2), Inches(4), [
    "Ingest two heterogeneous live sources concurrently.",
    "Compute per-station, per-pollutant 1-minute averages.",
    "Join weather context onto each window (interval join).",
    "Raise alerts when averages cross WHO-style thresholds.",
    "Escalate to CRITICAL when 3+ windows breach in 5 minutes.",
    "Stream everything to a live dashboard via WebSocket.",
    "End-to-end exactly-once delivery on Kafka sinks.",
], size=15)
footer(s, 2)


# -------------------- Slide 3: Architecture --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "End-to-end architecture",
             "Five containerised services, one Kafka backbone, six topics.")

y = Inches(2.6)
h = Inches(1.4)
w = Inches(1.9)
gap = Inches(0.2)
x0 = Inches(0.5)

components = [
    ("OpenAQ\nproducer",      "Python\n6 pollutants",   ACCENT),
    ("Open-Meteo\nproducer",  "Python\nweather poll",   ACCENT),
    ("Apache\nKafka",         "5 topics\n12 partitions",ACCENT_2),
    ("Apache\nFlink",         "PyFlink Table API\n8 slots", ACCENT_2),
    ("FastAPI\nbackend",      "WebSocket bridge\naiokafka", ACCENT),
    ("Dashboard\n(nginx)",    "Leaflet + Chart.js",     ACCENT),
]
xs = []
for i, (title, sub, col) in enumerate(components):
    x = x0 + i * (w + gap)
    xs.append(x)
    add_panel(s, x, y, w, h, line=col)
    add_text(s, x, y + Inches(0.2), w, Inches(0.6), title,
             size=14, bold=True, color=col, align=PP_ALIGN.CENTER)
    add_text(s, x, y + Inches(0.85), w, Inches(0.5), sub,
             size=10, color=MUTED, align=PP_ALIGN.CENTER)

mid_y = y + Inches(0.7)
for i in range(len(components) - 1):
    x_from = xs[i] + w
    x_to = xs[i + 1]
    add_arrow(s, x_from, mid_y, x_to, mid_y)

add_text(s, Inches(0.5), Inches(4.6), Inches(12), Inches(0.4),
         "Kafka topics", size=15, bold=True, color=ACCENT)
topics = [
    ("raw-air-quality",   "12 part", ACCENT),
    ("weather-stream",    "12 part", ACCENT),
    ("enriched-readings", "6 part",  ACCENT_2),
    ("pollution-alerts",  "6 part",  HIGH),
    ("critical-alerts",   "3 part",  CRITICAL),
]
x = Inches(0.5)
for name, parts, col in topics:
    w_ = Inches(2.4)
    add_panel(s, x, Inches(5.05), w_, Inches(0.7), line=col)
    add_text(s, x, Inches(5.12), w_, Inches(0.3), name,
             size=13, bold=True, color=TEXT, align=PP_ALIGN.CENTER)
    add_text(s, x, Inches(5.42), w_, Inches(0.3), parts,
             size=11, color=MUTED, align=PP_ALIGN.CENTER)
    x += w_ + Inches(0.1)

add_text(s, Inches(0.5), Inches(6.0), Inches(12.3), Inches(1),
         "Records are keyed by location \u2192 deterministic partitioning, in-order consumption per station.\n"
         "Auto-topic-creation is disabled; topics are pre-created by a one-shot kafka-init container.",
         size=12, color=MUTED)
footer(s, 3)


# -------------------- Slide 4: Processing pipeline flow --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Processing pipeline \u2014 end-to-end flow",
             "Two live sources \u2192 Kafka \u2192 Flink analytics \u2192 WebSocket \u2192 browser.")

# ----- helpers local to this slide -----
def _box(slide, cx, cy, w, h, label, *, fill=PANEL, line=ACCENT,
         label_color=TEXT, label_size=12, bold=True, tag=None, tag_color=None):
    """Centered rounded box with a label. cx, cy is the centre."""
    x = cx - w / 2
    y = cy - h / 2
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    sh.adjustments[0] = 0.18
    sh.fill.solid(); sh.fill.fore_color.rgb = fill
    sh.line.color.rgb = line
    sh.line.width = Pt(1.25)
    sh.shadow.inherit = False
    tb = sh.text_frame
    tb.margin_left = tb.margin_right = Inches(0.05)
    tb.margin_top = tb.margin_bottom = Inches(0.03)
    tb.vertical_anchor = MSO_ANCHOR.MIDDLE
    tb.word_wrap = True
    p = tb.paragraphs[0]; p.alignment = PP_ALIGN.CENTER
    if tag:
        r = p.add_run(); r.text = tag + "\n"
        r.font.name = "Segoe UI"; r.font.size = Pt(9); r.font.bold = True
        r.font.color.rgb = tag_color or MUTED
    r = p.add_run(); r.text = label
    r.font.name = "Segoe UI"; r.font.size = Pt(label_size); r.font.bold = bold
    r.font.color.rgb = label_color
    return (x, y, w, h)


def _arrow_v(slide, cx, y1, y2, color=ACCENT, dashed=False, width=1.75):
    conn = slide.shapes.add_connector(2, cx, y1, cx, y2)
    conn.line.color.rgb = color
    conn.line.width = Pt(width)
    conn.shadow.inherit = False
    from pptx.oxml.ns import qn
    from lxml import etree
    line_el = conn.line._get_or_add_ln()
    if dashed:
        dash = etree.SubElement(line_el, qn('a:prstDash'))
        dash.set('val', 'dash')
    tail = etree.SubElement(line_el, qn('a:tailEnd'))
    tail.set('type', 'triangle'); tail.set('w', 'med'); tail.set('len', 'med')
    return conn


def _arrow_xy(slide, x1, y1, x2, y2, color=ACCENT, dashed=False, width=1.5,
              label=None, label_offset=(Inches(0.05), Inches(-0.25))):
    conn = slide.shapes.add_connector(2, x1, y1, x2, y2)
    conn.line.color.rgb = color
    conn.line.width = Pt(width)
    conn.shadow.inherit = False
    from pptx.oxml.ns import qn
    from lxml import etree
    line_el = conn.line._get_or_add_ln()
    if dashed:
        dash = etree.SubElement(line_el, qn('a:prstDash'))
        dash.set('val', 'dash')
    tail = etree.SubElement(line_el, qn('a:tailEnd'))
    tail.set('type', 'triangle'); tail.set('w', 'med'); tail.set('len', 'med')
    if label:
        midx = (x1 + x2) // 2 + label_offset[0]
        midy = (y1 + y2) // 2 + label_offset[1]
        add_text(slide, midx, midy, Inches(2.4), Inches(0.3), label,
                 size=9, color=MUTED)
    return conn


# ----- columns -----
LEFT_X  = Inches(3.2)
RIGHT_X = Inches(10.2)
BOX_W   = Inches(2.9)
BOX_H   = Inches(0.55)

# Y centres for each row
Y_API     = Inches(1.95)
Y_PROD    = Inches(2.65)
Y_TOPIC1  = Inches(3.35)
Y_FLINK   = Inches(4.20)
Y_SINKS   = Inches(5.05)
Y_BACK    = Inches(5.85)
Y_UI      = Inches(6.55)

# ----- row 1: APIs -----
_box(s, LEFT_X,  Y_API, BOX_W, BOX_H, "OpenAQ API",     line=ACCENT,   tag="DATA SOURCE", tag_color=ACCENT)
_box(s, RIGHT_X, Y_API, BOX_W, BOX_H, "Open-Meteo API", line=ACCENT,   tag="DATA SOURCE", tag_color=ACCENT)

# ----- row 2: producers -----
_box(s, LEFT_X,  Y_PROD, BOX_W, BOX_H, "producer.py",         line=ACCENT_2, tag="PYTHON PRODUCER", tag_color=ACCENT_2)
_box(s, RIGHT_X, Y_PROD, BOX_W, BOX_H, "weather_producer.py", line=ACCENT_2, tag="PYTHON PRODUCER", tag_color=ACCENT_2)

# ----- row 3: input Kafka topics -----
_box(s, LEFT_X,  Y_TOPIC1, BOX_W, BOX_H, "raw-air-quality", line=ACCENT, tag="KAFKA TOPIC \u00b7 12 part", tag_color=ACCENT)
_box(s, RIGHT_X, Y_TOPIC1, BOX_W, BOX_H, "weather-stream",  line=ACCENT, tag="KAFKA TOPIC \u00b7 12 part", tag_color=ACCENT)

# ----- row 4: Flink job (wide, spans both columns) -----
FLINK_CX = Inches(6.7)
FLINK_W  = Inches(9.0)
FLINK_H  = Inches(0.7)
_box(s, FLINK_CX, Y_FLINK, FLINK_W, FLINK_H,
     "Apache Flink  \u00b7  1-min TUMBLE averages  \u00b7  threshold check  \u00b7  weather interval-join  \u00b7  breach escalation",
     line=ACCENT_2, fill=PANEL, label_size=12, tag="STREAMING ANALYTICS", tag_color=ACCENT_2)

# ----- row 5: sink topics (3 columns) -----
SINK_W = Inches(3.7)
SINK_CX_LEFT   = Inches(2.4)
SINK_CX_MID    = Inches(6.7)
SINK_CX_RIGHT  = Inches(11.0)
_box(s, SINK_CX_LEFT,  Y_SINKS, SINK_W, BOX_H, "pollution-alerts",  line=HIGH,     tag="KAFKA TOPIC \u00b7 6 part",  tag_color=HIGH)
_box(s, SINK_CX_MID,   Y_SINKS, SINK_W, BOX_H, "enriched-readings", line=ACCENT,   tag="KAFKA TOPIC \u00b7 6 part",  tag_color=ACCENT)
_box(s, SINK_CX_RIGHT, Y_SINKS, SINK_W, BOX_H, "critical-alerts",   line=CRITICAL, tag="KAFKA TOPIC \u00b7 3 part",  tag_color=CRITICAL)

# ----- row 6: FastAPI -----
_box(s, FLINK_CX, Y_BACK, Inches(5.0), BOX_H, "FastAPI backend  \u00b7  aiokafka  \u00b7  WebSocket bridge",
     line=ACCENT, tag="PYTHON SERVICE", tag_color=ACCENT, label_size=12)

# ----- row 7: Browser -----
_box(s, FLINK_CX, Y_UI, Inches(5.0), BOX_H, "Browser dashboard  \u00b7  Leaflet + Chart.js",
     line=ACCENT_2, tag="WEBSOCKET CLIENT", tag_color=ACCENT_2, label_size=12)

# =============== arrows ===============
half = BOX_H / 2

# APIs -> producers (vertical, both columns)
_arrow_v(s, LEFT_X,  Y_API  + half, Y_PROD - half)
_arrow_v(s, RIGHT_X, Y_API  + half, Y_PROD - half)

# Producers -> input topics
_arrow_v(s, LEFT_X,  Y_PROD + half, Y_TOPIC1 - half)
_arrow_v(s, RIGHT_X, Y_PROD + half, Y_TOPIC1 - half)

# Discovery arrow: raw-air-quality (left) ----dashed----> weather_producer (right, row above)
_arrow_xy(
    s,
    LEFT_X + BOX_W / 2, Y_TOPIC1,           # right edge of raw-air-quality
    RIGHT_X - BOX_W / 2, Y_PROD,            # left edge of weather_producer
    color=MUTED, dashed=True, width=1.25,
    label="station discovery",
    label_offset=(Inches(-2.7), Inches(-0.05)),
)

# Input topics -> Flink (both sides converge into the wide bar)
_arrow_xy(s, LEFT_X,  Y_TOPIC1 + half, FLINK_CX - Inches(2.5), Y_FLINK - FLINK_H / 2)
_arrow_xy(s, RIGHT_X, Y_TOPIC1 + half, FLINK_CX + Inches(2.5), Y_FLINK - FLINK_H / 2)

# Flink -> 3 sink topics
_arrow_xy(s, FLINK_CX - Inches(2.5), Y_FLINK + FLINK_H / 2, SINK_CX_LEFT,  Y_SINKS - half, color=HIGH)
_arrow_xy(s, FLINK_CX,               Y_FLINK + FLINK_H / 2, SINK_CX_MID,   Y_SINKS - half, color=ACCENT)
_arrow_xy(s, FLINK_CX + Inches(2.5), Y_FLINK + FLINK_H / 2, SINK_CX_RIGHT, Y_SINKS - half, color=CRITICAL)

# 3 sink topics -> FastAPI backend
_arrow_xy(s, SINK_CX_LEFT,  Y_SINKS + half, FLINK_CX - Inches(1.8), Y_BACK - half, color=HIGH)
_arrow_xy(s, SINK_CX_MID,   Y_SINKS + half, FLINK_CX,               Y_BACK - half, color=ACCENT)
_arrow_xy(s, SINK_CX_RIGHT, Y_SINKS + half, FLINK_CX + Inches(1.8), Y_BACK - half, color=CRITICAL)

# Backend -> Browser
_arrow_v(s, FLINK_CX, Y_BACK + half, Y_UI - half, color=ACCENT_2)

footer(s, 4)


# -------------------- Slide 5: Data sources --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Heterogeneous live data sources",
             "Two concurrent producers, one shared partition key.")

add_panel(s, Inches(0.5), Inches(1.9), Inches(6.1), Inches(5))
add_text(s, Inches(0.75), Inches(2.0), Inches(5.5), Inches(0.4),
         "OpenAQ \u2014 air-quality readings", size=18, bold=True, color=ACCENT)
add_bullets(s, Inches(0.75), Inches(2.55), Inches(5.6), Inches(4.5), [
    "v3 REST API, /parameters/{id}/latest",
    "6 pollutants polled concurrently (ThreadPoolExecutor)",
    "Up to 200 stations per pollutant per poll",
    "TTL-deduplicates by (station, parameter, event-time)",
    "Drops readings older than MAX_AGE_HOURS (=6h default)",
    "Rewrites Kafka record timestamp to ingest-time so Flink watermarks advance",
    "Catches BufferError / KafkaTimeoutError (no silent crashes)",
], size=14)

add_panel(s, Inches(6.75), Inches(1.9), Inches(6.1), Inches(5), line=ACCENT_2)
add_text(s, Inches(7.0), Inches(2.0), Inches(5.5), Inches(0.4),
         "Open-Meteo \u2014 weather context", size=18, bold=True, color=ACCENT_2)
add_bullets(s, Inches(7.0), Inches(2.55), Inches(5.6), Inches(4.5), [
    "Free, no-auth weather API",
    "Discovers stations dynamically from the raw-air-quality topic",
    "Polls each known station every 60 seconds",
    "Publishes {location, temperature, wind_speed, humidity, timestamp}",
    "Same `location` key \u2192 weather + readings co-partitioned",
    "Enables an event-time INTERVAL JOIN in Flink",
], size=14)
footer(s, 5)


# -------------------- Slide 6: Flink job --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Flink analytics pipeline",
             "Single PyFlink job, four sinks, one statement set.")

add_panel(s, Inches(0.5), Inches(1.9), Inches(7.6), Inches(5.0))
add_text(s, Inches(0.75), Inches(2.0), Inches(7), Inches(0.4),
         "Logical plan", size=18, bold=True, color=ACCENT)
add_bullets(s, Inches(0.75), Inches(2.55), Inches(7.1), Inches(4.5), [
    "Source tables: RawAirQuality, Weather (Kafka JSON, event-time watermarks).",
    "MinuteAggregates view \u2014 1-min TUMBLE per (station, parameter): AVG(value), COUNT(*), MAX(lat/lon).",
    "alerts_sql: filter windows whose avg exceeds WHO-style threshold per pollutant.",
    "enriched_sql: stream-stream INTERVAL JOIN with Weather \u00b15 min on `location`.",
    "critical_sql: TUMBLE 5-min over breach stream, HAVING COUNT(*) \u2265 3.",
    "All sinks executed in one StatementSet \u2192 single Flink job, shared state.",
], size=14)

add_panel(s, Inches(8.25), Inches(1.9), Inches(4.55), Inches(5.0), line=ACCENT_2)
add_text(s, Inches(8.5), Inches(2.0), Inches(4), Inches(0.4),
         "Thresholds (\u00b5g/m\u00b3)", size=18, bold=True, color=ACCENT_2)
thresholds = [
    ("pm25", "15"),  ("pm10", "45"), ("no2", "25"),
    ("o3",   "60"),  ("so2",  "40"), ("co",  "4"),
]
y = Inches(2.6)
for name, val in thresholds:
    add_text(s, Inches(8.5), y, Inches(2), Inches(0.4), name, size=14, color=TEXT)
    add_text(s, Inches(10.5), y, Inches(2), Inches(0.4), val, size=14, bold=True, color=ACCENT_2)
    y += Inches(0.4)

add_text(s, Inches(8.5), Inches(5.4), Inches(4), Inches(0.4),
         "Severity scale", size=14, bold=True, color=TEXT)
for label, col in [("MODERATE  > 1\u00d7", MODERATE),
                   ("HIGH      > 2\u00d7", HIGH),
                   ("CRITICAL  > 3\u00d7", CRITICAL)]:
    add_text(s, Inches(8.5), y, Inches(4), Inches(0.35), label, size=12, color=col)
    y += Inches(0.3)
footer(s, 6)


# -------------------- Slide 7: Backend + UI --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "FastAPI bridge & dashboard",
             "From Kafka to browser in milliseconds.")

add_panel(s, Inches(0.5), Inches(1.9), Inches(6.1), Inches(5.0))
add_text(s, Inches(0.75), Inches(2.0), Inches(5.5), Inches(0.4),
         "FastAPI WebSocket bridge", size=18, bold=True, color=ACCENT)
add_bullets(s, Inches(0.75), Inches(2.55), Inches(5.6), Inches(4.5), [
    "4 background asyncio tasks (aiokafka consumers).",
    "Fan-out via in-process broadcast() to every connected WebSocket.",
    "Alert topics replay from `earliest` on each restart \u2192 fresh clients see history.",
    "/snapshot REST endpoint hydrates the UI on first load.",
    "Maintains in-memory STATIONS + RECENT_ALERTS / CRITICAL buffers.",
], size=14)

add_panel(s, Inches(6.75), Inches(1.9), Inches(6.1), Inches(5.0), line=ACCENT_2)
add_text(s, Inches(7.0), Inches(2.0), Inches(5.5), Inches(0.4),
         "Dashboard (vanilla JS + Leaflet + Chart.js)", size=18, bold=True, color=ACCENT_2)
add_bullets(s, Inches(7.0), Inches(2.55), Inches(5.6), Inches(4.5), [
    "Glassmorphism dark UI, dark Carto map tiles.",
    "6 live stat cards refresh every 1 s (rate, total, last-age, alerts, criticals).",
    "Map dots colored by Flink-windowed averages \u2192 agree with alert thresholds.",
    "Per-parameter rolling chart, 5-second aggregation buckets.",
    "Toasts for critical episodes; per-parameter TTL fades dots back to green.",
], size=14)
footer(s, 7)


# -------------------- Slide 8: Reliability features --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Reliability & correctness",
             "Production-flavoured guarantees, not just a demo.")

items = [
    ("Exactly-once sinks",
     "All 3 Kafka sinks: sink.delivery-guarantee=exactly-once, unique transactional-id-prefix, 15-min tx timeout. Paired with Flink CheckpointingMode.EXACTLY_ONCE every 30 s."),
    ("Event-time watermarks",
     "Producer rewrites record timestamp to ingest-time \u2192 watermark advances reliably even when source data is hours-old."),
    ("Deduplication w/ TTL",
     "Producer dedupes by (location, parameter, event_time) for 90 s, then republishes \u2192 prevents the 1190\u00d7/min firehose while keeping windows fed."),
    ("Stale-data filter",
     "Drops readings older than MAX_AGE_HOURS (=6 h) \u2014 OpenAQ /latest returns multi-year-old values from defunct stations."),
    ("Parallel + partitioned",
     "12 partitions on hot topics, parallelism.default=4, 2 TaskManagers \u00d7 4 slots; records keyed by location \u2192 per-station ordering."),
    ("Bounded back-pressure",
     "Producer uses max_block_ms=5 s and catches BufferError/KafkaTimeoutError \u2192 broker hiccups don't crash ingest."),
]
y = Inches(1.85)
for i, (head, body) in enumerate(items):
    row_y = y + Inches(i * 0.85)
    add_panel(s, Inches(0.5), row_y, Inches(12.3), Inches(0.75))
    add_text(s, Inches(0.7), row_y + Inches(0.07), Inches(3.5), Inches(0.4),
             head, size=14, bold=True, color=ACCENT)
    add_text(s, Inches(4.4), row_y + Inches(0.07), Inches(8.3), Inches(0.65),
             body, size=11, color=TEXT)
footer(s, 8)


# -------------------- Slide 9: Bug audit & fixes --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Bug audit \u2014 8 fixes shipped",
             "Empirical audit via kafka-console-consumer + Flink job inspection.")

rows = [
    ("#1", "Stale OpenAQ timestamps (years old)", "Producer drops records >MAX_AGE_HOURS; rewrites Kafka ts to ingest-time."),
    ("#2", "Same readings re-published every 15 s (\u00d71190)", "(location, parameter, event_time) dedup table with TTL."),
    ("#3", "UI severity color \u2260 Flink alert thresholds", "Map driven by enriched-readings window AVG (source='enriched')."),
    ("#4", "HOP window emits 5\u00d7 duplicate criticals", "Replaced HOP(1m,5m) with TUMBLE 5m \u2014 one row per episode."),
    ("#5", "Map dots never decay back to green", "Per-param TTL (10 min) + periodic 30 s redraw."),
    ("#7", "Producer queue unbounded \u2192 OOM risk", "max_block_ms=5 s + try/except on send/flush."),
    ("#8", "Half-configured exactly-once on sinks", "delivery-guarantee=exactly-once + unique tx-id-prefix per sink."),
    ("UX", "Stats card stuck at '0 readings/min'", "Extended to 6 cards: /min, /s, total, last-age, worst severity, Flink windows/min."),
]
y = Inches(1.85)
add_panel(s, Inches(0.5), y, Inches(12.3), Inches(0.45))
add_text(s, Inches(0.7), y + Inches(0.07), Inches(0.8), Inches(0.3),  "ID",      size=12, bold=True, color=ACCENT)
add_text(s, Inches(1.5), y + Inches(0.07), Inches(5.0), Inches(0.3),  "Symptom", size=12, bold=True, color=ACCENT)
add_text(s, Inches(6.6), y + Inches(0.07), Inches(6.2), Inches(0.3),  "Fix",     size=12, bold=True, color=ACCENT)
for i, (idn, sym, fix) in enumerate(rows):
    ry = y + Inches(0.55 + i * 0.55)
    add_panel(s, Inches(0.5), ry, Inches(12.3), Inches(0.5), fill=BG, line=MUTED)
    add_text(s, Inches(0.7),  ry + Inches(0.08), Inches(0.8), Inches(0.4), idn, size=12, bold=True, color=ACCENT_2)
    add_text(s, Inches(1.5),  ry + Inches(0.08), Inches(5.0), Inches(0.4), sym, size=11, color=TEXT)
    add_text(s, Inches(6.6),  ry + Inches(0.08), Inches(6.2), Inches(0.4), fix, size=11, color=MUTED)
footer(s, 9)


# -------------------- Slide 10: Numbers & performance --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Live numbers",
             "Measured against a single dev laptop \u00b7 Docker Desktop.")

stats = [
    ("~80",      "readings / minute",    "raw-air-quality steady state"),
    ("40",       "active stations",      "after MAX_AGE_HOURS filter"),
    ("12",       "partitions",           "on raw-air-quality \u00b7 4-way parallel"),
    ("8",        "Flink slots",          "2 TM \u00d7 4 slots, parallelism=4"),
    ("30 s",     "checkpoint interval",  "exactly-once Kafka sinks"),
    ("<200 ms",  "Kafka \u2192 browser",  "end-to-end on localhost"),
]
cols = 3
cw, ch = Inches(4.0), Inches(2.2)
gx, gy = Inches(0.25), Inches(0.25)
ox, oy = Inches(0.5), Inches(2.0)
for i, (val, unit, sub) in enumerate(stats):
    r, c = divmod(i, cols)
    x = ox + c * (cw + gx)
    y = oy + r * (ch + gy)
    add_panel(s, x, y, cw, ch)
    add_text(s, x, y + Inches(0.25), cw, Inches(0.9), val,
             size=46, bold=True, color=ACCENT, align=PP_ALIGN.CENTER)
    add_text(s, x, y + Inches(1.2), cw, Inches(0.4), unit,
             size=15, bold=True, color=TEXT, align=PP_ALIGN.CENTER)
    add_text(s, x, y + Inches(1.6), cw, Inches(0.4), sub,
             size=11, color=MUTED, align=PP_ALIGN.CENTER)
footer(s, 10)


# -------------------- Slide 11: Demo flow --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Live demo \u2014 what to look for",
             "5-minute guided tour of the running system.")

steps = [
    ("01", "Open http://localhost:8080", "Dashboard hydrates from /snapshot REST: stations + historical alerts."),
    ("02", "Watch the stats bar", "Readings/min climbs to ~80, last-event age stays under 15 s."),
    ("03", "Map fills in", "Dots colored by Flink window AVG \u2014 first dots appear ~60 s after start."),
    ("04", "Trigger an alert path", "Switch the chart to PM2.5; locations above threshold start flashing in the alerts list."),
    ("05", "Critical escalation", "After \u22653 consecutive breaching minutes a red toast pops up: CRITICAL Station-X."),
    ("06", "Flink UI", "Visit http://localhost:8081 \u2192 see the running job, the operator graph, checkpoints."),
]
y = Inches(1.85)
for i, (num, title, body) in enumerate(steps):
    row_y = y + Inches(i * 0.85)
    add_panel(s, Inches(0.5), row_y, Inches(12.3), Inches(0.75))
    add_chip(s, Inches(0.65), row_y + Inches(0.17), Inches(0.55), Inches(0.4), num)
    add_text(s, Inches(1.4), row_y + Inches(0.07), Inches(3.8), Inches(0.4),
             title, size=14, bold=True, color=TEXT)
    add_text(s, Inches(5.3), row_y + Inches(0.07), Inches(7.4), Inches(0.65),
             body, size=11, color=MUTED)
footer(s, 11)


# -------------------- Slide 12: Tech stack & code layout --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "Code & infrastructure",
             "Everything lives in one Compose stack.")

add_panel(s, Inches(0.5), Inches(1.9), Inches(6.1), Inches(5.0))
add_text(s, Inches(0.75), Inches(2.0), Inches(5.5), Inches(0.4),
         "Repository layout", size=18, bold=True, color=ACCENT)
files = [
    ("producer.py",          "OpenAQ \u2192 Kafka"),
    ("weather_producer.py",  "Open-Meteo \u2192 Kafka"),
    ("flink_processor.py",   "PyFlink job"),
    ("backend/main.py",      "FastAPI + WS"),
    ("frontend/",            "HTML/CSS/JS dashboard"),
    ("docker-compose.yml",   "9 services, 1 network"),
    ("Dockerfile.flink",     "Flink + JDK + PyFlink"),
    ("jars/",                "kafka-sql-connector (bind-mount)"),
]
y = Inches(2.6)
for name, desc in files:
    add_text(s, Inches(0.75), y, Inches(3.0), Inches(0.35), name,
             size=12, bold=True, color=ACCENT_2, font="Consolas")
    add_text(s, Inches(3.8),  y, Inches(2.7), Inches(0.35), desc,
             size=12, color=MUTED)
    y += Inches(0.4)

add_panel(s, Inches(6.75), Inches(1.9), Inches(6.1), Inches(5.0), line=ACCENT_2)
add_text(s, Inches(7.0), Inches(2.0), Inches(5.5), Inches(0.4),
         "Versions", size=18, bold=True, color=ACCENT_2)
versions = [
    ("Apache Kafka",    "7.4.0 (Confluent)"),
    ("Apache Flink",    "1.20.1 (custom image)"),
    ("PyFlink",         "1.20.1"),
    ("kafka-connector", "3.3.0-1.20"),
    ("FastAPI",         "0.115.0"),
    ("aiokafka",        "0.11.0"),
    ("kafka-python",    "2.0.2"),
    ("Python",          "3.11"),
    ("Leaflet",         "1.9"),
    ("Chart.js",        "4.4"),
]
y = Inches(2.6)
for name, ver in versions:
    add_text(s, Inches(7.0), y, Inches(3.0), Inches(0.35), name,
             size=12, color=TEXT)
    add_text(s, Inches(10.0), y, Inches(2.5), Inches(0.35), ver,
             size=12, bold=True, color=ACCENT, font="Consolas")
    y += Inches(0.32)
footer(s, 12)


# -------------------- Slide 13: Future work --------------------
s = prs.slides.add_slide(BLANK)
slide_header(s, "What's next",
             "Natural extensions if this becomes a product.")
add_bullets(s, Inches(0.6), Inches(1.9), Inches(12), Inches(5), [
    "Persistent storage: sink enriched-readings + alerts to ClickHouse / TimescaleDB for historical analytics.",
    "Schema registry (Avro/Protobuf) instead of free-form JSON, with schema evolution checks.",
    "Geo-spatial joins: cluster nearby stations, compute city-level AQI, render heatmaps in the UI.",
    "Notification fan-out: subscribe-by-email/SMS/webhook for critical episodes per region.",
    "Production hardening: Kubernetes, KRaft Kafka, JobManager HA, Prometheus/Grafana observability.",
    "ML layer: short-horizon forecasting per station (ARIMA / temporal-fusion-transformer).",
    "Multi-tenant API key model on the WebSocket bridge.",
], size=17)
footer(s, 13)


# -------------------- Slide 14: Thanks --------------------
s = prs.slides.add_slide(BLANK)
add_bg(s)
add_text(s, Inches(0.7), Inches(2.8), Inches(12), Inches(1.2),
         "Thank you", size=64, bold=True, color=TEXT)
add_text(s, Inches(0.7), Inches(3.9), Inches(12), Inches(0.5),
         "Questions \u00b7 discussion \u00b7 live demo",
         size=22, color=ACCENT)
add_text(s, Inches(0.7), Inches(5.0), Inches(12), Inches(0.4),
         "AirQualityAnalyzer  \u00b7  Kafka \u2192 Flink \u2192 WebSocket \u2192 Browser",
         size=14, color=MUTED)


# -------------------- Save --------------------
out = "AirQualityAnalyzer.pptx"
prs.save(out)
print(f"Wrote {out}")
