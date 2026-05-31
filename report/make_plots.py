"""Generate report figures from a collector run directory.

Usage:
  python make_plots.py --run data/run_YYYYMMDD_HHMMSS
  python make_plots.py            # auto-picks the most recent run
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# WHO 2021 short-term guideline values (µg/m³; CO in mg/m³)
WHO = {"pm25": 15, "pm10": 45, "no2": 25, "o3": 60, "so2": 40, "co": 4}
PARAM_ORDER = ["pm25", "pm10", "no2", "o3", "so2", "co"]
SEV_ORDER = ["MODERATE", "HIGH", "CRITICAL"]
SEV_COLORS = {"MODERATE": "#fbbf24", "HIGH": "#fb923c", "CRITICAL": "#ef4444"}

plt.rcParams.update({
    "figure.dpi": 130,
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def _latest_run() -> Path:
    base = Path(__file__).parent / "data"
    runs = sorted(base.glob("run_*"))
    if not runs:
        raise SystemExit("No run_* directories found under data/. Run collect_stats.py first.")
    return runs[-1]


def _read_jsonl(p: Path) -> pd.DataFrame:
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    rows = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
    return pd.DataFrame(rows)


def plot_throughput(run: Path, out: Path) -> None:
    csv = run / "throughput.csv"
    if not csv.exists():
        return
    df = pd.read_csv(csv)
    if len(df) < 2:
        return
    topics = ["raw-air-quality", "weather-stream", "enriched-readings", "pollution-alerts"]
    mins = df["elapsed_s"] / 60.0
    fig, ax = plt.subplots(figsize=(9, 5))
    for t in topics:
        if t not in df:
            continue
        # per-minute rate from cumulative offsets
        d_off = df[t].diff()
        d_min = df["elapsed_s"].diff() / 60.0
        rate = (d_off / d_min).clip(lower=0)
        ax.plot(mins, rate, label=t, linewidth=1.8)
    ax.set_xlabel("Elapsed time (min)")
    ax.set_ylabel("Messages / minute")
    ax.set_title("Pipeline throughput over time")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out / "throughput_over_time.png")
    plt.close(fig)


def plot_cumulative(run: Path, out: Path) -> None:
    csv = run / "throughput.csv"
    if not csv.exists():
        return
    df = pd.read_csv(csv)
    if len(df) < 2:
        return
    mins = df["elapsed_s"] / 60.0
    fig, ax = plt.subplots(figsize=(9, 5))
    for t in ["raw-air-quality", "enriched-readings", "pollution-alerts", "critical-alerts"]:
        if t in df:
            ax.plot(mins, df[t], label=t, linewidth=1.8)
    ax.set_xlabel("Elapsed time (min)")
    ax.set_ylabel("Cumulative records")
    ax.set_title("Cumulative records produced per topic")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out / "cumulative_records.png")
    plt.close(fig)


def plot_severity(alerts: pd.DataFrame, out: Path) -> None:
    if alerts.empty or "severity" not in alerts:
        return
    counts = alerts["severity"].value_counts().reindex(SEV_ORDER).fillna(0)
    fig, ax = plt.subplots(figsize=(6, 4.5))
    ax.bar(counts.index, counts.values, color=[SEV_COLORS[s] for s in counts.index])
    for i, v in enumerate(counts.values):
        ax.text(i, v, f"{int(v)}", ha="center", va="bottom")
    ax.set_ylabel("Alert count")
    ax.set_title("Alert severity distribution")
    fig.tight_layout()
    fig.savefig(out / "severity_distribution.png")
    plt.close(fig)


def plot_alerts_per_pollutant(alerts: pd.DataFrame, out: Path) -> None:
    if alerts.empty or "parameter" not in alerts:
        return
    order = [p for p in PARAM_ORDER if p in set(alerts["parameter"])]
    if not order:
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    if "severity" in alerts:
        pivot = alerts.pivot_table(index="parameter", columns="severity",
                                   aggfunc="size", fill_value=0).reindex(order)
        bottom = pd.Series(0, index=order, dtype=float)
        for s in SEV_ORDER:
            if s in pivot:
                ax.bar(order, pivot[s], bottom=bottom, label=s, color=SEV_COLORS[s])
                bottom += pivot[s]
        ax.legend(frameon=False)
    else:
        counts = alerts["parameter"].value_counts().reindex(order)
        ax.bar(order, counts.values, color="#fb923c")
    ax.set_ylabel("Alert count")
    ax.set_xlabel("Pollutant")
    ax.set_title("Alerts per pollutant (stacked by severity)")
    fig.tight_layout()
    fig.savefig(out / "alerts_per_pollutant.png")
    plt.close(fig)


def plot_concentration_vs_who(enriched: pd.DataFrame, out: Path) -> None:
    if enriched.empty or "parameter" not in enriched or "value" not in enriched:
        return
    enriched = enriched.copy()
    enriched["value"] = pd.to_numeric(enriched["value"], errors="coerce")
    grp = enriched.groupby("parameter")["value"].mean()
    order = [p for p in PARAM_ORDER if p in grp.index]
    if not order:
        return
    means = [grp[p] for p in order]
    who = [WHO[p] for p in order]
    x = range(len(order))
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.bar([i - 0.2 for i in x], means, width=0.4, label="Mean measured", color="#38bdf8")
    ax.bar([i + 0.2 for i in x], who, width=0.4, label="WHO guideline", color="#94a3b8")
    ax.set_xticks(list(x))
    ax.set_xticklabels(order)
    ax.set_ylabel("Concentration (µg/m³, CO in mg/m³)")
    ax.set_title("Mean measured concentration vs WHO guideline")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out / "concentration_vs_who.png")
    plt.close(fig)


def plot_top_stations(alerts: pd.DataFrame, out: Path) -> None:
    if alerts.empty or "location" not in alerts:
        return
    top = alerts["location"].value_counts().head(15)[::-1]
    if top.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5.5))
    ax.barh([str(s) for s in top.index], top.values, color="#fb7185")
    ax.set_xlabel("Alert count")
    ax.set_title("Top 15 stations by alert volume")
    fig.tight_layout()
    fig.savefig(out / "top_stations.png")
    plt.close(fig)


def plot_alerts_timeline(alerts: pd.DataFrame, out: Path) -> None:
    if alerts.empty or "recv_ts" not in alerts:
        return
    a = alerts.copy()
    a["recv_ts"] = pd.to_datetime(a["recv_ts"], errors="coerce", utc=True)
    a = a.dropna(subset=["recv_ts"])
    if a.empty:
        return
    a["minute"] = a["recv_ts"].dt.floor("1min")
    if "severity" in a:
        pivot = a.pivot_table(index="minute", columns="severity", aggfunc="size", fill_value=0)
        pivot = pivot.reindex(columns=[s for s in SEV_ORDER if s in pivot])
        fig, ax = plt.subplots(figsize=(9, 5))
        bottom = None
        for s in pivot.columns:
            ax.bar(pivot.index, pivot[s], bottom=bottom, width=0.0006,
                   label=s, color=SEV_COLORS[s])
            bottom = pivot[s] if bottom is None else bottom + pivot[s]
        ax.legend(frameon=False)
    else:
        per = a.groupby("minute").size()
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(per.index, per.values, color="#fb923c")
    ax.set_xlabel("Wall-clock time (UTC)")
    ax.set_ylabel("Alerts / minute")
    ax.set_title("Alert rate over time")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out / "alerts_timeline.png")
    plt.close(fig)


def per_pollutant_summary(enriched: pd.DataFrame, out: Path) -> None:
    """Numeric statistics table per pollutant -> summary_per_pollutant.csv."""
    if enriched.empty or "parameter" not in enriched or "value" not in enriched:
        return
    e = enriched.copy()
    e["value"] = pd.to_numeric(e["value"], errors="coerce")
    e = e.dropna(subset=["value"])
    rows = []
    for p in PARAM_ORDER:
        sub = e[e["parameter"] == p]["value"]
        if sub.empty:
            continue
        who = WHO[p]
        idxmax = e[e["parameter"] == p]["value"].idxmax()
        worst_station = e.loc[idxmax, "location"] if idxmax in e.index else ""
        rows.append({
            "pollutant": p,
            "who_guideline": who,
            "windows": int(sub.count()),
            "mean": round(sub.mean(), 3),
            "median": round(sub.median(), 3),
            "std": round(sub.std(), 3),
            "min": round(sub.min(), 3),
            "max": round(sub.max(), 3),
            "p95": round(sub.quantile(0.95), 3),
            "pct_over_who": round(100.0 * (sub >= who).mean(), 1),
            "worst_station": worst_station,
        })
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_csv(out / "summary_per_pollutant.csv", index=False)
    print("\n[summary per pollutant]")
    print(df.to_string(index=False))


def plot_boxplot(enriched: pd.DataFrame, out: Path) -> None:
    """Distribution of concentrations per pollutant (log scale)."""
    if enriched.empty or "parameter" not in enriched or "value" not in enriched:
        return
    e = enriched.copy()
    e["value"] = pd.to_numeric(e["value"], errors="coerce")
    e = e.dropna(subset=["value"])
    e = e[e["value"] > 0]
    order = [p for p in PARAM_ORDER if p in set(e["parameter"])]
    if not order:
        return
    data = [e[e["parameter"] == p]["value"].values for p in order]
    fig, ax = plt.subplots(figsize=(8, 5))
    bp = ax.boxplot(data, labels=order, patch_artist=True, showfliers=False,
                    medianprops=dict(color="#0b0c12", linewidth=1.6))
    palette = ["#38bdf8", "#818cf8", "#a78bfa", "#f472b6", "#fb923c", "#facc15"]
    for patch, c in zip(bp["boxes"], palette):
        patch.set_facecolor(c)
        patch.set_alpha(0.75)
    for i, p in enumerate(order, start=1):
        ax.scatter([i], [WHO[p]], marker="_", s=400, color="#ef4444", zorder=5,
                   label="WHO guideline" if i == 1 else None)
    ax.set_yscale("log")
    ax.set_ylabel("Concentration (log scale)")
    ax.set_xlabel("Pollutant")
    ax.set_title("Concentration distribution per pollutant (WHO marked)")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out / "distribution_boxplot.png")
    plt.close(fig)


def plot_per_pollutant_timeseries(enriched: pd.DataFrame, out: Path) -> None:
    """Network-mean time-series per pollutant in a 2x3 grid."""
    if enriched.empty or "recv_ts" not in enriched or "value" not in enriched:
        return
    e = enriched.copy()
    e["value"] = pd.to_numeric(e["value"], errors="coerce")
    e["recv_ts"] = pd.to_datetime(e["recv_ts"], errors="coerce", utc=True)
    e = e.dropna(subset=["value", "recv_ts"])
    if e.empty:
        return
    e["minute"] = e["recv_ts"].dt.floor("1min")
    order = [p for p in PARAM_ORDER if p in set(e["parameter"])]
    if not order:
        return
    fig, axes = plt.subplots(2, 3, figsize=(13, 7), sharex=True)
    axes = axes.flatten()
    palette = {"pm25": "#38bdf8", "pm10": "#818cf8", "no2": "#a78bfa",
               "o3": "#f472b6", "so2": "#fb923c", "co": "#facc15"}
    for ax, p in zip(axes, order):
        sub = e[e["parameter"] == p].groupby("minute")["value"].mean()
        ax.plot(sub.index, sub.values, color=palette[p], linewidth=1.8)
        ax.axhline(WHO[p], color="#ef4444", linestyle="--", linewidth=1, alpha=0.7)
        ax.set_title(f"{p}  (WHO {WHO[p]})", fontsize=11)
        ax.tick_params(axis="x", labelrotation=30, labelsize=8)
    for ax in axes[len(order):]:
        ax.set_visible(False)
    fig.suptitle("Network-mean concentration over time, per pollutant", fontsize=13)
    fig.tight_layout()
    fig.savefig(out / "per_pollutant_timeseries.png")
    plt.close(fig)


def plot_weather_correlation(enriched: pd.DataFrame, out: Path) -> None:
    """Scatter of PM2.5 vs wind speed and temperature, plus a correlation table."""
    if enriched.empty or "parameter" not in enriched:
        return
    needed = {"value", "temperature", "wind_speed", "humidity"}
    if not needed.issubset(enriched.columns):
        return
    e = enriched.copy()
    for col in ["value", "temperature", "wind_speed", "humidity"]:
        e[col] = pd.to_numeric(e[col], errors="coerce")
    pm = e[e["parameter"] == "pm25"].dropna(subset=["value", "wind_speed", "temperature"])
    if len(pm) < 10:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    axes[0].scatter(pm["wind_speed"], pm["value"], s=10, alpha=0.35, color="#38bdf8")
    axes[0].set_xlabel("Wind speed (km/h)")
    axes[0].set_ylabel("PM2.5 (µg/m³)")
    axes[0].set_title("PM2.5 vs wind speed")
    axes[1].scatter(pm["temperature"], pm["value"], s=10, alpha=0.35, color="#fb923c")
    axes[1].set_xlabel("Temperature (°C)")
    axes[1].set_ylabel("PM2.5 (µg/m³)")
    axes[1].set_title("PM2.5 vs temperature")
    fig.tight_layout()
    fig.savefig(out / "weather_correlation.png")
    plt.close(fig)

    rows = []
    for p in PARAM_ORDER:
        sub = e[e["parameter"] == p].dropna(subset=["value", "wind_speed", "temperature", "humidity"])
        if len(sub) < 10:
            continue
        rows.append({
            "pollutant": p,
            "corr_wind": round(sub["value"].corr(sub["wind_speed"]), 3),
            "corr_temp": round(sub["value"].corr(sub["temperature"]), 3),
            "corr_humidity": round(sub["value"].corr(sub["humidity"]), 3),
            "n": len(sub),
        })
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(out / "weather_correlation.csv", index=False)
        print("\n[weather correlation]")
        print(df.to_string(index=False))


def write_summary(run: Path, alerts, critical, enriched, out: Path) -> None:
    lines = ["# Run summary\n"]
    man = run / "manifest.json"
    if man.exists():
        m = json.loads(man.read_text(encoding="utf-8"))
        lines.append(f"- Duration: {m.get('minutes')} min  (interval {m.get('interval_s')}s)\n")
    csv = run / "throughput.csv"
    if csv.exists():
        df = pd.read_csv(csv)
        if len(df) >= 2:
            for t in ["raw-air-quality", "enriched-readings", "pollution-alerts", "critical-alerts"]:
                if t in df:
                    total = int(df[t].iloc[-1] - df[t].iloc[0])
                    lines.append(f"- {t}: +{total} records during run\n")
    lines.append(f"- pollution-alerts captured: {len(alerts)}\n")
    lines.append(f"- critical-alerts captured: {len(critical)}\n")
    lines.append(f"- enriched records captured: {len(enriched)}\n")
    if not alerts.empty and "severity" in alerts:
        for s in SEV_ORDER:
            c = int((alerts["severity"] == s).sum())
            lines.append(f"  - {s}: {c}\n")
    (out / "SUMMARY.md").write_text("".join(lines), encoding="utf-8")
    print("".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default=None)
    args = ap.parse_args()
    run = Path(args.run) if args.run else _latest_run()
    print(f"[plots] using run: {run}")
    out = run / "figures"
    out.mkdir(exist_ok=True)

    alerts = _read_jsonl(run / "alerts.jsonl")
    critical = _read_jsonl(run / "critical.jsonl")
    enriched = _read_jsonl(run / "enriched.jsonl")

    plot_throughput(run, out)
    plot_cumulative(run, out)
    plot_severity(alerts, out)
    plot_alerts_per_pollutant(alerts, out)
    plot_concentration_vs_who(enriched, out)
    plot_top_stations(alerts, out)
    plot_alerts_timeline(alerts, out)
    per_pollutant_summary(enriched, out)
    plot_boxplot(enriched, out)
    plot_per_pollutant_timeseries(enriched, out)
    plot_weather_correlation(enriched, out)
    write_summary(run, alerts, critical, enriched, out)
    print(f"[plots] figures -> {out}")


if __name__ == "__main__":
    main()
