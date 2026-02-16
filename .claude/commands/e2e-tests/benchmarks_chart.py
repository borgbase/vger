# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "numpy>=2.0,<3",
#   "matplotlib>=3.9,<4",
# ]
# ///

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from matplotlib.patches import Patch

# Data — 41 GiB dataset
tools = ["V'Ger", "Restic", "Rustic", "Borg"]
data = {
    "Throughput (MiB/s)": {
        "backup": [1850.3, 1291.1, 1902.4, 904.4],
        "restore": [468.3, 352.9, 460.1, 218.6],
        "higher_better": True,
    },
    "Duration (s)": {
        "backup": [22.65, 32.46, 22.03, 46.34],
        "restore": [89.50, 118.76, 91.09, 191.69],
        "higher_better": False,
    },
    "CPU Usage (%)": {
        "backup": [64, 68, 127, 78],
        "restore": [188, 196, 233, 96],
        "higher_better": False,
    },
    "Peak Memory (MB)": {
        "backup": [411232/1024, 151608/1024, 141328/1024, 160844/1024],
        "restore": [806840/1024, 366780/1024, 7735816/1024, 126532/1024],
        "higher_better": False,
    },
}

# Style
VGER = "#fb8c00"
VGER_LIGHT = "#ffb74d"
OTHER = "#546e7a"
OTHER_LIGHT = "#78909c"
BG = "#1a2327"

plt.rcParams.update({
    "font.family": "monospace",
    "figure.facecolor": BG,
    "axes.facecolor": BG,
    "text.color": "#eceff1",
    "axes.labelcolor": "#b0bec5",
    "xtick.color": "#90a4ae",
    "ytick.color": "#90a4ae",
})

x = np.arange(len(tools))
w = 0.32

def get_colors():
    backup_colors = [VGER if t == "V'Ger" else OTHER for t in tools]
    restore_colors = [VGER_LIGHT if t == "V'Ger" else OTHER_LIGHT for t in tools]
    return backup_colors, restore_colors

def draw_standard_panel(ax, title, d):
    bc, rc = get_colors()
    bars1 = ax.bar(x - w/2, d["backup"], w, color=bc, zorder=3)
    bars2 = ax.bar(x + w/2, d["restore"], w, color=rc, zorder=3)

    ymax = max(max(d["backup"]), max(d["restore"]))
    ax.set_ylim(0, ymax * 1.18)

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            fmt = f"{h:.0f}" if h >= 10 else f"{h:.1f}"
            ax.text(bar.get_x() + bar.get_width()/2, h + ymax * 0.01,
                    fmt, ha="center", va="bottom", fontsize=7.5,
                    color="#b0bec5", fontweight="bold")

    style_axis(ax, title, d)

def draw_broken_panel(ax_top, ax_bot, title, d):
    bc, rc = get_colors()

    for ax in [ax_top, ax_bot]:
        ax.bar(x - w/2, d["backup"], w, color=bc, zorder=3)
        ax.bar(x + w/2, d["restore"], w, color=rc, zorder=3)

    ax_bot.set_ylim(0, 1100)
    ax_top.set_ylim(7200, 8200)

    ax_top.spines["bottom"].set_visible(False)
    ax_bot.spines["top"].set_visible(False)
    ax_top.tick_params(bottom=False, labelbottom=False)

    # Zigzag break lines
    zigzag_n = 12
    zx = np.linspace(-0.6, len(tools) - 0.4, zigzag_n * 2 + 1)

    # Bottom axis top edge
    zy_amp = (ax_bot.get_ylim()[1] - ax_bot.get_ylim()[0]) * 0.018
    zy_bot = [ax_bot.get_ylim()[1] + (zy_amp if i % 2 == 0 else -zy_amp) for i in range(len(zx))]
    ax_bot.plot(zx, zy_bot, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

    # Top axis bottom edge
    zy_amp_t = (ax_top.get_ylim()[1] - ax_top.get_ylim()[0]) * 0.018
    zy_top = [ax_top.get_ylim()[0] + (zy_amp_t if i % 2 == 0 else -zy_amp_t) for i in range(len(zx))]
    ax_top.plot(zx, zy_top, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

    # Value labels
    bars_bot_1 = ax_bot.containers[0]
    bars_bot_2 = ax_bot.containers[1]
    bars_top_1 = ax_top.containers[0]
    bars_top_2 = ax_top.containers[1]

    for i in range(len(tools)):
        for bars_b, bars_t, vals in [(bars_bot_1, bars_top_1, d["backup"]),
                                      (bars_bot_2, bars_top_2, d["restore"])]:
            v = vals[i]
            if v > 1100:
                bar = bars_t[i]
                ax_top.text(bar.get_x() + bar.get_width()/2, v + 40,
                            f"{v:.0f}", ha="center", va="bottom", fontsize=7.5,
                            color="#ff6666", fontweight="bold")
            else:
                bar = bars_b[i]
                ax_bot.text(bar.get_x() + bar.get_width()/2, v + 15,
                            f"{v:.0f}", ha="center", va="bottom", fontsize=7.5,
                            color="#b0bec5", fontweight="bold")

    ax_bot.set_xticks(x)
    ax_bot.set_xticklabels(tools, fontsize=10)
    for label in ax_bot.get_xticklabels():
        if "V'Ger" in label.get_text():
            label.set_color(VGER)
            label.set_fontweight("bold")
    ax_top.set_xticks([])

    ax_top.set_title(f"{title}  (\u2193 lower is better)", fontsize=10, color="#cfd8dc", pad=10)

    for ax in [ax_top, ax_bot]:
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#2a2a40")
        ax.spines["bottom"].set_color("#2a2a40")
    ax_top.spines["bottom"].set_visible(False)
    ax_top.spines["right"].set_visible(False)
    ax_bot.spines["top"].set_visible(False)

def style_axis(ax, title, d):
    ax.set_xticks(x)
    ax.set_xticklabels(tools, fontsize=10)
    for label in ax.get_xticklabels():
        if "V'Ger" in label.get_text():
            label.set_color(VGER)
            label.set_fontweight("bold")

    arrow = "\u2191" if d["higher_better"] else "\u2193"
    qualifier = "higher is better" if d["higher_better"] else "lower is better"
    ax.set_title(f"{title}  ({arrow} {qualifier})", fontsize=10, color="#cfd8dc", pad=10)

    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#2a2a40")
    ax.spines["bottom"].set_color("#2a2a40")


# Layout
fig = plt.figure(figsize=(11, 8.5))
fig.suptitle("Backup Tool Benchmark  \u00b7  ~41 GiB dataset", fontsize=16,
             fontweight="bold", color="#e8e8f0", y=0.97)

outer = gridspec.GridSpec(2, 2, figure=fig, hspace=0.38, wspace=0.28,
                          left=0.06, right=0.97, top=0.90, bottom=0.09)

# Top-left: Throughput
draw_standard_panel(fig.add_subplot(outer[0, 0]), "Throughput (MiB/s)", data["Throughput (MiB/s)"])

# Top-right: Duration
draw_standard_panel(fig.add_subplot(outer[0, 1]), "Duration (s)", data["Duration (s)"])

# Bottom-left: CPU
draw_standard_panel(fig.add_subplot(outer[1, 0]), "CPU Usage (%)", data["CPU Usage (%)"])

# Bottom-right: Memory with broken axis
inner = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=outer[1, 1],
                                         height_ratios=[1, 3], hspace=0.08)
draw_broken_panel(fig.add_subplot(inner[0]), fig.add_subplot(inner[1]),
                  "Peak Memory (MB)", data["Peak Memory (MB)"])

# Legend
legend_elements = [
    Patch(facecolor=VGER, label="V'Ger backup"),
    Patch(facecolor=VGER_LIGHT, label="V'Ger restore"),
    Patch(facecolor=OTHER, label="Others backup"),
    Patch(facecolor=OTHER_LIGHT, label="Others restore"),
]
fig.legend(handles=legend_elements, loc="lower center", ncol=4, frameon=True,
           fontsize=9, fancybox=False, edgecolor="#4a5d66",
           facecolor="#2c3940", labelcolor="#c8c8d4",
           bbox_to_anchor=(0.5, 0.005))

plt.savefig("benchmark_multi.png", dpi=200, bbox_inches="tight")
print("Done")
