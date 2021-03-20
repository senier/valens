import io
from datetime import date, timedelta

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.style
import numpy as np
import pandas as pd
from matplotlib.backends.backend_svg import FigureCanvasSVG
from matplotlib.figure import Figure

from valens import storage

matplotlib.style.use("seaborn-whitegrid")

matplotlib.rc("font", family="Roboto", size=12)
matplotlib.rc("legend", handletextpad=0.5, columnspacing=0.5, handlelength=1)

STYLE = ".-"
COLOR = {
    "avg. weight": "#FAA43A",
    "reps": "#5DA5DA",
    "reps+rir": "#FAA43A",
    "rpe": "#F17CB0",
    "time": "#B276B2",
    "tut": "#F15854",
    "volume": "#4D4D4D",
    "weight": "#60BD68",
}


def plot_svg(fig: Figure) -> bytes:
    output = io.BytesIO()
    FigureCanvasSVG(fig).print_svg(output)
    return output.getvalue()


def workouts(user_id: int, first: date = None, last: date = None) -> Figure:
    df = storage.read_sets(user_id)
    df["reps+rir"] = df["reps"] + df["rir"]
    df["tut"] = df["reps"].replace(np.nan, 1) * df["time"]
    return _workouts_exercise(df, first, last)


def exercise(user_id: int, name: str, first: date = None, last: date = None) -> Figure:
    df = storage.read_sets(user_id)
    df["reps+rir"] = df["reps"] + df["rir"]
    df["tut"] = df["reps"].replace(np.nan, 1) * df["time"]
    df_ex = df.loc[lambda x: x["exercise"] == name]
    return _workouts_exercise(df_ex, first, last)


def _workouts_exercise(df: pd.DataFrame, first: date = None, last: date = None) -> Figure:
    fig, axs = plt.subplots(4)

    interval_first = first - timedelta(days=30) if first else None

    df_mean = df.loc[:, ["date", "reps", "reps+rir", "weight", "time"]].groupby(["date"]).mean()
    df_mean_interval = df_mean[interval_first:last]  # type: ignore  # ISSUE: python/typing#159

    for i, cols in enumerate([["reps", "reps+rir"], ["weight"], ["time"]]):
        d = df_mean_interval.loc[:, cols]
        ymax = max(
            10,
            int(max(list(d.max()))) + 1 if not d.empty and not all(pd.isna(list(d.max()))) else 0,
        )
        d.plot(
            ax=axs[i],
            style=STYLE,
            color=COLOR,
            xlim=(first, last),
            ylim=(0, ymax),
            legend=False,
        )

    df_sum = df.loc[:, ["date", "reps", "tut"]].groupby(["date"]).sum()
    df_sum_interval = df_sum[interval_first:last]  # type: ignore  # ISSUE: python/typing#159
    df_sum_interval.columns = ["volume", "tut"]
    df_sum_interval.plot(
        ax=axs[3],
        style=STYLE,
        color=COLOR,
        xlim=(first, last),
        ylim=(0, None),
        legend=False,
    ).set(xlabel=None)

    _common_layout(fig)
    fig.set_size_inches(5, 8)
    fig.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    return fig


def bodyweight(user_id: int, first: date = None, last: date = None) -> Figure:
    df = storage.read_bodyweight(user_id).set_index("date")

    interval_first = first - timedelta(days=30) if first else None

    df_interval = df.loc[interval_first:last]  # type: ignore  # ISSUE: python/typing#159
    ymin = int(df_interval.min()) if not df_interval.empty else None
    ymax = int(df_interval.max()) + 1 if not df_interval.empty else None

    plot = df_interval.plot(
        style=STYLE,
        color=COLOR,
        xlim=(first, last),
        ylim=(ymin, ymax),
        legend=False,
    )
    df.rolling(window=9, center=True).mean()["weight"].plot(
        style="-", color=COLOR, label="avg. weight"
    ).set(xlabel=None)

    fig = plot.get_figure()
    _common_layout(fig)
    fig.set_size_inches(5, 4)
    return fig


def _common_layout(fig: Figure) -> None:
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.97), ncol=6)
    fig.autofmt_xdate()
