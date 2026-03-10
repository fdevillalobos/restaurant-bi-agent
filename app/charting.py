from __future__ import annotations

import io
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple


# --------------------------------------------------------------------------- #
#  Column-type detection helpers
# --------------------------------------------------------------------------- #

def _is_numeric(val: Any) -> bool:
    return isinstance(val, (int, float, Decimal)) and not isinstance(val, bool)


def _is_date_like(val: Any) -> bool:
    return isinstance(val, (date, datetime))


_DATE_KEYWORDS = ("date", "day", "hour", "week", "month", "time", "period")


def _find_date_col(cols: List[str], rows: List[Dict]) -> Optional[str]:
    for col in cols:
        if any(kw in col.lower() for kw in _DATE_KEYWORDS):
            return col
    # Fallback: check value type
    for col in cols:
        if rows and _is_date_like(rows[0].get(col)):
            return col
    return None


def _find_numeric_cols(cols: List[str], rows: List[Dict], exclude: List[str]) -> List[str]:
    result = []
    for col in cols:
        if col in exclude:
            continue
        if rows and _is_numeric(rows[0].get(col)):
            result.append(col)
    return result


def _find_label_col(cols: List[str], rows: List[Dict], exclude: List[str]) -> Optional[str]:
    for col in cols:
        if col in exclude:
            continue
        val = rows[0].get(col) if rows else None
        if isinstance(val, str):
            return col
    return None


# --------------------------------------------------------------------------- #
#  Number formatting
# --------------------------------------------------------------------------- #

def _fmt_number(n: Any) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return str(n)
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:,.1f}"


# --------------------------------------------------------------------------- #
#  Chart builders
# --------------------------------------------------------------------------- #

_PALETTE = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63", "#9C27B0",
            "#00BCD4", "#8BC34A", "#FF5722", "#607D8B", "#FFC107"]


def _col_label(col: str) -> str:
    return col.replace("_", " ").title()


def _line_chart(rows: List[Dict], x_col: str, y_col: str) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    x_labels = [str(row[x_col])[:10] for row in rows]
    ys = [float(row[y_col] or 0) for row in rows]
    xs = range(len(rows))

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(xs, ys, color=_PALETTE[0], linewidth=2, marker="o", markersize=4)
    ax.fill_between(xs, ys, alpha=0.12, color=_PALETTE[0])

    ax.set_xticks(xs)
    ax.set_xticklabels(x_labels, rotation=35, ha="right", fontsize=8)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _fmt_number(v)))
    ax.set_xlabel(_col_label(x_col), fontsize=9)
    ax.set_ylabel(_col_label(y_col), fontsize=9)
    ax.set_title(f"{_col_label(y_col)} over time", fontsize=11, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _bar_chart(rows: List[Dict], label_col: str, value_col: str) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    # Limit to top 15 for readability
    rows = rows[:15]
    labels = [str(row[label_col])[:30] for row in rows]
    values = [float(row[value_col] or 0) for row in rows]

    horizontal = len(labels) > 5
    fig, ax = plt.subplots(figsize=(9, max(4, len(labels) * 0.5 + 1)) if horizontal else (9, 4))

    colors = [_PALETTE[i % len(_PALETTE)] for i in range(len(labels))]

    if horizontal:
        bars = ax.barh(labels[::-1], values[::-1], color=colors[::-1])
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _fmt_number(v)))
        # Value labels
        for bar, val in zip(bars, values[::-1]):
            ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                    _fmt_number(val), va="center", fontsize=8)
        ax.set_xlabel(_col_label(value_col), fontsize=9)
        ax.spines[["top", "right"]].set_visible(False)
    else:
        bars = ax.bar(range(len(labels)), values, color=colors)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _fmt_number(v)))
        ax.set_ylabel(_col_label(value_col), fontsize=9)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=9)
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
                    _fmt_number(val), ha="center", fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)

    ax.set_title(f"{_col_label(value_col)} by {_col_label(label_col)}",
                 fontsize=11, fontweight="bold")
    ax.grid(axis="x" if horizontal else "y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# --------------------------------------------------------------------------- #
#  Public entry point
# --------------------------------------------------------------------------- #

def make_chart(rows: List[Dict[str, Any]], question: str = "") -> Optional[bytes]:
    """
    Decide whether the data warrants a chart and generate one.
    Returns PNG bytes, or None if no chart is appropriate.
    """
    if not rows or len(rows) < 2:
        return None  # Scalar or empty — no chart needed

    cols = list(rows[0].keys())

    date_col = _find_date_col(cols, rows)
    numeric_cols = _find_numeric_cols(cols, rows, exclude=[date_col] if date_col else [])
    label_col = _find_label_col(cols, rows, exclude=([date_col] if date_col else []) + numeric_cols)

    if not numeric_cols:
        return None

    try:
        if date_col:
            return _line_chart(rows, date_col, numeric_cols[0])
        if label_col:
            return _bar_chart(rows, label_col, numeric_cols[0])
    except Exception:
        return None

    return None
