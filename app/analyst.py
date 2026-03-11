from __future__ import annotations

import json
import os
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional
import datetime


# --------------------------------------------------------------------------- #
#  Language detection
# --------------------------------------------------------------------------- #

_SPANISH_CHARS = re.compile(r"[áéíóúüñ¿¡ÁÉÍÓÚÜÑ]", re.IGNORECASE)

_SPANISH_WORDS = re.compile(
    r"\b(ventas?|cuánto|cuánta|cuanto|cuanta|cuáles?|cuales?|cuál|cual|"
    r"semanas?|meses?|años?|días?|dia|promedio|ticket|productos?|categoría|"
    r"categoria|pagos?|descuentos?|mejor|peor|mayor|menor|último|ultimo|"
    r"últimos|ultimos|por|del|las|los|una|uno|qué|que|cómo|como|"
    r"lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo|"
    r"enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|octubre|noviembre|diciembre|"
    r"hoy|ayer|semana|mes|año|pasadas?|cerradas?|completas?)\b",
    re.IGNORECASE,
)


def detect_language(text: str) -> str:
    """Return 'es' if the text appears to be Spanish, otherwise 'en'."""
    if _SPANISH_CHARS.search(text):
        return "es"
    if len(_SPANISH_WORDS.findall(text)) >= 2:
        return "es"
    return "en"


# --------------------------------------------------------------------------- #
#  Box-drawing table formatter
# --------------------------------------------------------------------------- #

def _fmt_val(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float, Decimal)):
        f = float(v)
        return f"{int(f):,}" if isinstance(v, int) else f"{f:,.2f}"
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)[:10]
    return str(v)[:40]


def _is_numeric(v: Any) -> bool:
    return isinstance(v, (int, float, Decimal)) and not isinstance(v, bool)


def format_table(rows: List[Dict[str, Any]], max_rows: int = 30) -> str:
    """
    Render rows as a Unicode box-drawing table.
    Returns an empty string if there are fewer than 2 rows (scalar results
    don't need a table).
    """
    if not rows or len(rows) < 2:
        return ""

    cols = list(rows[0].keys())
    headers = [c.replace("_", " ").title() for c in cols]

    display = rows[:max_rows]
    formatted = [[_fmt_val(row.get(c)) for c in cols] for row in display]

    # Right-align numeric columns
    right = [_is_numeric(display[0].get(c)) for c in cols]

    # Column widths = max of header and any cell
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in formatted))
        for i in range(len(cols))
    ]

    def _row(cells: List[str]) -> str:
        parts = []
        for i, (cell, w) in enumerate(zip(cells, widths)):
            padded = cell.rjust(w) if right[i] else cell.ljust(w)
            parts.append(f" {padded} ")
        return "│" + "│".join(parts) + "│"

    top    = "┌" + "┬".join("─" * (w + 2) for w in widths) + "┐"
    mid    = "├" + "┼".join("─" * (w + 2) for w in widths) + "┤"
    bottom = "└" + "┴".join("─" * (w + 2) for w in widths) + "┘"

    lines = [top, _row(headers), mid, *[_row(r) for r in formatted], bottom]

    if len(rows) > max_rows:
        lines.append(f"  … and {len(rows) - max_rows} more rows")

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
#  LLM analyst prompts
# --------------------------------------------------------------------------- #

_SYSTEM_EN = """\
You are a senior BI analyst for a restaurant group. Your role is to answer \
data questions clearly and concisely, as if briefing the restaurant owner or manager.

Guidelines:
- Answer the question directly and specifically — give the actual numbers.
- Use plain text only (no markdown, no bullet points, no bold).
- Format numbers with thousand separators (e.g. 1,234,567).
  For currency, just use the number — do not add currency symbols unless asked.
- If you see a notable trend, top item, or anomaly, call it out briefly.
- Be concise. Avoid filler like "Based on the data provided..." or \
  "I hope this helps". Just answer.
- Never mention SQL, databases, tables, or columns.
- If only 1 row is returned, it is likely a top-N query with LIMIT 1 or an aggregate. \
  Do NOT say "only one data point exists" — just report what the number means.
"""

_SYSTEM_EN_INSIGHT = """\
You are a senior BI analyst for a restaurant group. The data has already been \
shown to the user in a formatted table — do NOT repeat or list the numbers again.

Your job: write 1-3 sentences of plain-text insight — highlight the standout \
item, a trend, or an anomaly worth noting. No markdown, no bullet points, no bold. \
No filler phrases. Never mention SQL, databases, tables, or columns.
"""

_SYSTEM_ES = """\
Eres un analista BI senior para un grupo de restaurantes. Tu rol es responder \
preguntas de datos de forma clara y concisa, como si le informaras al dueño \
o gerente del restaurante.

Pautas:
- Responde la pregunta de forma directa y específica — da los números reales.
- Usa solo texto plano (sin markdown, sin viñetas, sin negrita).
- Formatea los números con separadores de miles (ej: 1.234.567).
  Para moneda, solo usa el número — no agregues símbolo a menos que te lo pidan.
- Si hay una tendencia notable, un ítem destacado o una anomalía, menciónalo brevemente.
- Sé conciso. Sin frases de relleno como "Según los datos proporcionados...".
- Nunca menciones SQL, bases de datos, tablas ni columnas.
- Si solo hay 1 fila, probablemente es el resultado de una consulta con LIMIT 1 o un agregado. \
  NO digas "solo hay un dato" — simplemente informa qué significa el número.
"""

_SYSTEM_ES_INSIGHT = """\
Eres un analista BI senior para un grupo de restaurantes. Los datos ya fueron \
mostrados al usuario en una tabla formateada — NO repitas ni listes los números.

Tu tarea: escribe 1-3 oraciones de texto plano con el insight principal — \
destaca el ítem más relevante, una tendencia o una anomalía notable. Sin markdown, \
sin viñetas, sin negrita. Sin frases de relleno. Nunca menciones SQL ni bases de datos.
"""


# --------------------------------------------------------------------------- #
#  Public entry point
# --------------------------------------------------------------------------- #

def generate_answer(
    question: str,
    rows: List[Dict[str, Any]],
    language: str = "en",
    history: Optional[List[dict]] = None,
) -> str:
    """
    Produce a natural-language answer from DB rows.

    For multi-row results the table is assumed to be shown separately by the
    caller (CLI prints it, Telegram sends it as <pre>). This function returns
    only the insight text in that case.

    For single-row (scalar) results the full answer is returned.
    """
    if not rows:
        if language == "es":
            return (
                "No encontré datos para esta consulta. "
                "El período puede no tener ventas, o los filtros no coinciden con registros existentes."
            )
        return (
            "No data found for this query. "
            "The period may have no sales, or the filters don't match any records."
        )

    from openai import OpenAI
    client = OpenAI()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    multi_row = len(rows) > 1

    if multi_row:
        system = _SYSTEM_ES_INSIGHT if language == "es" else _SYSTEM_EN_INSIGHT
    else:
        system = _SYSTEM_ES if language == "es" else _SYSTEM_EN

    rows_to_send = rows[:200]
    extra = len(rows) - len(rows_to_send)
    rows_json = json.dumps(rows_to_send, default=str)
    row_note = f" (showing first {len(rows_to_send)} of {len(rows)} rows)" if extra > 0 else ""

    user_content = f"Question: {question}\n\nData{row_note}:\n{rows_json}"

    messages: list[dict] = [{"role": "system", "content": system}]
    if history:
        messages.extend(history[-6:])
    messages.append({"role": "user", "content": user_content})

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
    )

    return resp.choices[0].message.content or ""
