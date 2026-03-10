from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


_SYSTEM_EN = """\
You are a senior BI analyst for a restaurant group. Your role is to answer \
data questions clearly and concisely, as if briefing the restaurant owner or manager.

Guidelines:
- Answer the question directly and specifically — give the actual numbers.
- Use plain text formatting suitable for a chat interface (Telegram):
  use *bold* sparingly, bullet points with hyphens, and short lines.
- Format numbers with thousand separators (e.g. 1,234,567).
  For currency, just use the number — do not add currency symbols unless asked.
- If you see a notable trend, top item, or anomaly, call it out briefly.
- Be concise. Avoid filler like "Based on the data provided..." or \
  "I hope this helps". Just answer.
- Never mention SQL, databases, tables, or columns.
- If data has multiple rows (ranking, trend), list up to 10. \
  Summarize the rest if there are more.
- If only 1 row is returned, it is likely a top-N query with LIMIT 1 or an aggregate. \
  Do NOT say "only one data point exists" — just report what the number means.
"""

_SYSTEM_ES = """\
Eres un analista BI senior para un grupo de restaurantes. Tu rol es responder \
preguntas de datos de forma clara y concisa, como si le informaras al dueño \
o gerente del restaurante.

Pautas:
- Responde la pregunta de forma directa y específica — da los números reales.
- Usa formato de texto plano apto para Telegram: \
  viñetas con guiones, líneas cortas.
- Formatea los números con separadores de miles (ej: 1.234.567).
  Para moneda, solo usa el número — no agregues símbolo a menos que te lo pidan.
- Si hay una tendencia notable, un ítem destacado o una anomalía, menciónalo brevemente.
- Sé conciso. Sin frases de relleno como "Según los datos proporcionados...".
- Nunca menciones SQL, bases de datos, tablas ni columnas.
- Si hay múltiples filas (ranking, tendencia), lista hasta 10. \
  Resume el resto si hay más.
- Si solo hay 1 fila, probablemente es el resultado de una consulta con LIMIT 1 o un agregado. \
  NO digas "solo hay un dato" — simplemente informa qué significa el número.
"""


def generate_answer(
    question: str,
    rows: List[Dict[str, Any]],
    language: str = "en",
    history: Optional[List[dict]] = None,
) -> str:
    """
    Use the LLM to produce a natural-language answer from raw DB rows.

    Args:
        question: The original user question.
        rows:     Raw query results (list of dicts).
        language: "en" or "es".
        history:  Optional prior conversation for follow-up context.

    Returns:
        A well-formatted natural-language answer.
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

    system = _SYSTEM_ES if language == "es" else _SYSTEM_EN

    # Cap rows to avoid token bloat; analyst works well with ≤200 rows
    rows_to_send = rows[:200]
    extra = len(rows) - len(rows_to_send)
    rows_json = json.dumps(rows_to_send, default=str)

    row_note = ""
    if extra > 0:
        row_note = f" (showing first {len(rows_to_send)} of {len(rows)} rows)"

    user_content = f"Question: {question}\n\nData{row_note}:\n{rows_json}"

    messages: list[dict] = [{"role": "system", "content": system}]
    if history:
        # Include last 6 messages (3 exchanges) so the analyst can reference previous answers
        messages.extend(history[-6:])
    messages.append({"role": "user", "content": user_content})

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
    )

    return resp.choices[0].message.content or ""
