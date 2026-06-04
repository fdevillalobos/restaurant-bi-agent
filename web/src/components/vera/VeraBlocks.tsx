import React from "react";
import ReactECharts from "echarts-for-react";
import { BarChart3, ChevronDown, ChevronUp, Lightbulb, Table2 } from "lucide-react";
import ReactMarkdown from "react-markdown";
import rehypeSanitize from "rehype-sanitize";
import remarkGfm from "remark-gfm";
import { chartOption } from "../../charting";
import { formatCell, isCountField, isMoneyField, isPercentField, numericValue } from "../../format";
import type { ChatResponse, VeraTable } from "../../types";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";

function titleize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function kpiVariant(field: string) {
  if (isMoneyField(field)) return "success";
  if (isPercentField(field)) return "warning";
  if (isCountField(field)) return "secondary";
  return "outline";
}

function KpiCards({ tables }: { tables: VeraTable[] }) {
  const first = tables.find((table) => table.rows.length === 1);
  if (!first) return null;
  const cells = first.columns
    .map((column) => ({ column, value: first.rows[0]?.[column], numeric: numericValue(first.rows[0]?.[column]) }))
    .filter((cell) => cell.numeric !== null)
    .slice(0, 4);
  if (cells.length === 0) return null;

  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {cells.map((cell) => (
        <div className="rounded-md border border-border bg-card p-3" key={cell.column}>
          <div className="mb-2 flex items-center justify-between gap-2">
            <span className="text-xs font-medium uppercase text-muted-foreground">{titleize(cell.column)}</span>
            <Badge variant={kpiVariant(cell.column)}>{isMoneyField(cell.column) ? "Money" : isCountField(cell.column) ? "Count" : "Metric"}</Badge>
          </div>
          <p className="font-mono text-xl font-semibold text-card-foreground">{formatCell(cell.value, cell.column)}</p>
        </div>
      ))}
    </div>
  );
}

export function DataTable({ table }: { table: VeraTable }) {
  const [sortKey, setSortKey] = React.useState(table.columns[0] || "");
  const [direction, setDirection] = React.useState<"asc" | "desc">("asc");
  const rows = React.useMemo(() => {
    return [...table.rows].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      const aNum = numericValue(av);
      const bNum = numericValue(bv);
      const cmp = aNum !== null && bNum !== null ? aNum - bNum : String(av ?? "").localeCompare(String(bv ?? ""), undefined, { numeric: true });
      return direction === "asc" ? cmp : -cmp;
    });
  }, [table.rows, sortKey, direction]);

  return (
    <section className="rounded-md border border-border bg-card p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 font-semibold text-card-foreground">
          <Table2 className="h-4 w-4" />
          {table.title}
        </div>
        <Badge variant="outline">{table.row_count} rows</Badge>
      </div>
      <div className="overflow-auto rounded-md border border-border">
        <table className="w-full min-w-[680px] border-collapse text-sm">
          <thead>
            <tr className="bg-muted">
              {table.columns.map((column) => (
                <th className="sticky top-0 z-10 border-b border-border px-3 py-2 text-left font-semibold text-muted-foreground" key={column}>
                  <button
                    className="inline-flex items-center gap-1 capitalize"
                    onClick={() => {
                      setDirection(sortKey === column && direction === "asc" ? "desc" : "asc");
                      setSortKey(column);
                    }}
                  >
                    {titleize(column)}
                    {sortKey === column ? direction === "asc" ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" /> : null}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr className="border-b border-border last:border-b-0" key={rowIndex}>
                {table.columns.map((column) => (
                  <td className={numericValue(row[column]) !== null ? "px-3 py-2 text-right font-mono tabular-nums" : "px-3 py-2"} key={column}>
                    {formatCell(row[column], column)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function VeraResponseBlocks({ response, onAsk }: { response: ChatResponse; onAsk: (text: string) => void }) {
  return (
    <div className="grid gap-4">
      <div className="markdown rounded-md border border-border bg-card p-4 leading-7 text-card-foreground">
        <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSanitize]}>
          {response.message}
        </ReactMarkdown>
      </div>
      <KpiCards tables={response.tables} />
      {response.charts.map((chart, index) => (
        <section className="rounded-md border border-border bg-card p-4" key={`${chart.title}-${index}`}>
          <div className="mb-3 flex items-center gap-2 font-semibold text-card-foreground">
            <BarChart3 className="h-4 w-4" />
            {chart.title}
          </div>
          <ReactECharts option={chartOption(chart)} style={{ height: 320 }} notMerge />
          {chart.caption && <p className="mt-2 text-sm text-muted-foreground">{chart.caption}</p>}
        </section>
      ))}
      {response.tables.map((table, index) => <DataTable table={table} key={`${table.title}-${index}`} />)}
      {response.recommendations.length > 0 && (
        <section className="rounded-md border border-border bg-card p-4">
          <div className="mb-2 flex items-center gap-2 font-semibold">
            <Lightbulb className="h-4 w-4" />
            Recommended next steps
          </div>
          <div className="grid gap-2 text-sm text-muted-foreground">
            {response.recommendations.map((item) => <p key={item}>{item}</p>)}
          </div>
        </section>
      )}
      {response.suggested_next_questions.length > 0 && (
        <section className="flex flex-wrap gap-2">
          {response.suggested_next_questions.map((question) => (
            <Button key={question} variant="outline" size="sm" onClick={() => onAsk(question)}>
              {question}
            </Button>
          ))}
        </section>
      )}
    </div>
  );
}
