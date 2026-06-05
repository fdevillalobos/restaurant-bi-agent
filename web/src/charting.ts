import type { VeraChart } from "./types";
import { formatCell, formatChartAxis } from "./format";
import type { Language } from "./i18n";

export function chartOption(chart: VeraChart, language: Language = "en") {
  const xField = chart.x || chart.label || Object.keys(chart.data[0] || {})[0];
  const yField = chart.y || Object.keys(chart.data[0] || {}).find((key) => typeof chart.data[0]?.[key] === "number");
  const sortedData = [...chart.data].sort((a, b) => {
    const av = String(a[xField] ?? "");
    const bv = String(b[xField] ?? "");
    const ad = Date.parse(av);
    const bd = Date.parse(bv);
    if (Number.isFinite(ad) && Number.isFinite(bd)) return ad - bd;
    return av.localeCompare(bv, undefined, { numeric: true });
  });
  const xData = sortedData.map((row) => String(row[xField] ?? ""));
  const yData = sortedData.map((row) => Number(row[yField || ""] ?? 0));
  const type = chart.type === "line" || chart.type === "area" ? "line" : "bar";
  return {
    tooltip: {
      trigger: "axis",
      valueFormatter: (value: number | string) => formatCell(value, yField || "", language)
    },
    grid: { left: 78, right: 24, top: 28, bottom: 48 },
    xAxis: {
      type: "category",
      data: xData,
      axisLabel: { rotate: xData.length > 8 ? 35 : 0, color: "#64748b" },
      axisLine: { lineStyle: { color: "#cbd5e1" } }
    },
    yAxis: {
      type: "value",
      axisLabel: {
        color: "#64748b",
        formatter: (value: number) => formatChartAxis(value, yField || "", language)
      },
      splitLine: { lineStyle: { color: "#e2e8f0" } }
    },
    series: [
      {
        name: yField,
        type,
        smooth: type === "line",
        areaStyle: chart.type === "area" ? { color: "rgba(37, 99, 235, 0.12)" } : undefined,
        data: yData,
        color: type === "line" ? "#2563eb" : "#0f766e",
        itemStyle: { borderRadius: type === "bar" ? [4, 4, 0, 0] : 0 }
      }
    ]
  };
}
