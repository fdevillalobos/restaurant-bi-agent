export function isMoneyField(field: string) {
  if (/(count|num_|number|tickets|orders|sales_count|transactions|units|quantity|qty|covers)/i.test(field)) return false;
  return /(sales|sale|revenue|gross|total|amount|ticket|price|cost|expense|payment|discount|spend|value)/i.test(field);
}

export function isPercentField(field: string) {
  return /(pct|percent|percentage|change|variation|varied|ratio|rate)/i.test(field);
}

export function isCountField(field: string) {
  return /(count|num_|number|tickets|orders|sales_count|transactions|units|quantity|qty|covers)/i.test(field);
}

export function numericValue(value: unknown) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function numberOptions(field: string): Intl.NumberFormatOptions {
  if (isCountField(field)) {
    return { maximumFractionDigits: 0 };
  }
  return {
    minimumFractionDigits: isMoneyField(field) ? 2 : 0,
    maximumFractionDigits: 2
  };
}

export function formatNumber(value: number, field: string) {
  if (isPercentField(field)) {
    return `${value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}%`;
  }
  if (isMoneyField(field)) {
    return `$${value.toLocaleString(undefined, numberOptions(field))}`;
  }
  return value.toLocaleString(undefined, numberOptions(field));
}

export function formatCompactNumber(value: number, field: string) {
  const compact = value.toLocaleString(undefined, {
    notation: "compact",
    compactDisplay: "short",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  });
  if (isPercentField(field)) return `${compact}%`;
  if (isMoneyField(field)) return `$${compact}`;
  return compact;
}

export function formatCell(value: unknown, field: string) {
  if (value === null || value === undefined) return "";
  const parsed = numericValue(value);
  if (parsed !== null) return formatNumber(parsed, field);
  return String(value);
}

export function formatChartAxis(value: unknown, field: string) {
  const parsed = numericValue(value);
  if (parsed === null) return String(value ?? "");
  return formatCompactNumber(parsed, field);
}
