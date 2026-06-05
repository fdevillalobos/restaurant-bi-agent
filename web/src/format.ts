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

function localeFor(language: Language = "en") {
  return language === "es" ? "es-AR" : "en-US";
}

export function formatNumber(value: number, field: string, language: Language = "en") {
  const locale = localeFor(language);
  if (isPercentField(field)) {
    return `${value.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}%`;
  }
  if (isMoneyField(field)) {
    return `$${value.toLocaleString(locale, numberOptions(field))}`;
  }
  return value.toLocaleString(locale, numberOptions(field));
}

export function formatCompactNumber(value: number, field: string, language: Language = "en") {
  const compact = value.toLocaleString(localeFor(language), {
    notation: "compact",
    compactDisplay: "short",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  });
  if (isPercentField(field)) return `${compact}%`;
  if (isMoneyField(field)) return `$${compact}`;
  return compact;
}

export function formatCell(value: unknown, field: string, language: Language = "en") {
  if (value === null || value === undefined) return "";
  const parsed = numericValue(value);
  if (parsed !== null) return formatNumber(parsed, field, language);
  return String(value);
}

export function formatChartAxis(value: unknown, field: string, language: Language = "en") {
  const parsed = numericValue(value);
  if (parsed === null) return String(value ?? "");
  return formatCompactNumber(parsed, field, language);
}
import type { Language } from "./i18n";
