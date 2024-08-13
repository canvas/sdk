import { Ordinal } from "./lib/types";

export type ValueFormat =
  | { type: "decimal" }
  | { type: "currency"; currency: string; notation?: "standard" | "compact" }
  | { type: "percent" }
  | { type: "datetime"; year?: "numeric"; month?: "short"; day?: "numeric" };

export function formatValue(
  value: Ordinal,
  format: ValueFormat = { type: "decimal" },
  locale?: Intl.LocalesArgument
): string {
  if (value instanceof Date) {
    const fmt =
      format.type === "datetime"
        ? format
        : ({
            year: "numeric",
            month: "short",
            day: "numeric",
          } as const);

    return Intl.DateTimeFormat(locale, {
      year: fmt.year,
      month: fmt.month,
      day: fmt.day,
    }).format(value);
  }

  if (typeof value === "string") {
    return value;
  }

  switch (format.type) {
    case "decimal":
      return Intl.NumberFormat(locale, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 1,
        notation: "compact",
      }).format(value);
    case "currency":
      return Intl.NumberFormat(locale, {
        style: "currency",
        currency: format.currency,
        notation: format.notation,
      }).format(value);
    case "percent":
      return Intl.NumberFormat(locale, { style: "percent" }).format(value);
    case "datetime":
      return Intl.DateTimeFormat(locale, {
        year: format.year,
        month: format.month,
        day: format.day,
      }).format(value);
  }
}
