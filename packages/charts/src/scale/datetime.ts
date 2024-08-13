import { getMinDifference } from "../lib/math";
import { Scale } from "../lib/types";

export function dateTimeScale(
  domain: Date[],
  range: [number, number]
): Scale<Date> | null {
  const sortedDomain = [...domain];
  sortedDomain.sort((a, b) => a.getTime() - b.getTime());

  const minDiffDays =
    getMinDifference(sortedDomain, (value) => value.getTime()) /
    1000 /
    60 /
    60 /
    24;

  const domainMin = sortedDomain[0];
  const domainMax = sortedDomain[sortedDomain.length - 1];

  const [rangeMin, rangeMax] = range;

  if (domainMin === undefined || domainMax === undefined) {
    return null;
  }

  let resolution;
  let bandWidthDivisor;

  if (
    sortedDomain.every(
      (value) => value.getUTCDate() === sortedDomain[0]?.getUTCDate()
    )
  ) {
    resolution = "month";
    bandWidthDivisor = 30;
  } else if (
    sortedDomain.every(
      (value) => value.getUTCDay() === sortedDomain[0]?.getUTCDay()
    )
  ) {
    resolution = "week";
    bandWidthDivisor = 7;
  } else {
    resolution = "day";
    bandWidthDivisor = 1;
  }

  const ticks: Date[] = [];
  const dateRangeDays =
    Math.abs(domainMax.getTime() - domainMin.getTime()) / 1000 / 60 / 60 / 24;
  if (dateRangeDays > 700) {
    for (
      let year = domainMin.getFullYear() + 1;
      year <= domainMax.getFullYear();
      year += 1
    ) {
      ticks.push(new Date(year, 0, 1));
    }
  } else {
    const tickCount = Math.max(2, Math.min(sortedDomain.length, 6));

    const halves = sortedDomain.length > 8;

    const tickDelta = Math.floor(dateRangeDays / (tickCount - 1));

    for (let i = 0; i < tickCount - (halves ? 1 : 0); i += 1) {
      const tick = new Date(domainMin);

      tick.setDate(domainMin.getDate() + tickDelta * (i + (halves ? 0.5 : 0)));

      if (tick.getTime() > domainMax.getTime()) {
        break;
      }
      ticks.push(tick);
    }
  }

  const rangeWidth = rangeMax - rangeMin;
  const domainWidth = domainMax.getTime() - domainMin.getTime() || 1;

  let bandWidth =
    rangeWidth / (domainWidth / (1000 * 60 * 60 * 24 * bandWidthDivisor));

  bandWidth = Math.min(bandWidth, rangeWidth / domain.length);
  bandWidth = Math.max(2, bandWidth);

  function size(domainValue: Date) {
    const domainPos =
      (domainValue.getTime() - (domainMin?.getTime() ?? 0)) / domainWidth;
    return domainPos * (rangeWidth - bandWidth);
  }

  function position(domainValue: Date) {
    return size(domainValue) + rangeMin;
  }

  function midPoint(domainValue: Date) {
    return position(domainValue) + bandWidth / 2;
  }

  const format = {
    type: "datetime",
    year:
      domainMin.getUTCFullYear() !== domainMax.getUTCFullYear()
        ? "numeric"
        : undefined,
    month: "short",
    day: resolution === "month" ? undefined : "numeric",
  } as const;

  return {
    size,
    position,
    midPoint,
    ticks,
    format,
    bandWidth,
    domainMin,
    domainMax,
    rangeMin,
    rangeMax,
  };
}
