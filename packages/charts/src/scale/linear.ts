import { ValueFormat } from "../format";
import {
  getMinDifference,
  roundToNextRoundNumber,
  roundToPreviousRoundNumber,
} from "../lib/math";
import { Scale } from "../lib/types";

export function linearScale(
  domain: number[],
  range: [number, number],
  options?: {
    extendToZero?: boolean;
    lastTick?: "trim" | "extend" | "max";
    format?: ValueFormat;
  }
): Scale<number> | null {
  return numberScale(domain, range, {
    ...options,
    scaleFn: (value: number) => value,
  });
}

export function numberScale(
  domain: number[],
  range: [number, number],
  options?: {
    extendToZero?: boolean;
    lastTick?: "trim" | "extend" | "max";
    format?: ValueFormat;
    scaleFn?: (value: number) => number;
  }
): Scale<number> | null {
  if (domain.length < 1) {
    return null;
  }

  const simpleAxisThreshold = 5;

  const extendToZero = options?.extendToZero ?? true;
  const lastTick = options?.lastTick ?? "extend";
  const format = options?.format;
  const scaleFn = options?.scaleFn ?? ((value) => value);

  const [rangeMin, rangeMax] = range;

  let sortedDomain;

  if (Array.isArray(domain?.[0])) {
    sortedDomain = [
      ...domain.map((t) =>
        (t as any as number[]).reduce((partialSum, a) => partialSum + a, 0)
      ),
    ];
  } else {
    sortedDomain = [...domain];
  }

  sortedDomain.sort((a, b) => a - b);

  const minDiff = getMinDifference(sortedDomain, scaleFn);

  let domainMin = sortedDomain[0] ?? 0;
  let domainMax = sortedDomain[sortedDomain.length - 1] ?? 0;

  if (extendToZero) {
    domainMin = Math.min(0, domainMin);
    domainMax = Math.max(0, domainMax);
  }

  if (lastTick === "extend") {
    domainMax = roundToNextRoundNumber(domainMax);
    if (domainMin < 0) {
      domainMin = -roundToNextRoundNumber(-domainMin);
    }
  }

  let tickMax = domainMax;
  if (lastTick === "trim" && domain.length > simpleAxisThreshold) {
    tickMax = roundToPreviousRoundNumber(domainMax);
  }

  let tickCount;

  if (tickMax === 1) {
    tickCount = 2;
  } else if (tickMax - domainMin <= 5) {
    tickCount = tickMax - domainMin + 1;
  } else if (tickMax % 4 === 0) {
    tickCount = 5;
  } else if (tickMax % 5 === 0) {
    tickCount = 6;
  } else if (tickMax % 3 === 0) {
    tickCount = 4;
  } else {
    tickCount = 2;
  }

  const tickDelta = (tickMax - domainMin) / (Math.max(tickCount, 2) - 1);

  const ticks = [Math.max(0, domainMin)];

  for (let i = 1; i < 10; i++) {
    const tick = Math.max(0, domainMin) + tickDelta * i;
    if (tick > domainMax) {
      break;
    }
    ticks.push(tick);
  }

  if (domainMin < 0) {
    for (let i = 1; i < 10; i++) {
      const tick = Math.min(0, domainMax) - tickDelta * i;
      if (tick < domainMin) {
        break;
      }
      ticks.unshift(tick);
    }
  }

  const rangeWidth = rangeMax - rangeMin;
  const domainWidth = scaleFn(domainMax) - scaleFn(domainMin) || 1;

  let bandWidth = rangeWidth / domainWidth;
  bandWidth = Math.min(bandWidth, rangeWidth / domain.length);
  if (minDiff) {
    bandWidth = Math.min(bandWidth, rangeWidth * (minDiff / domainWidth));
  }
  bandWidth = Math.max(2, bandWidth);

  function size(domainValue: number) {
    const domainPos = (scaleFn(domainValue) - scaleFn(0)) / domainWidth;
    return domainPos * (rangeWidth - bandWidth);
  }

  function domainPosition(domainValue: number) {
    const domainPos = (scaleFn(domainValue) - scaleFn(domainMin)) / domainWidth;
    return domainPos * (rangeWidth - bandWidth);
  }

  function position(domainValue: number) {
    return domainPosition(domainValue) + rangeMin;
  }

  function midPoint(domainValue: number) {
    return position(domainValue) + bandWidth / 2;
  }

  return {
    size,
    position,
    midPoint,
    format,
    ticks,
    bandWidth,
    domainMin,
    domainMax,
    rangeMin,
    rangeMax,
  };
}
