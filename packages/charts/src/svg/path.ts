export function moveCursorTo(x: number, y: number): string {
  return `M${round(x)} ${round(y)}`;
}

export function moveCursor(deltaX: number, deltaY: number): string {
  return `m${round(deltaX)} ${round(deltaY)}`;
}

export function verticalLineTo(y: number): string {
  return `V${round(y)}`;
}

export function verticalLine(deltaY: number): string {
  return `v${round(deltaY)}`;
}

export function lineTo(x: number, y: number): string {
  return `L${round(x)} ${round(y)}`;
}

export function line(deltaX: number, deltaY: number): string {
  return `l${round(deltaX)} ${round(deltaY)}`;
}

export function horizontalLineTo(x: number): string {
  return `H${round(x)}`;
}

export function horizontalLine(deltaX: number): string {
  return `h${round(deltaX)}`;
}

export function bezierCurveTo(
  x1: number,
  y1: number,
  x2: number,
  y2: number,
  x: number,
  y: number
): string {
  return `C${round(x1)} ${round(y1)} ${round(x2)} ${round(y2)} ${round(
    x
  )} ${round(y)}`;
}

export function ellipticalArcCurveTo(
  x: number,
  y: number,
  radiusX: number,
  radiusY: number,
  angle = 0,
  arc: "large" | "small" = "small",
  sweep: "clockwise" | "counterclockwise" = "clockwise"
): string {
  const arcParam = arc === "large" ? 1 : 0;
  const sweepParam = sweep === "clockwise" ? 1 : 0;

  return `A${round(radiusX)} ${round(radiusY)} ${round(
    angle
  )} ${arcParam} ${sweepParam} ${round(x)} ${round(y)}`;
}

export function ellipticalArcCurve(
  deltaX: number,
  deltaY: number,
  radiusX: number,
  radiusY: number,
  angle = 0,
  arc: "large" | "small" = "small",
  sweep: "clockwise" | "counterclockwise" = "clockwise"
): string {
  const arcParam = arc === "large" ? 1 : 0;
  const sweepParam = sweep === "clockwise" ? 1 : 0;

  return `a${round(radiusX)} ${round(radiusY)} ${round(
    angle
  )} ${arcParam} ${sweepParam} ${round(deltaX)} ${round(deltaY)}`;
}

/**
 * Round a pixel value to at most one digit. This considerably reduces the sizes of svg paths.
 *
 * Safe because our viewboxes roughly correspond to logical pixels.
 */
export function round(value: number) {
  return Math.round(value * 10) / 10;
}
