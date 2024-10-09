import { ReactElement } from "react";
import { Data, Ordinal } from "./lib/types";
import { Arc } from "./svg/arc";

export function DonutChart<DomainValue extends Ordinal>({
  size,
  data,
}: {
  size: number;
  data: Data<DomainValue>;
}): ReactElement {
  const arcs: { startAngle: number; endAngle: number }[] = [];

  const values = data
    .map((point) => {
      if (Array.isArray(point.y)) {
        return point.y[0];
      }
      return point.y;
    })
    .filter((value): value is number => value != null);

  if (values.length === 0) {
    return <>No data</>;
  }

  const sum = values.reduce((acc, value) => acc + value, 0);

  let angle = 0;
  values.forEach((value, valueIndex) => {
    const ySize = value / sum;
    if (Number.isNaN(ySize)) {
      return;
    }

    const angularSize = 360 * ySize;

    arcs.push({
      startAngle: angle,
      endAngle: angle + angularSize,
    });

    angle += angularSize;
  });

  return (
    <>
      {arcs.map((arc, arcIndex) => {
        const cssColor = `var(--chart-color-${arcIndex})`;

        return (
          <g style={{ fill: cssColor }}>
            <Arc
              key={arcIndex}
              outerDiameter={size}
              thickness={Math.max(1, size / 10)}
              startAngle={arc.startAngle}
              endAngle={arc.endAngle}
              rounded={false}
            />
          </g>
        );
      })}
    </>
  );
}
