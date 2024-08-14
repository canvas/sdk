import { ReactElement } from "react";
import { Scale } from "./lib/types";
import { Data, Ordinal } from "./lib/types";

export function BubbleChart<DomainValue extends Ordinal>({
  xScale,
  yScale,
  sizes,
  data,
  className,
}: {
  xScale: Scale<DomainValue>;
  yScale: Scale<number>;
  sizes: number[];
  data: Data<DomainValue>;
  className?: string;
}): ReactElement {
  return (
    <g className={className}>
      {data.map((point, index) => {
        const x = xScale.midPoint(point.x);

        const yPoint = Array.isArray(point.y) ? point.y[0] : point.y;
        let y;
        if (yPoint === null || yPoint === undefined) {
          y = (yScale.rangeMin + yScale.rangeMax) / 2;
        } else {
          y = yScale.midPoint(yPoint);
        }
        const size = sizes[index];

        if (size === null) {
          return;
        }

        const bubbleDiameter = Math.sqrt(Math.abs(size));

        return (
          <circle
            cx={x}
            cy={y}
            r={bubbleDiameter / 2}
            style={{ opacity: 0.6 }}
            key={index}
          />
        );
      })}
    </g>
  );
}
