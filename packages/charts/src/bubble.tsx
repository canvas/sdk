import { ReactElement } from "react";
import { Scale } from "./lib/types";
import { Data, Ordinal } from "./lib/types";

export function BubbleChart<DomainValue extends Ordinal>({
  xScale,
  yScale,
  sizeScale,
  data,
  className,
}: {
  xScale: Scale<DomainValue>;
  yScale: Scale<number>;
  sizeScale: Scale<number>;
  data: Data<DomainValue>;
  className?: string;
}): ReactElement {
  return (
    <g className={className}>
      {data.map((point, index) => {
        const x = xScale.midPoint(point.x);
        const y = (yScale.rangeMin + yScale.rangeMax) / 2;
        const size = Array.isArray(point.y) ? point.y[0] : point.y;

        if (size === null) {
          return;
        }
        const bubbleSize = Math.sqrt(Math.abs(yScale.size(size)));

        return (
          <circle
            cx={10 + x}
            cy={y}
            r={bubbleSize}
            style={{ opacity: 0.6 }}
            key={index}
          />
        );
      })}
    </g>
  );
}
