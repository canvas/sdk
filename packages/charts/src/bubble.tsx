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
        const size = yScale.midPoint(point.y);

        return (
          <circle
            cx={10 + x}
            cy={y}
            r={Math.sqrt(size)}
            style={{ opacity: 0.6 }}
            key={index}
          />
        );
      })}
    </g>
  );
}
