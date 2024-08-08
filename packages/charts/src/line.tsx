import { Fragment, ReactElement } from "react";
import { Scale } from "./lib/types";
import { Data, Ordinal } from "./lib/types";
import { lineTo, moveCursorTo, roundPixel } from "./svg/path";

export function LineChart<DomainValue extends Ordinal>({
  xScale,
  yScale,
  data,
  className,
  options,
}: {
  xScale: Scale<DomainValue>;
  yScale: Scale<number>;
  data: Data<DomainValue>;
  className?: string;
  options?: {
    gradient?: string;
    showMarkers?: boolean;
  };
}): ReactElement {
  const gradient = options?.gradient;

  const firstY = data[0]?.y ?? [];
  const seriesCount = (Array.isArray(firstY) ? firstY : [firstY]).length;
  const paths: string[][] = new Array(seriesCount).fill(null).map((_) => {
    return [];
  });

  const markers: { x: number; y: number }[][] = paths.map((_) => []);
  const showMarkers = options?.showMarkers ?? data.length <= 5;

  data.forEach((point, pointIndex) => {
    const values = Array.isArray(point.y) ? point.y : [point.y];

    const x = xScale.midPoint(point.x);

    values.forEach((value, valueIndex) => {
      const y = yScale.position(value as any);
      if (Number.isNaN(y)) {
        return;
      }

      const command =
        pointIndex === 0
          ? moveCursorTo(roundPixel(x), roundPixel(y))
          : lineTo(roundPixel(x), roundPixel(y));
      const path = paths[valueIndex];
      if (path) {
        path.push(command);
      }

      if (showMarkers) {
        if (markers[valueIndex]) {
          markers[valueIndex].push({ x, y });
        }
      }
    });
  });

  return (
    <>
      <g
        className={className}
        style={{
          strokeWidth: 2,
          stroke: "currentColor",
          fill: "none",
        }}
      >
        {paths.map((path, index) => {
          const cssColor = `var(--chart-color-${index}, currentcolor)`;

          return (
            <path key={index} d={path.join(" ")} style={{ stroke: cssColor }} />
          );
        })}
      </g>
      {showMarkers ? (
        <g>
          {markers.map((series, index) => {
            const cssColor = `var(--chart-color-${index}, currentcolor)`;

            return (
              <Fragment key={index}>
                {series.map((point, index) => {
                  return (
                    <circle
                      key={index}
                      r={3.5}
                      cx={point.x}
                      cy={point.y}
                      style={{ fill: cssColor, stroke: "none" }}
                    />
                  );
                })}
              </Fragment>
            );
          })}
        </g>
      ) : null}
      {gradient && (
        <g className={className} /*filter="url(#noise)"*/>
          {paths.map((path, index) => {
            return (
              <path
                key={index}
                d={`${path.join(" ")} L ${xScale.rangeMax},${
                  yScale.rangeMin
                } L ${xScale.rangeMin},${yScale.rangeMin} Z`}
                style={{ stroke: "none" }}
                fill={`url(#${gradient})`}
              />
            );
          })}
        </g>
      )}
    </>
  );
}
