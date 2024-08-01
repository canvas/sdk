"use client";

import { useRef } from "react";
import { dateTimeScale } from "./scale/datetime";
import { linearScale } from "./scale/linear";
import { LineChart } from "./line";
import { categoricalScale } from "./scale";
import { VerticalBarChart } from "./bar";
import { XAxis, YAxis } from "./axis";
import { useElementWidth } from "./lib/responsive";
import { BubbleChart } from "./bubble";

export type ChartOptions = {
  locale?: Intl.LocalesArgument;
  showAxis?: boolean;
};

type TableColumn = any; /* TODO */

export function Chart({
  x,
  y,
  type,
  width: argWidth,
  height = 300,
  options = {},
}: {
  x: TableColumn;
  y: TableColumn;
  type: "line" | "bar" | "bubble";
  width?: number;
  height?: number;
  options?: ChartOptions;
}) {
  const yAxisRef = useRef<SVGGElement>(null);
  const sizeDivRef = useRef<HTMLDivElement>(null);

  const showAxis = options.showAxis === true;
  const yAxisRefWidth = useElementWidth(yAxisRef);
  const parentWidth = useElementWidth(sizeDivRef);
  const yAxisTextWidth = showAxis ? yAxisRefWidth : 0;
  const yAxisMargin = showAxis === true ? 20 : 0;

  const width = argWidth !== undefined ? argWidth : Math.max(200, parentWidth);

  const xAxisHeight = showAxis ? 24 : 0;

  const topMargin = showAxis ? 24 : 0;
  const bottomMargin = showAxis ? 4 : 0;

  const planeLeft = yAxisTextWidth + yAxisMargin;
  const yAxisWidth = yAxisTextWidth + yAxisMargin;
  const planeRight = width;

  const planeTop = topMargin;
  const planeBottom = height - xAxisHeight - bottomMargin;

  const data = { x, y };

  if (!data.x || !data.y) {
    return (
      <div ref={sizeDivRef} className="w-full flex-1">
        <div style={{ height }}>Loading...</div>
      </div>
    );
  }

  let xScale;
  if (data.x?.[0] instanceof Date) {
    xScale = dateTimeScale(data.x as Date[], [planeLeft, planeRight]);
  } else {
    xScale = categoricalScale(data.x as any, [planeLeft, planeRight]);
  }
  const yScale = linearScale(data.y as number[], [planeBottom, planeTop], {
    extendToZero: true,
    lastTick: "extend",
  });

  if (!xScale || !yScale) {
    return <></>;
  }

  return (
    <div ref={sizeDivRef} className="w-full flex-1">
      <svg width={width} height={height}>
        {type === "line" && (
          <g style={{ stroke: "currentColor" }}>
            <LineChart
              xScale={xScale as any}
              yScale={yScale}
              data={
                data.x.map((x: any, index: number) => ({
                  x,
                  y: data.y?.[index],
                })) as any
              }
            />
          </g>
        )}

        {type === "bar" && (
          <g style={{ fill: "currentColor", stroke: "none" }}>
            <VerticalBarChart
              xScale={xScale as any}
              yScale={yScale}
              data={
                data.x.map((x: any, index: number) => ({
                  x,
                  y: data.y?.[index],
                })) as any
              }
            />
          </g>
        )}

        {type === "bubble" && (
          <g>
            <BubbleChart
              xScale={xScale as any}
              yScale={yScale}
              sizeScale={yScale}
              data={
                data.x.map((x: any, index: number) => ({
                  x,
                  y: data.y?.[index],
                })) as any
              }
            />
          </g>
        )}

        {options?.showAxis ? (
          <g style={{ stroke: "none", fill: "#333", fontSize: 10 }}>
            <XAxis
              xScale={xScale as any}
              y={yScale.rangeMin}
              locale={options.locale}
              key={width}
            />
            <g ref={yAxisRef}>
              <YAxis
                yScale={yScale}
                width={yAxisWidth}
                locale={options.locale}
              />
            </g>
          </g>
        ) : null}
      </svg>
    </div>
  );
}
