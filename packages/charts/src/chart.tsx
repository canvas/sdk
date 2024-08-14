"use client";

import { useEffect, useRef, useState } from "react";
import { dateTimeScale } from "./scale/datetime";
import { linearScale } from "./scale/linear";
import { LineChart } from "./line";
import { categoricalScale } from "./scale";
import { VerticalBarChart } from "./bar";
import { XAxis, YAxis } from "./axis";
import { useElementWidth } from "./lib/responsive";
import { BubbleChart } from "./bubble";

export type Padding =
  | {
      top?: number;
      bottom?: number;
      left?: number;
      right?: number;
      vertical?: number;
      horizontal?: number;
    }
  | number;

export type ChartOptions = {
  locale?: Intl.LocalesArgument;
  showAxis?: boolean;
  svgPadding?: Padding;
  hugContainer?: boolean;
};

/* TODO: this type is kinda bad, will fix once everything stabilizes */
type TableColumn =
  | unknown[]
  | { data: unknown[] }
  | { fetch: () => Promise<unknown[]> };

export function Chart({
  x,
  y,
  sizes,
  type,
  svgClassName,
  width: argWidth,
  height = 300,
  options = {},
}: {
  x: TableColumn;
  y: TableColumn;
  sizes?: number[];
  type: "line" | "bar" | "bubble";
  svgClassName?: string;
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

  const padding = paddingRect(options.svgPadding ?? 0);

  const xAxisHeight = showAxis ? 24 : 0;

  const topMargin = padding.top + (showAxis ? 24 : 0);
  const bottomMargin = padding.bottom + (showAxis ? 4 : 0);

  const planeLeft = yAxisTextWidth + yAxisMargin + padding.left;
  const yAxisWidth = yAxisTextWidth + yAxisMargin;
  const planeRight = width - padding.right;

  const planeTop = topMargin;
  const planeBottom = height - xAxisHeight - bottomMargin;

  const xData = useData(x);
  const yData = useData(y);

  if (!xData || !yData) {
    return (
      <div ref={sizeDivRef} className="w-full flex-1">
        <div style={{ height }}>Loading...</div>
      </div>
    );
  }

  let xScale;
  if (xData?.[0] instanceof Date) {
    xScale = dateTimeScale(xData as Date[], [planeLeft, planeRight], {
      hugContainer: options.hugContainer,
    });
  } else if (typeof xData?.[0] === "number") {
    xScale = linearScale(xData as number[], [planeLeft, planeRight], {
      lastTick: "max",
    });
  } else {
    xScale = categoricalScale(xData as any, [planeLeft, planeRight]);
  }
  const yScale = linearScale(yData as number[], [planeBottom, planeTop], {
    extendToZero: true,
    lastTick: "extend",
  });

  if (!xScale || !yScale) {
    return <></>;
  }

  return (
    <div ref={sizeDivRef} className="w-full flex-1">
      <svg width={width} height={height} className={svgClassName}>
        {type === "line" && (
          <g style={{ stroke: "currentColor" }}>
            <LineChart
              xScale={xScale as any}
              yScale={yScale}
              data={
                xData.map((x: any, index: number) => ({
                  x,
                  y: yData?.[index],
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
                xData.map((x: any, index: number) => ({
                  x,
                  y: yData?.[index],
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
              sizes={sizes}
              data={
                xData.map((x: any, index: number) => ({
                  x,
                  y: yData?.[index],
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

function useData(axis: TableColumn) {
  const [_data, setData] = useState<unknown[] | undefined>(undefined);

  useEffect(() => {
    if ("fetch" in axis) {
      (async () => {
        const fetchedData = await axis.fetch();
        setData(fetchedData);
      })();
    }
  }, [axis]);

  if (Array.isArray(axis)) {
    return axis;
  } else if ("data" in axis) {
    return axis.data;
  } else if ("fetch" in axis) {
    return _data;
  } else {
    return undefined;
  }
}

function paddingRect(value: Padding) {
  if (typeof value === "number") {
    return { left: value, right: value, top: value, bottom: value };
  } else {n
    return {
      left: value.left ?? value.horizontal ?? 0,
      right: value.right ?? value.horizontal ?? 0,
      top: value.top ?? value.vertical ?? 0,
      bottom: value.bottom ?? value.vertical ?? 0,
    };
  }
}
