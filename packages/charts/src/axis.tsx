import { Fragment, ReactElement, useEffect, useRef } from "react";
import { formatValue } from "./format";
import { Ordinal, Scale } from "./lib/types";

export function XAxis<DomainValue extends Ordinal>({
  xScale,
  y,
  locale,
}: {
  xScale: Scale<DomainValue>;
  y: number;
  locale?: Intl.LocalesArgument;
}): ReactElement {
  const ref = useRef<SVGGElement>(null);

  useEffect(() => {
    if (ref.current) {
      removeOverlappedText(ref.current);
    }
  });

  return (
    <g ref={ref}>
      {xScale.ticks.map((tick, index) => {
        const x = xScale.midPoint(tick);
        const transform = `translate(${x}px, ${y}px)`;
        return (
          <Fragment key={index}>
            <text
              style={{
                transform,
                fill: "currentColor",
                transition: "transform 150ms",
              }}
              x={0}
              y={10}
              textAnchor="middle"
              dominantBaseline="hanging"
            >
              {formatValue(tick, xScale.format, locale)}
            </text>
            {/* <path
              d={`M 0 0 v 5`}
              style={{
                transform,
                stroke: "currentColor",
                opacity: 0.2,
                transition: "transform 150ms",
              }}
              // className="stroke-faded transition-transform [.hidden+&]:hidden"
            /> */}
          </Fragment>
        );
      })}
    </g>
  );
}

export function YAxis({
  yScale,
  width,
  locale,
}: {
  yScale: Scale<number>;
  width: number;
  locale?: Intl.LocalesArgument;
}): ReactElement {
  return (
    <g>
      {yScale.ticks.map((tick, index) => {
        const x = width - 4;
        const y = yScale.position(tick);
        return (
          <text
            style={{
              transform: `translate(${x}px, ${y}px)`,
              transition: "transform 150ms",
              fill: "currentColor",
            }}
            key={index}
            textAnchor="end"
            dominantBaseline="right"
            alignmentBaseline="middle"
          >
            {formatValue(tick, yScale.format, locale)}
          </text>
        );
      })}
    </g>
  );
}

function removeOverlappedText(parent: SVGGElement) {
  const textElements = [...parent.children].filter(
    (element) => element.tagName === "text"
  );

  let prev: Element | null = null;
  let rightMost = 0;
  for (const element of parent.children) {
    if (element.tagName !== "text") {
      continue;
    }

    if (prev) {
      const prevRect = prev.getBoundingClientRect();
      const rect = element.getBoundingClientRect();

      rightMost = Math.max(rightMost, prevRect.right);

      if (rect.left < rightMost) {
        (element as HTMLElement).style.visibility = "hidden";
      } else {
        (element as HTMLElement).style.visibility = "visible";
      }
    }
    prev = element;
  }
}
