import { ReactElement } from "react";
import { ellipticalArcCurveTo, lineTo } from "./path";

export function Arc({
  startAngle: startAngle_,
  endAngle: endAngle_,
  outerDiameter,
  thickness,
  className,
  rounded = true,
}: {
  startAngle: number;
  endAngle: number;
  outerDiameter: number;
  thickness: number;
  className?: string;
  rounded?: boolean;
}): ReactElement {
  const outerRadius = outerDiameter / 2;

  const reversed_ = Math.abs(startAngle_ - endAngle_) > 180;
  const startAngle = reversed_ ? endAngle_ : startAngle_;
  const endAngle = reversed_ ? startAngle_ : endAngle_;

  //   const startAngle = startAngle_;
  //   const endAngle = endAngle_;
  //   const endAngle = reversed_ ? 360 + endAngle_ : endAngle_;

  const start = {
    x: (Math.cos((startAngle / 360) * 2 * Math.PI) + 1) * outerRadius,
    y: (Math.sin((startAngle / 360) * 2 * Math.PI) + 1) * outerRadius,
  };

  const end = {
    x: (Math.cos((endAngle / 360) * 2 * Math.PI) + 1) * outerRadius,
    y: (Math.sin((endAngle / 360) * 2 * Math.PI) + 1) * outerRadius,
  };

  const reversed = Math.abs(startAngle - endAngle) > 180;
  const size = startAngle <= endAngle ? "small" : "large";
  // const size = "large";

  return (
    <g className={className}>
      <path
        d={`M${start.x} ${start.y} 
                  ${ellipticalArcCurveTo(
                    end.x,
                    end.y,
                    outerRadius,
                    outerRadius,
                    undefined,
                    size,
                    reversed ? "counterclockwise" : "clockwise"
                  )}
                  ${
                    rounded
                      ? ellipticalArcCurveTo(
                          end.x -
                            thickness *
                              Math.cos((endAngle / 360) * 2 * Math.PI),
                          end.y -
                            thickness *
                              Math.sin((endAngle / 360) * 2 * Math.PI),
                          thickness / 2,
                          thickness / 2,
                          0,
                          size,
                          reversed ? "counterclockwise" : "clockwise"
                        )
                      : lineTo(
                          end.x -
                            thickness *
                              Math.cos((endAngle / 360) * 2 * Math.PI),
                          end.y -
                            thickness * Math.sin((endAngle / 360) * 2 * Math.PI)
                        )
                  }
                  ${ellipticalArcCurveTo(
                    start.x +
                      thickness *
                        Math.cos(((180 - startAngle) / 360) * 2 * Math.PI),
                    start.y -
                      thickness *
                        Math.sin(((180 - startAngle) / 360) * 2 * Math.PI),
                    outerRadius - thickness,
                    outerRadius - thickness,
                    undefined,
                    size,
                    reversed ? "clockwise" : "counterclockwise"
                  )}
                  ${
                    rounded
                      ? ellipticalArcCurveTo(
                          start.x,
                          start.y,
                          thickness / 2,
                          thickness / 2,
                          undefined,
                          size,
                          reversed ? "counterclockwise" : "clockwise"
                        )
                      : ""
                  }`}
        style={{
          stroke: "none",
          strokeWidth: 1,
          //   fill: "var(--chart-color-0, #33333350)",
          opacity: 0.6,
        }}
      />
      {/* <path
        d={`M${start.x} ${start.y} 
                  ${ellipticalArcCurveTo(
                    end.x,
                    end.y,
                    outerRadius,
                    outerRadius,
                    undefined,
                    "large",
                    "counterclockwise"
                  )}
                  ${ellipticalArcCurveTo(
                    end.x -
                      thickness * Math.cos((endAngle / 360) * 2 * Math.PI),
                    end.y -
                      thickness * Math.sin((endAngle / 360) * 2 * Math.PI),
                    thickness / 2,
                    thickness / 2,
                    0,
                    "large",
                    "counterclockwise"
                  )}
                  ${ellipticalArcCurveTo(
                    start.x +
                      thickness *
                        Math.cos(((180 - startAngle) / 360) * 2 * Math.PI),
                    start.y -
                      thickness *
                        Math.sin(((180 - startAngle) / 360) * 2 * Math.PI),
                    outerRadius - thickness,
                    outerRadius - thickness,
                    undefined,
                    "large",
                    "clockwise"
                  )}
                  ${ellipticalArcCurveTo(
                    start.x,
                    start.y,
                    thickness / 2,
                    thickness / 2,
                    undefined,
                    "large",
                    "counterclockwise"
                  )}`}
        style={{
          stroke: "none",
          strokeWidth: 1,
          fill: "var(--chart-color-0, #00333350)",
          opacity: 0.4,
        }}
      /> */}
      {/* <circle
        cx={start.x - thickness / 2}
        cy={start.y}
        r={5}
        style={{ fill: reversed ? "blue" : "green" }}
      />
      <circle
        cx={end.x - thickness / 2}
        cy={end.y}
        r={5}
        style={{ fill: reversed ? "green" : "blue" }}
      /> */}
    </g>
  );
}
