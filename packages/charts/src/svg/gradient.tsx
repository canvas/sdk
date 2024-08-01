import { ReactNode, useId } from "react";

type GradientDirection = "top" | "bottom" | "left" | "right";
type GradientStop = {
  offset: string;
  color: string;
  opacity: number;
};
export function useLinearGradient(
  to: GradientDirection,
  stops: GradientStop[]
): [string, ReactNode] {
  const id = `gradient-${useId()}`;
  let points;
  switch (to) {
    case "top":
      points = ["0%", "100%", "0%", "0%"];
      break;
    case "bottom":
      points = ["0%", "0%", "0%", "100%"];
      break;
    case "left":
      points = ["100%", "0%", "0%", "0%"];
      break;
    case "right":
      points = ["0%", "0%", "100%", "0%"];
      break;
  }
  const [x1, y1, x2, y2] = points;
  const gradient = (
    <linearGradient id={id} x1={x1} y1={y1} x2={x2} y2={y2}>
      {stops.map(({ offset, color, opacity }, index) => {
        return (
          <stop
            key={index}
            offset={offset}
            style={{ stopColor: color, stopOpacity: opacity }}
          />
        );
      })}
    </linearGradient>
  );

  return [id, gradient];
}
