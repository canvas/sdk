import { RefObject, useEffect, useState } from "react";

export function useElementWidth<DOMElement extends Element>(
  elementRef: RefObject<DOMElement>
) {
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const resizeObserver = new ResizeObserver((entries) => {
      if (entries.length > 0) {
        setWidth(entries[entries.length - 1].contentRect.width);
      }
    });

    if (elementRef.current) {
      resizeObserver.observe(elementRef.current);
    }

    return () => {
      resizeObserver.disconnect();
    };
  });

  return width;
}
