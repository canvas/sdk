import ReactLangIcon from "@/icons/lang/react.svg";
import TypescriptIcon from "@/icons/lang/typescript.svg";

export function LanguageIcon({
  language,
  className,
}: {
  language: string;
  className?: string;
}) {
  switch (language) {
    case "tsx":
      return (
        <ReactLangIcon
          className={`${className} mt-px`}
          style={{ width: 12, height: 12 }}
        />
      );
    case "typescript":
      return (
        <TypescriptIcon
          className={`${className} -mt-px`}
          style={{ width: 12, height: 12 }}
        />
      );
    default:
      return null;
  }
}
