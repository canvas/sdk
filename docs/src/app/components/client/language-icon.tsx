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
      return <ReactLangIcon className={`${className} mt-px`} />;
    case "typescript":
      return <TypescriptIcon className={`${className} -mt-px`} />;
    default:
      return null;
  }
}
