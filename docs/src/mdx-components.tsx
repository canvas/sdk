import type { MDXComponents } from "mdx/types";
import { Fragment, ReactNode, cloneElement, isValidElement } from "react";
import { LanguageIcon } from "./app/components/client/language-icon";
import { CodeTab } from "./app/components/client/mdx";

export function useMDXComponents(components: MDXComponents): MDXComponents {
  return {
    ...components,
    pre: CodePre,
    code: ({ children, ...props }) => {
      return <>{children}</>;
    },
    CodeTab,
  };
}

function CodePre({
  className,
  filename,
  children,
  ...props
}: {
  className?: string;
  filename?: string;
  children?: ReactNode;
}) {
  return (
    <div className="border-marble-900/10 border rounded-lg shadow-box my-[1.6666667em] [.shadow-box_&]:my-0 [.shadow-box_&]:border-0">
      {filename && (
        <div className="text-[12px] font-semibold text-marble-800 px-4 py-2 border-b bg-marble-50 hover:bg-marble-100 border-b-marble-900/10 rounded-t-lg flex gap-1.5 items-center">
          <LanguageIcon
            language={className?.replace("language-", "") ?? ""}
            className="size-3 w-3 h-3"
          />
          <div>{filename}</div>
        </div>
      )}
      <pre
        className={`${className} bg-transparent font-code my-0`}
        style={{ background: "transparent" }}
      >
        <TextReplace vars={props}>{children}</TextReplace>
      </pre>
    </div>
  );
}

function TextReplace({
  children,
  vars,
}: {
  children: ReactNode;
  vars: { [key: string]: string };
}) {
  function replace(node: ReactNode) {
    if (isValidElement(node)) {
      let props = node.props;

      if (Array.isArray(props.children)) {
        props = {
          ...props,
          children: props.children.map((child: ReactNode, index: number) => (
            <Fragment key={index}>{replace(child)}</Fragment>
          )),
        };
      } else {
        props = {
          ...props,
          children: replace(props.children),
        };
      }
      return cloneElement(node, props);
    } else if (typeof node === "string") {
      return node.replace(/\$\$([a-zA-Z])+/g, (match) => {
        return vars[match.substring(2)] ?? match;
      });
    } else {
      return node;
    }
  }

  return replace(children);
}
