import { ReactElement } from "react";
import { ClientCodeSwitcher } from "@/app/components/client/mdx";
import { cookies } from "next/headers";

export function CodeSwitcher({ children }: { children: ReactElement[] }) {
  const savedLanguage = cookies().get("language")?.value;

  return (
    <ClientCodeSwitcher savedLanguage={savedLanguage}>
      {children}
    </ClientCodeSwitcher>
  );
}
