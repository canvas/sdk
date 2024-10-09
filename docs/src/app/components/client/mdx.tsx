"use client";

import { setLanguage } from "@/app/actions/set-language";
import { ReactElement, ReactNode, useState } from "react";

export function ClientCodeSwitcher({
  children,
  savedLanguage,
}: {
  children: ReactElement[];
  savedLanguage: string | undefined;
}) {
  const [activeTab, setActiveTab] = useState<string | undefined>(savedLanguage);

  let tab = null;
  const tabs = [];
  for (const child of children) {
    tabs.push(child.props.name);
    if (child.props.name === activeTab) {
      tab = child;
    }
  }

  if (!tab) {
    tab = children[0];
  }

  return (
    <div className="border-marble-900/10 border rounded-lg shadow-box my-[1.6666667em]">
      <div className="text-[12px] text-marble-800 px-4 py-2 border-b bg-marble-50 hover:bg-marble-100 border-b-marble-900/10 rounded-t-lg flex gap-4">
        {tabs.map((tabName, index) => {
          const isActive = tabName === activeTab || (!activeTab && index === 0);
          return (
            <div
              key={tabName}
              onClick={() => {
                setActiveTab(tabName);
                setLanguage(tabName);
              }}
              className={`cursor-pointer ${isActive ? "font-semibold" : ""}`}
            >
              {tabName}
            </div>
          );
        })}
      </div>

      {tab}
    </div>
  );
}

export function CodeTab({
  name: _name,
  className,
  children,
}: {
  name: string;
  className?: string;
  children: ReactNode;
}) {
  return <div className={className}>{children}</div>;
}
