import { ReactNode } from "react";
import { SidebarOption } from "../components/sidebar-option";
import { Header } from "../components/layout/header";
import localFont from "next/font/local";
import { Inter, Inconsolata } from "next/font/google";

import "./syntax-highlighting.css";

export const twkLausanne = localFont({
  src: [
    {
      path: "../../fonts/TWKLausanne/TWKLausanne-400.woff2",
      weight: "400",
      style: "normal",
    },
  ],
  variable: "--font-twklausanne",
});
const inter = Inter({ subsets: ["latin"] });
const inconsolata = Inconsolata({
  subsets: ["latin"],
  variable: "--font-inconsolata",
});

export const sections = [
  {
    label: "Introduction",
    slug: "index",
  },

  { section: "Charts" },
  {
    label: "Bubble",
    slug: "charts/bubble",
  },
];

export function sectionUrl(slug: string) {
  if (slug === "index") {
    return "/";
  }

  return `/${slug}`;
}

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div
      className={`${twkLausanne.variable} ${inter.className} ${inconsolata.variable}`}
    >
      <Header filledBackground={true} subtitle={""} description={""} />

      <main className="relative mx-auto mt-16 flex max-w-[1148px]">
        <div className="top-[192px] flex flex-col gap-6 self-start lg:w-[256px] shrink-0">
          <div className="flex flex-col">
            {sections.map((section, index) =>
              section.section ? (
                <div
                  key={index}
                  className="mt-4 px-3 text-[11px] font-semibold leading-6 text-marble-400"
                >
                  {section.section}
                </div>
              ) : section.label && section.slug ? (
                <SidebarOption
                  key={index}
                  label={section.label}
                  href={sectionUrl(section.slug)}
                  selected={false}
                />
              ) : null
            )}
          </div>
        </div>

        <div className="prose prose-sm relative ml-16 flex flex-1 max-w-[1024px] flex-col px-4 prose-headings:font-semibold prose-headings:text-marble-900 prose-h1:text-2xl prose-h1:tracking-[.-0.025em] max-sm:prose-table:block max-sm:prose-table:overflow-auto">
          <div className="mb-8 [.content-mobile_&]:max-w-[390px] max-w-[1024px] [.content-mobile_&]:outline-2 [.content-mobile_&]:outline outline-marble-900 [.content-mobile_&]:px-4 [.content-mobile_&]:py-4 [.content-mobile_&]:flex [.content-mobile_&]:justify-center [.content-mobile_&]:bg-marble-50/30 rounded-lg [.content-mobile_&]:overflow-x-auto transition-all">
            <article className="text-black max-w-[1024px] [.content-mobile_&]:max-w-[356px]">
              {children}
            </article>
          </div>
        </div>
      </main>
    </div>
  );
}
