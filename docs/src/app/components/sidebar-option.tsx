import Link from "next/link";

type Props = {
  label: string;
  href: string;
  icon?: React.ReactNode;
  selected: boolean;
};
export function SidebarOption(props: Props) {
  const colors = props.selected
    ? "text-white bg-[#2d68ff]"
    : "text-marble-700 hover:bg-marble-900/5 hover:text-marble-900";
  return (
    <Link href={props.href}>
      <div
        className={`flex h-8 items-center gap-4 rounded-md px-3 text-[13px] font-medium leading-6 ${colors}`}
      >
        {props.icon && (
          <div className="flex h-4 w-4 items-center justify-center">
            {props.icon}
          </div>
        )}
        <div className="flex-1 select-none">{props.label}</div>
      </div>
    </Link>
  );
}
