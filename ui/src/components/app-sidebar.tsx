import { useQuery } from "@tanstack/react-query";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  ActivityIcon,
  CalendarClockIcon,
  ChartNoAxesCombinedIcon,
  GaugeIcon,
  GitForkIcon,
  ListChecksIcon,
  ServerIcon,
  ShieldAlertIcon,
} from "lucide-react";

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { api } from "@/lib/api";
import { config } from "@/lib/config";

interface RuntimeMeta {
  version?: string;
}

const navigation = [
  { icon: ChartNoAxesCombinedIcon, label: "Overview", to: "/overview" },
  { icon: GaugeIcon, label: "Queues", to: "/queues" },
  { icon: ListChecksIcon, label: "Jobs", to: "/jobs" },
  { icon: GitForkIcon, label: "Workflows", to: "/workflows" },
  { icon: ActivityIcon, label: "Rate classes", to: "/rate-classes" },
  { icon: ShieldAlertIcon, label: "Quarantine", to: "/quarantine" },
  { icon: CalendarClockIcon, label: "Periodic", to: "/periodic" },
  { icon: ServerIcon, label: "Workers", to: "/workers" },
] as const;

export function AppSidebar(props: React.ComponentProps<typeof Sidebar>) {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const metaQuery = useQuery({
    queryFn: ({ signal }) => api<RuntimeMeta>("/meta", { signal }),
    queryKey: ["api", "meta"],
    retry: 1,
    staleTime: Number.POSITIVE_INFINITY,
  });
  const runtimeVersion = metaQuery.data?.version?.trim();
  const versionLabel = runtimeVersion
    ? `${VERSION_NUMBER_PREFIX.test(runtimeVersion) ? "v" : ""}${runtimeVersion}`
    : metaQuery.isPending
      ? "…"
      : "version unavailable";

  return (
    <Sidebar collapsible="icon" variant="inset" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              render={<Link to="/overview" />}
              size="lg"
              tooltip="headgate"
            >
              <img
                alt=""
                className="size-8 shrink-0"
                height="32"
                src="/favicon.svg"
                width="32"
              />
              <div className="grid flex-1 text-left leading-tight">
                <span className="truncate font-semibold">headgate</span>
                <span className="truncate text-xs">operations console</span>
              </div>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Operate</SidebarGroupLabel>
          <SidebarMenu className="gap-0.5">
            {navigation.map((item) => (
              <SidebarMenuItem key={item.to}>
                <SidebarMenuButton
                  isActive={
                    pathname === item.to || pathname.startsWith(`${item.to}/`)
                  }
                  render={<Link to={item.to} />}
                  tooltip={item.label}
                >
                  <item.icon />
                  <span>{item.label}</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <div className="space-y-0.5 px-2 text-sidebar-foreground/65 text-xs group-data-[collapsible=icon]:hidden">
          <p>
            Headgate{" "}
            <span className="font-mono" translate="no">
              {versionLabel}
            </span>
          </p>
          <p className="truncate" title={`API ${config.apiBase}`}>
            API {config.apiBase}
          </p>
        </div>
      </SidebarFooter>
      {/*<SidebarRail />*/}
    </Sidebar>
  );
}
const VERSION_NUMBER_PREFIX = /^\d/;
