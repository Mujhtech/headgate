import { Link, useRouterState } from "@tanstack/react-router"
import {
  ActivityIcon,
  CalendarClockIcon,
  GaugeIcon,
  GitForkIcon,
  ListChecksIcon,
  ServerIcon,
  ShieldAlertIcon,
} from "lucide-react"

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
  SidebarRail,
} from "@/components/ui/sidebar"
import { config } from "@/lib/config"

const navigation = [
  { to: "/queues", label: "Queues", icon: GaugeIcon },
  { to: "/jobs", label: "Jobs", icon: ListChecksIcon },
  { to: "/workflows", label: "Workflows", icon: GitForkIcon },
  { to: "/rate-classes", label: "Rate classes", icon: ActivityIcon },
  { to: "/quarantine", label: "Quarantine", icon: ShieldAlertIcon },
  { to: "/periodic", label: "Periodic", icon: CalendarClockIcon },
  { to: "/workers", label: "Workers", icon: ServerIcon },
] as const

export function AppSidebar(props: React.ComponentProps<typeof Sidebar>) {
  const pathname = useRouterState({ select: (state) => state.location.pathname })

  return (
    <Sidebar variant="inset" collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" tooltip="headgate" render={<Link to="/queues" />}>
              <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                <span className="text-sm font-semibold">h</span>
              </div>
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
          <SidebarMenu>
            {navigation.map((item) => (
              <SidebarMenuItem key={item.to}>
                <SidebarMenuButton
                  isActive={pathname === item.to || pathname.startsWith(`${item.to}/`)}
                  tooltip={item.label}
                  render={<Link to={item.to} />}
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
        <p className="px-2 text-xs text-sidebar-foreground/65 group-data-[collapsible=icon]:hidden">
          API {config.apiBase}
        </p>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}
