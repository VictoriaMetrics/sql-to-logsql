export interface QuickRange {
  readonly label: string;
  readonly from: string;
  readonly to: string;
}

export const predefinedQuickRanges: QuickRange[] = [
  {
    label: "Last 5 minutes",
    from: "5m ago",
    to: "now",
  },
  {
    label: "Last 10 minutes",
    from: "10m ago",
    to: "now",
  },
  {
    label: "Last 15 minutes",
    from: "15m ago",
    to: "now",
  },
  {
    label: "Last 20 minutes",
    from: "20m ago",
    to: "now",
  },
  {
    label: "Last 30 minutes",
    from: "30m ago",
    to: "now",
  },
  {
    label: "Last 1 hour",
    from: "1h ago",
    to: "now",
  },
  {
    label: "Last 2 hours",
    from: "2h ago",
    to: "now",
  },
  {
    label: "Last 3 hours",
    from: "3h ago",
    to: "now",
  },
  {
    label: "Last 6 hour",
    from: "6h ago",
    to: "now",
  },
  {
    label: "Last 12 hours",
    from: "12h ago",
    to: "now",
  },
  {
    label: "Last 24 hours",
    from: "24h ago",
    to: "now",
  },
  {
    label: "Last 2 days",
    from: "2d ago",
    to: "now",
  },
  {
    label: "Last 3 days",
    from: "3d ago",
    to: "now",
  },
  {
    label: "Last 7 days",
    from: "7d ago",
    to: "now",
  },
  {
    label: "Last 14 days",
    from: "14d ago",
    to: "now",
  },
  {
    label: "Last 30 days",
    from: "30d ago",
    to: "now",
  },
  {
    label: "Last 90 days",
    from: "90d ago",
    to: "now",
  },
  {
    label: "Last 6 months",
    from: "180d ago",
    to: "now",
  },
  {
    label: "Last 1 year",
    from: "1y ago",
    to: "now",
  },
  {
    label: "Last 2 years",
    from: "2y ago",
    to: "now",
  },
  {
    label: "Last 3 years",
    from: "3y ago",
    to: "now",
  },
  {
    label: "Last 5 years",
    from: "5y ago",
    to: "now",
  },
];
