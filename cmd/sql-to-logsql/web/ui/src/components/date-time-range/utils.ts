import { predefinedQuickRanges } from "@/components/date-time-range/quick-ranges.ts";

export function getCaption(from: string, to: string) {
  if (from === "" && to === "") {
    return "Select a date range";
  }
  if (from === "" && to !== "") {
    return `To ${to}`;
  }
  if (from !== "" && to === "") {
    return `From ${from}`;
  }
  const qr = predefinedQuickRanges.find(
    (range) => range.from === from && range.to === to,
  );
  if (qr) {
    return qr.label;
  }
  return `${from} - ${to}`;
}
