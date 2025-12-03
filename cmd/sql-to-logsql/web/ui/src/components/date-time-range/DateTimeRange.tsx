import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover.tsx";
import { Button } from "@/components/ui/button.tsx";
import { CalendarIcon } from "lucide-react";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command.tsx";
import { DatePicker } from "@/components/date-picker";
import { useState } from "react";
import { getCaption } from "@/components/date-time-range/utils.ts";
import { predefinedQuickRanges } from "@/components/date-time-range/quick-ranges.ts";

export interface DateTimeRangeValue {
  readonly from: string;
  readonly to: string;
}

export interface DateRangeProps {
  readonly value?: DateTimeRangeValue;
  readonly onValueChange?: (value: DateTimeRangeValue) => void;
}

export function DateTimeRange({ value, onValueChange }: DateRangeProps) {
  const { from, to } = value ?? { from: "", to: "" };
  const [intFrom, setIntFrom] = useState(from);
  const [intTo, setIntTo] = useState(to);
  const [open, setOpen] = useState(false);
  const onChange = (from: string, to: string) => {
    setIntFrom(from);
    setIntTo(to);
    if (onValueChange) {
      onValueChange({ from, to });
    }
    setOpen(false);
  };
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild className={"cursor-pointer"}>
        <Button
          variant="outline"
          data-empty={!value?.from && !value?.to}
          className="data-[empty=true]:text-muted-foreground min-w-[200px] justify-start text-left font-normal"
        >
          <CalendarIcon /> {getCaption(from, to)}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-1 flex gap-2 h-80" align={"start"}>
        <Command className={"w-[180px] h-full"}>
          <CommandInput placeholder="Search quick range..." />
          <CommandList
            className={
              "scrollbar-thin scrollbar-track-transparent scrollbar-thumb-muted-foreground/50 hover:scrollbar-thumb-muted-foreground/75"
            }
          >
            <CommandEmpty>No date ranges found.</CommandEmpty>
            <CommandGroup>
              {predefinedQuickRanges.map((range) => (
                <CommandItem
                  key={range.label}
                  onSelect={() => onChange(range.from, range.to)}
                >
                  {range.label}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
        <div
          className={
            "flex flex-col justify-between p-4 border-l-1 border-muted h-full"
          }
        >
          <div className={"flex flex-col gap-4"}>
            <h2>Time range</h2>
            <DatePicker
              label={"From"}
              value={intFrom}
              onValueChange={setIntFrom}
            />
            <DatePicker label={"To"} value={intTo} onValueChange={setIntTo} />
          </div>
          <Button
            className={"cursor-pointer"}
            onClick={() => onChange(intFrom, intTo)}
          >
            Apply time range
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
}
