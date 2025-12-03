import { Label } from "@/components/ui/label.tsx";
import { Input } from "@/components/ui/input.tsx";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover.tsx";
import { Button } from "@/components/ui/button.tsx";
import { CalendarIcon } from "lucide-react";
import { Calendar } from "@/components/ui/calendar.tsx";
import { useId, useState } from "react";
import { parseDate } from "chrono-node";
import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip.tsx";
import dayjs from "dayjs";

export interface DatePickerProps {
  readonly label?: string;
  readonly value?: string;
  readonly onValueChange?: (value: string) => void;
  readonly disabled?: boolean;
  readonly tooltip?: string;
  readonly error?: string;
  readonly id?: string;
}

export function DatePicker({
  label,
  value,
  onValueChange,
  disabled,
  tooltip,
  error: extError,
  id: extId,
}: DatePickerProps) {
  const [open, setOpen] = useState(false);
  const date = (value && parseDate(value)) || undefined;
  const [month, setMonth] = useState<Date | undefined>(date);
  const intError = value && (date ? undefined : "invalid date format");
  const error = intError || extError;
  const formattedDate = date && formatDate(date);
  const intId = useId();
  const id = extId || intId;

  return (
    <div className="flex flex-col gap-1">
      <Label
        htmlFor={id}
        className={cn("px-1 pb-1", { ["text-muted-foreground"]: disabled })}
        aria-disabled={disabled}
      >
        {label}
      </Label>
      <div className="relative flex gap-2">
        <Tooltip>
          <TooltipTrigger asChild>
            <Input
              autoComplete={"off"}
              disabled={disabled}
              aria-disabled={disabled}
              id={id}
              value={value}
              placeholder={formatDate(new Date())}
              className={cn("bg-background pr-10", {
                ["border-destructive"]: error && !disabled,
                ["text-destructive"]: error && !disabled,
              })}
              onChange={(e) => onValueChange && onValueChange(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "ArrowDown") {
                  e.preventDefault();
                  setOpen(true);
                }
              }}
            />
          </TooltipTrigger>
          {tooltip && (
            <TooltipContent className="relative flex gap-2" side={"right"}>
              {tooltip}
            </TooltipContent>
          )}
        </Tooltip>
        <Popover open={open} onOpenChange={setOpen}>
          <PopoverTrigger asChild disabled={disabled} aria-disabled={disabled}>
            <Button
              disabled={disabled}
              aria-disabled={disabled}
              variant="ghost"
              className="cursor-pointer absolute top-1/2 right-2 size-6 -translate-y-1/2"
            >
              <CalendarIcon
                className={cn("size-3.5", {
                  ["text-muted-foreground"]: disabled,
                })}
              />
              <span className="sr-only">Select date</span>
            </Button>
          </PopoverTrigger>
          <PopoverContent
            className="w-auto overflow-hidden p-0"
            align="end"
            alignOffset={-8}
            sideOffset={10}
          >
            <Calendar
              mode="single"
              selected={date}
              captionLayout="dropdown"
              month={month}
              onMonthChange={setMonth}
              onSelect={(date) => {
                if (onValueChange) {
                  onValueChange(formatDate(date));
                }
                setOpen(false);
              }}
            />
          </PopoverContent>
        </Popover>
      </div>
      <div
        aria-disabled={disabled}
        className={cn("text-xs px-1", {
          ["text-destructive"]: error && !disabled,
          ["text-muted-foreground"]: !error,
          ["invisible"]: !error && formattedDate && formattedDate === value,
          ["text-muted"]: disabled,
        })}
      >
        {error ||
          (formattedDate && formattedDate === value
            ? formattedDate
            : formattedDate || "select a date")}
      </div>
    </div>
  );
}

function formatDate(date: Date | undefined) {
  if (!date) {
    return "";
  }
  return dayjs(date).format("YYYY-MM-DD HH:mm:ss");
}
