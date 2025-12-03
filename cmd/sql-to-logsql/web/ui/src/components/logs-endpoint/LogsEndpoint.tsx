import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card.tsx";
import { Input } from "@/components/ui/input.tsx";
import { Label } from "@/components/ui/label.tsx";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group.tsx";

export interface LogsEndpointProps {
  readonly endpointUrl?: string;
  readonly bearerToken?: string;
  readonly execMode?: "translate" | "query";
  readonly onUrlChange?: (url: string) => void;
  readonly onTokenChange?: (password: string) => void;
  readonly onExecModeChange?: (mode: "translate" | "query") => void;
  readonly isLoading?: boolean;
  readonly endpointEnabled?: boolean;
  readonly onEndpointEnabledChange?: (enabled: boolean) => void;
}

export function LogsEndpoint({
  endpointUrl,
  onUrlChange,
  bearerToken,
  onTokenChange,
  execMode = "query",
  onExecModeChange,
  isLoading,
  endpointEnabled,
}: LogsEndpointProps) {
  return (
    <Card className={"w-full py-4"}>
      <CardHeader>
        <CardTitle>VictoriaLogs endpoint</CardTitle>
        <CardDescription>
          You can query data from VictoriaLogs instance or just translate SQL to
          LogsQL without querying
        </CardDescription>
        <CardAction className={"flex flex-row gap-2"}>
          <ToggleGroup
            type="single"
            value={execMode}
            onValueChange={(value) => {
              if (
                onExecModeChange &&
                (value === "translate" || value === "query")
              ) {
                onExecModeChange(value);
              }
            }}
            variant="outline"
          >
            <ToggleGroupItem value="translate" className={"cursor-pointer"}>
              Translate
            </ToggleGroupItem>
            <ToggleGroupItem value="query" className={"cursor-pointer"}>
              Query
            </ToggleGroupItem>
          </ToggleGroup>
        </CardAction>
      </CardHeader>
      {execMode !== "translate" && (
        <CardContent className={"flex max-sm:flex-col gap-2"}>
          <div className={"flex flex-col gap-1 sm:w-3/4"}>
            <Label htmlFor={endpointUrl}>URL:</Label>
            <Input
              disabled={isLoading || !endpointEnabled}
              id={"endpointUrl"}
              value={endpointUrl}
              type={"url"}
              placeholder={"https://play-vmlogs.victoriametrics.com"}
              onChange={(e) => onUrlChange && onUrlChange(e.target.value)}
            />
          </div>
          <div className={"flex flex-col gap-1 sm:w-1/4"}>
            <Label htmlFor={"bearerToken"}>Bearer token:</Label>
            <Input
              disabled={isLoading || !endpointEnabled}
              id={"bearerToken"}
              value={bearerToken}
              type={"password"}
              onChange={(e) => onTokenChange && onTokenChange(e.target.value)}
            />
          </div>
        </CardContent>
      )}
    </Card>
  );
}
