import { SQLEditor } from "@/components/sql-editor";
import { LogsEndpoint } from "@/components/logs-endpoint";
import { useCallback, useEffect, useState } from "react";
import { QueryResults } from "@/components/query-results";
import { toast } from "sonner";
import { Docs } from "@/components/docs";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";

const formatExecutionTime = (ms: number): string => {
  if (!Number.isFinite(ms) || ms < 0) {
    return "";
  }
  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  }
  const seconds = ms / 1000;
  const precision = seconds >= 10 ? 1 : 2;
  return `${seconds.toFixed(precision)} s`;
};

export function Main() {
  const [endpointEnabled, setEndpointEnabled] = useState<boolean>(true);
  const [endpointUrl, setEndpointUrl] = useState<string>(
    "https://play-vmlogs.victoriametrics.com",
  );
  const [bearerToken, setBearerToken] = useState<string>("");
  const [results, setResults] = useState<unknown>();
  const [query, setQuery] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [success, setSuccess] = useState<string>("");
  const [limit, setLimit] = useState<number>(0);
  const [execMode, setExecMode] = useState<"translate" | "query">("query");

  useEffect(() => {
    setLoading(true);
    fetch(`/api/v1/config`)
      .then((resp) => resp.json())
      .then((data) => {
        if (data.endpoint) {
          setEndpointUrl(data.endpoint);
          setBearerToken("secret");
          setEndpointEnabled(false);
        }
        setLimit(data.limit || 0);
        setLoading(false);
      });
  }, []);

  const handleExecute = useCallback(
    async (sql: string, start?: number, end?: number) => {
      setLoading(true);
      setError("");
      setSuccess("");

      const reqBody: {
        sql: string;
        endpoint?: string;
        bearerToken?: string;
        start?: string;
        end?: string;
        execMode?: "translate" | "query";
      } = {
        sql,
        start: start ? `${start}` : undefined,
        end: end ? `${end}` : undefined,
        execMode: execMode,
      };
      if (endpointEnabled) {
        reqBody.endpoint = endpointUrl;
        reqBody.bearerToken = bearerToken;
      }

      const execStart = performance.now();
      const resp = await fetch(`/api/v1/sql-to-logsql`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${bearerToken}`,
        },
        body: JSON.stringify(reqBody),
      });
      const body = await resp.json();
      if (resp.status !== 200) {
        setError(body.error);
        setResults(undefined);
        setQuery("");
        setLoading(false);
        toast.error("execute error:", {
          description: body.error,
          duration: 10000,
        });
        return;
      }
      setQuery(body.logsql);
      setResults(body.data);
      setLoading(false);
      const durationMs = performance.now() - execStart;
      const executionTimeMessage = formatExecutionTime(durationMs);
      setSuccess(
        executionTimeMessage
          ? `successful execution in ${executionTimeMessage}`
          : "successful execution",
      );
    },
    [bearerToken, endpointUrl, endpointEnabled, execMode],
  );

  return (
    <main className={"p-4 w-full min-h-screen flex flex-col gap-3"}>
      <ResizablePanelGroup direction="vertical" className="flex-1 min-h-0">
        <ResizablePanel defaultSize={50} minSize={30}>
          <ResizablePanelGroup
            direction="horizontal"
            className="h-full min-h-0"
          >
            <ResizablePanel
              defaultSize={60}
              minSize={45}
              className="flex flex-col min-h-0"
            >
              <div className="flex h-full w-full flex-col gap-3 min-h-0 min-w-[20rem]">
                <div className="shrink-0">
                  <LogsEndpoint
                    endpointUrl={endpointUrl}
                    onUrlChange={setEndpointUrl}
                    bearerToken={bearerToken}
                    onTokenChange={setBearerToken}
                    execMode={execMode}
                    onExecModeChange={setExecMode}
                    isLoading={loading}
                    endpointEnabled={endpointEnabled}
                  />
                </div>
                <SQLEditor
                  onRun={handleExecute}
                  isLoading={loading}
                  error={error}
                  success={success}
                  limit={limit}
                  className="flex-1 min-h-0"
                />
              </div>
            </ResizablePanel>
            <ResizableHandle withHandle className={"m-2"} />
            <ResizablePanel defaultSize={40} minSize={20} className="flex">
              <Docs />
            </ResizablePanel>
          </ResizablePanelGroup>
        </ResizablePanel>
        <ResizableHandle withHandle className={"my-2"} />
        <ResizablePanel defaultSize={50} minSize={10} className="min-h-0">
          <div className="h-full overflow-auto">
            <QueryResults
              query={query}
              data={results}
              isLoading={loading}
              execMode={execMode}
            />
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
    </main>
  );
}
