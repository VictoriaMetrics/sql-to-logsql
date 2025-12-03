import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../ui/card";
import { QueryResultsTable } from "@/components/query-results/QueryResultsTable.tsx";
import { Skeleton } from "@/components/ui/skeleton.tsx";

export interface QueryResultsProps {
  readonly query?: string;
  readonly data?: unknown;
  readonly isLoading?: boolean;
  readonly execMode?: "translate" | "query";
}

export function QueryResults({
  query,
  data,
  isLoading,
  execMode,
}: QueryResultsProps) {
  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {[...Array(17).keys()].map((n) => (
          <div key={n} className={"flex flex-row gap-1"}>
            {[...Array(7).keys()].map((m) => (
              <Skeleton className="h-8 w-full" key={m} />
            ))}
          </div>
        ))}
      </div>
    );
  }
  if (!query && !data) return null;

  return (
    <Card className={"max-h-full py-4"}>
      <CardHeader className={"sticky top-0 z-5 bg-white"}>
        <CardTitle>
          {execMode === "query" ? "LogsQL query results" : "LogsQL query"}
        </CardTitle>
        {query && (
          <CardDescription>
            <code>{query}</code>
          </CardDescription>
        )}
      </CardHeader>
      {!!data && (
        <CardContent className={"overflow-y-auto"}>
          <QueryResultsTable data={data} />
        </CardContent>
      )}
    </Card>
  );
}
