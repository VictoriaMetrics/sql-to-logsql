import Editor from "@monaco-editor/react";
import {
  Card,
  CardAction,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card.tsx";
import {useCallback, useEffect, useState} from "react";
import { Button } from "@/components/ui/button.tsx";
import {Select, SelectContent, SelectItem, SelectTrigger} from "@/components/ui/select.tsx";
import {SelectValue} from "@radix-ui/react-select";
import {DEFAULT_EXAMPLE_ID, EXAMPLES} from "@/components/sql-editor/examples.ts";
import {COMPLETIONS} from "@/components/sql-editor/complections.ts";
import {CircleXIcon, CircleCheckBigIcon, PlayIcon, ListFilterIcon} from "lucide-react"
import {Spinner} from "@/components/ui/spinner.tsx";
import {Badge} from "@/components/ui/badge.tsx";
import {cn} from "@/lib/utils";

export interface SqlEditorProps {
  readonly onRun?: (sql: string) => void;
  readonly isLoading?: boolean;
  readonly error?: string;
  readonly success?: string;
  readonly limit?: number;
  readonly className?: string;
}

export function SQLEditor({
  onRun,
  isLoading,
  error,
  success,
  limit,
  className,
}: SqlEditorProps) {
  const [value, setValue] = useState<string>(DEFAULT_EXAMPLE_ID);
  const [sql, setSql] = useState("");

  const runQuery = useCallback((text?: string) => {
    if (!onRun || isLoading) {
      return;
    }
    const current = typeof text === "string" ? text : sql;
    onRun(current);
  }, [onRun, isLoading, sql]);

  useEffect(() => {
    const example = EXAMPLES.find((example) => example.id === value);
    if (example) {
        setSql(example.sql ?? "");
    }
  }, [value]);

  return (
    <Card className={cn("w-full h-full", className)}>
      <CardHeader className={"max-sm:flex max-sm:flex-col max-sm:gap-4 max-sm:px-4"}>
        <CardTitle className={"sm:py-3"}>SQL</CardTitle>
        <CardAction className={"flex max-sm:flex-col gap-2 w-full"}>
          <Select onValueChange={setValue} value={value} disabled={isLoading}>
            <SelectTrigger className={"cursor-pointer max-sm:w-full"}>
              <SelectValue placeholder="Select example" />
            </SelectTrigger>
            <SelectContent>
              {EXAMPLES.map((example) => (
                <SelectItem value={example.id} key={example.id} className={"cursor-pointer"}>
                    Example: {example.title}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            disabled={isLoading}
            className={"cursor-pointer max-sm:w-full"}
            onClick={() => runQuery()}
          >
            {isLoading ? <Spinner /> : <PlayIcon />}
            Execute
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="flex-1 min-h-0">
        <Editor
          className={cn(
            "h-full",
            isLoading ? "pointer-events-none opacity-50 select-none grayscale-50" : ""
          )}
          height="100%"
          defaultLanguage="sql"
          theme="vs-light"
          value={sql}
          options={{
            readOnly: isLoading,
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: "off",
            scrollBeyondLastLine: false,
            selectionHighlight: !isLoading,
          }}
          onChange={(next) => setSql(next ?? "")}
          onMount={(editorInstance, monaco) => {
            monaco.languages.registerCompletionItemProvider("sql", {
              provideCompletionItems: () => {
                const suggestions = COMPLETIONS.map((label) => ({
                  label,
                  kind: monaco.languages.CompletionItemKind.Keyword,
                  insertText: label,
                  range: {
                    startLineNumber: 1,
                    endLineNumber: 1,
                    startColumn: 1,
                    endColumn: 1,
                  },
                  detail: "SQL keyword",
                  documentation: "SQL keyword",
                  sortText: label,
                  filterText: label,
                }));
                return { suggestions };
              },
            });

            const executeFromEditor = () => runQuery(editorInstance.getValue() ?? "");
            const keybindings = [
              monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
              monaco.KeyMod.Shift | monaco.KeyCode.Enter,
              monaco.KeyMod.WinCtrl | monaco.KeyCode.Enter,
              monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter,
            ];
            keybindings.forEach((binding) => {
              editorInstance.addCommand(binding, executeFromEditor);
            });
          }}
        />
      </CardContent>
      {error && (
        <CardFooter className={"flex gap-1"}>
          <CircleXIcon color={"red"} />
          <span className={"text-destructive"}>{error}</span>
        </CardFooter>
      )}
      {!error && success && (
          <CardFooter className={"flex gap-1"}>
            <CircleCheckBigIcon color={"green"} />
            <span className={"text-green-700"}>{success}</span>
          </CardFooter>
      )}
      {!error && !success && limit && limit > 0 && (
        <CardFooter className={"flex gap-1"}>
          <ListFilterIcon />
          <span>
            Any query will be limited to <Badge variant={"secondary"} className={"font-semibold"}>{limit}</Badge> rows.
          </span>
        </CardFooter>
      )}
    </Card>
  );
}
