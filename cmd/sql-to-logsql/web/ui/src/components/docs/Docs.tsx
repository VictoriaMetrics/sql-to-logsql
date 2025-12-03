import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card.tsx";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion.tsx";
import { InfoIcon, LinkIcon } from "lucide-react";
import { Button } from "@/components/ui/button.tsx";
import { Separator } from "@/components/ui/separator.tsx";
import { Badge } from "@/components/ui/badge.tsx";

export function Docs() {
  return (
    <Card className={"w-full max-h-full min-w-[20rem] overflow-y-scroll py-4"}>
      <CardHeader>
        <CardTitle className={"flex flex-row gap-2 items-center"}>
          <span>Information about SQL to LogsQL</span>
          <Badge variant={"outline"}>
            {__APP_VERSION__ == "${VERSION}" ? "local" : __APP_VERSION__}
          </Badge>
        </CardTitle>
        <CardDescription>
          Service that helps to query VictoriaLogs with SQL
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className={"flex gap-2 items-center"}>
          <a
            href={"https://github.com/VictoriaMetrics/sql-to-logsql"}
            target={"_blank"}
          >
            <Button variant={"link"} className={"cursor-pointer"}>
              <LinkIcon />
              Source code and documentation
            </Button>
          </a>
        </div>
        <Separator className={"mt-2 ml-3"} />
        <Accordion type="single" collapsible className="w-full pl-3">
          <AccordionItem value="statement-types">
            <AccordionTrigger className={"cursor-pointer"}>
              <span className={"flex flex-row gap-2 items-center"}>
                <InfoIcon size={16} />
                <span>Supported statement types</span>
              </span>
            </AccordionTrigger>
            <AccordionContent className="flex flex-col gap-4 text-balance">
              <p>
                <ul className={"list-disc pl-4 pt-2"}>
                  <li>
                    <code>SHOW TABLES / VIEWS</code>
                  </li>
                  <li>
                    <code>DESCRIBE TABLE / VIEW ...</code>
                  </li>
                  <li>
                    <code>SELECT ... FROM ...</code>
                  </li>
                  <li>
                    <code>CREATE VIEW ...</code>
                  </li>
                  <li>
                    <code>DROP VIEW ...</code>
                  </li>
                </ul>
              </p>
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="clauses">
            <AccordionTrigger className={"cursor-pointer"}>
              <span className={"flex flex-row gap-2 items-center"}>
                <InfoIcon size={16} />
                <span>Supported query clauses</span>
              </span>
            </AccordionTrigger>
            <AccordionContent className="flex flex-col gap-4 text-balance">
              <p>
                <ul className={"list-disc pl-4 pt-2"}>
                  <li>
                    <code>SELECT, DISTINCT, AS, OVER, PARTITION BY</code>
                  </li>
                  <li>
                    <code>FROM, WITH, subqueries</code>
                  </li>
                  <li>
                    <code>WHERE, AND, OR</code>
                  </li>
                  <li>
                    <code>LEFT JOIN / JOIN / INNER JOIN</code>
                  </li>
                  <li>
                    <code>
                      LIKE, NOT LIKE, BETWEEN, IN, NOT IN, IS NULL, IS NOT NULL
                    </code>
                  </li>
                  <li>
                    <code>GROUP BY, HAVING</code>
                  </li>
                  <li>
                    <code>ORDER BY, ASC, DESC, LIMIT, OFFSET</code>
                  </li>
                  <li>
                    <code>UNION ALL</code>
                  </li>
                </ul>
              </p>
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="functions">
            <AccordionTrigger className={"cursor-pointer"}>
              <span className={"flex flex-row gap-2 items-center"}>
                <InfoIcon size={16} />
                <span>Supported functions and operators</span>
              </span>
            </AccordionTrigger>
            <AccordionContent className="flex flex-col gap-4 text-balance">
              <p>
                <ul className={"list-disc pl-4 pt-2"}>
                  <li>
                    <code>
                      SUBSTR, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, REPLACE,
                      REGEXP_REPLACE
                    </code>
                  </li>
                  <li>
                    <code>
                      CASE/WHEN, LIKE, NOT LIKE, =, !=, &lt;, &gt;, &lt;=,
                      &gt;=, BETWEEN
                    </code>
                  </li>
                  <li>
                    <code>+,-, *, /, %, ^</code>
                  </li>
                  <li>
                    <code>
                      ABS, GREATEST, LEAST, ROUND, FLOOR, CEIL, POW, LN, EXP
                    </code>
                  </li>
                  <li>
                    <code>SUM, COUNT, MAX, MIN, AVG</code>
                  </li>
                  <li>
                    <code>CURRENT_TIMESTAMP, CURREN_DATE</code>
                  </li>
                  <li>
                    <code>JSON_VALUE</code>
                  </li>
                </ul>
              </p>
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="virtual-tables">
            <AccordionTrigger className={"cursor-pointer"}>
              <span className={"flex flex-row gap-2 items-center"}>
                <InfoIcon size={16} />
                <span>Supported data sources</span>
              </span>
            </AccordionTrigger>
            <AccordionContent className="flex flex-col gap-4 text-balance">
              <p>
                <ul className={"list-disc pl-4 pt-2"}>
                  <li>
                    Only{" "}
                    <b>
                      <code>logs</code>
                    </b>{" "}
                    table is supported
                  </li>
                  <li>You can create any views</li>
                </ul>
              </p>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </CardContent>
    </Card>
  );
}
