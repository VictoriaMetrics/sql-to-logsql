package render

import (
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/ast"
)

// Render produces a canonical SQL string for the supplied AST node.
func Render(node ast.Node) (string, error) {
	r := &renderer{}
	if node == nil {
		return "", fmt.Errorf("render: nil node")
	}
	node.Accept(r)
	if len(r.errs) > 0 {
		return "", r.errs[0]
	}
	return strings.TrimSpace(r.builder.String()), nil
}

type renderer struct {
	builder strings.Builder
	errs    []error
}

func (r *renderer) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return r
	}
	switch n := node.(type) {
	case *ast.SelectStatement:
		r.renderSelect(n)
	case *ast.InsertStatement:
		r.renderInsert(n)
	case *ast.UpdateStatement:
		r.renderUpdate(n)
	case *ast.DeleteStatement:
		r.renderDelete(n)
	case *ast.CreateViewStatement:
		r.renderCreateView(n)
	case ast.Expr:
		r.renderExpr(n)
	default:
		r.errs = append(r.errs, fmt.Errorf("render: unsupported node %T", n))
	}
	return nil // prevent default traversal; we recurse manually
}

func (r *renderer) write(parts ...string) {
	for _, p := range parts {
		r.builder.WriteString(p)
	}
}

func (r *renderer) renderSelect(stmt *ast.SelectStatement) {
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		r.renderWith(stmt.With)
		r.write(" ")
	}
	r.write("SELECT ")
	if stmt.Distinct {
		r.write("DISTINCT ")
	}
	for i, item := range stmt.Columns {
		if i > 0 {
			r.write(", ")
		}
		r.renderExpr(item.Expr)
		if item.Alias != "" {
			r.write(" AS ", item.Alias)
		}
	}
	if stmt.From != nil {
		r.write(" FROM ")
		r.renderTable(stmt.From)
	}
	if stmt.Where != nil {
		r.write(" WHERE ")
		r.renderExpr(stmt.Where)
	}
	if len(stmt.GroupBy) > 0 {
		r.write(" GROUP BY ")
		for i, expr := range stmt.GroupBy {
			if i > 0 {
				r.write(", ")
			}
			r.renderExpr(expr)
		}
	}
	if stmt.Having != nil {
		r.write(" HAVING ")
		r.renderExpr(stmt.Having)
	}
	if len(stmt.OrderBy) > 0 {
		r.write(" ORDER BY ")
		for i, item := range stmt.OrderBy {
			if i > 0 {
				r.write(", ")
			}
			r.renderExpr(item.Expr)
			if item.Direction == ast.Descending {
				r.write(" DESC")
			} else {
				r.write(" ASC")
			}
		}
	}
	if stmt.Limit != nil {
		if stmt.Limit.Count != nil {
			r.write(" LIMIT ")
			r.renderExpr(stmt.Limit.Count)
		}
		if stmt.Limit.Offset != nil {
			r.write(" OFFSET ")
			r.renderExpr(stmt.Limit.Offset)
		}
	}
	for _, op := range stmt.SetOps {
		r.write(" ", string(op.Operator))
		if op.All {
			r.write(" ALL")
		}
		r.write(" ")
		r.renderSetOperand(op.Select)
	}
}

func (r *renderer) renderWith(with *ast.WithClause) {
	r.write("WITH ")
	if with.Recursive {
		r.write("RECURSIVE ")
	}
	for i, cte := range with.CTEs {
		if i > 0 {
			r.write(", ")
		}
		r.renderIdentifier(cte.Name)
		if len(cte.Columns) > 0 {
			r.write(" (")
			for j, col := range cte.Columns {
				if j > 0 {
					r.write(", ")
				}
				r.renderIdentifier(col)
			}
			r.write(")")
		}
		r.write(" AS (")
		if cte.Select == nil {
			r.errs = append(r.errs, fmt.Errorf("render: CTE %v has nil select", strings.Join(cte.Name.Parts, ".")))
		} else {
			r.renderSelect(cte.Select)
		}
		r.write(")")
	}
}

func (r *renderer) renderSetOperand(stmt *ast.SelectStatement) {
	if stmt == nil {
		r.errs = append(r.errs, fmt.Errorf("render: nil set operand"))
		return
	}
	needsParens := (stmt.With != nil && len(stmt.With.CTEs) > 0) || len(stmt.SetOps) > 0
	if needsParens {
		r.write("(")
	}
	r.renderSelect(stmt)
	if needsParens {
		r.write(")")
	}
}

func (r *renderer) renderInsert(stmt *ast.InsertStatement) {
	r.write("INSERT INTO ")
	r.renderIdentifier(stmt.Table.Name)
	if len(stmt.Columns) > 0 {
		r.write(" (")
		for i, col := range stmt.Columns {
			if i > 0 {
				r.write(", ")
			}
			r.renderIdentifier(col)
		}
		r.write(")")
	}
	if len(stmt.Rows) > 0 {
		r.write(" VALUES ")
		for i, row := range stmt.Rows {
			if i > 0 {
				r.write(", ")
			}
			r.write("(")
			for j, expr := range row {
				if j > 0 {
					r.write(", ")
				}
				r.renderExpr(expr)
			}
			r.write(")")
		}
	} else if stmt.Select != nil {
		r.write(" ")
		r.renderSelect(stmt.Select)
	}
}

func (r *renderer) renderUpdate(stmt *ast.UpdateStatement) {
	r.write("UPDATE ")
	r.renderTable(stmt.Table)
	if len(stmt.Assignments) > 0 {
		r.write(" SET ")
		for i, asg := range stmt.Assignments {
			if i > 0 {
				r.write(", ")
			}
			r.renderIdentifier(asg.Column)
			r.write(" = ")
			r.renderExpr(asg.Value)
		}
	}
	if stmt.Where != nil {
		r.write(" WHERE ")
		r.renderExpr(stmt.Where)
	}
}

func (r *renderer) renderDelete(stmt *ast.DeleteStatement) {
	r.write("DELETE FROM ")
	r.renderTable(stmt.Table)
	if stmt.Where != nil {
		r.write(" WHERE ")
		r.renderExpr(stmt.Where)
	}
}

func (r *renderer) renderCreateView(stmt *ast.CreateViewStatement) {
	r.write("CREATE ")
	if stmt.OrReplace {
		r.write("OR REPLACE ")
	}
	if stmt.Materialized {
		r.write("MATERIALIZED ")
	}
	r.write("VIEW ")
	if stmt.IfNotExists {
		r.write("IF NOT EXISTS ")
	}
	r.renderIdentifier(stmt.Name)
	if len(stmt.Columns) > 0 {
		r.write(" (")
		for i, col := range stmt.Columns {
			if i > 0 {
				r.write(", ")
			}
			r.renderIdentifier(col)
		}
		r.write(")")
	}
	r.write(" AS ")
	if stmt.Select == nil {
		r.errs = append(r.errs, fmt.Errorf("render: CREATE VIEW missing select"))
		return
	}
	r.renderSelect(stmt.Select)
}

func (r *renderer) renderTable(tbl ast.TableExpr) {
	switch t := tbl.(type) {
	case *ast.TableName:
		r.renderIdentifier(t.Name)
		if t.Alias != "" {
			r.write(" AS ", t.Alias)
		}
	case *ast.SubqueryTable:
		r.write("(")
		r.renderSelect(t.Select)
		r.write(")")
		if t.Alias != "" {
			r.write(" AS ", t.Alias)
		}
	case *ast.JoinExpr:
		r.renderTable(t.Left)
		r.write(" ")
		r.renderJoinType(t.Type)
		r.write(" ")
		r.renderTable(t.Right)
		if t.Condition.On != nil && t.Type != ast.JoinCross {
			r.write(" ON ")
			r.renderExpr(t.Condition.On)
		}
	default:
		r.errs = append(r.errs, fmt.Errorf("render: unsupported table expression %T", t))
	}
}

func (r *renderer) renderJoinType(j ast.JoinType) {
	switch j {
	case ast.JoinInner:
		r.write("INNER JOIN")
	case ast.JoinLeft:
		r.write("LEFT JOIN")
	case ast.JoinRight:
		r.write("RIGHT JOIN")
	case ast.JoinFull:
		r.write("FULL JOIN")
	case ast.JoinCross:
		r.write("CROSS JOIN")
	default:
		r.write("JOIN")
	}
}

func (r *renderer) renderIdentifier(id *ast.Identifier) {
	if id == nil {
		return
	}
	r.write(strings.Join(id.Parts, "."))
}

func (r *renderer) renderExpr(expr ast.Expr) {
	switch e := expr.(type) {
	case *ast.Identifier:
		r.renderIdentifier(e)
	case *ast.NumericLiteral:
		r.write(e.Value)
	case *ast.StringLiteral:
		r.write("'", strings.ReplaceAll(e.Value, "'", "''"), "'")
	case *ast.BooleanLiteral:
		if e.Value {
			r.write("TRUE")
		} else {
			r.write("FALSE")
		}
	case *ast.NullLiteral:
		r.write("NULL")
	case *ast.Placeholder:
		r.write(e.Symbol)
	case *ast.BinaryExpr:
		r.write("(")
		r.renderExpr(e.Left)
		r.write(" ", e.Operator, " ")
		r.renderExpr(e.Right)
		r.write(")")
	case *ast.UnaryExpr:
		op := e.Operator
		if op == "NOT" {
			r.write("NOT ")
		} else {
			r.write(op)
		}
		r.renderExpr(e.Expr)
	case *ast.FuncCall:
		r.renderIdentifier(&e.Name)
		r.write("(")
		if e.Distinct {
			r.write("DISTINCT ")
		}
		for i, arg := range e.Args {
			if i > 0 {
				r.write(", ")
			}
			r.renderExpr(arg)
		}
		r.write(")")
	case *ast.StarExpr:
		if e.Table != nil {
			r.renderIdentifier(e.Table)
			r.write(".*")
		} else {
			r.write("*")
		}
	case *ast.InExpr:
		r.renderExpr(e.Expr)
		if e.Not {
			r.write(" NOT")
		}
		r.write(" IN (")
		if e.Subquery != nil {
			r.renderSelect(e.Subquery)
		} else {
			for i, v := range e.List {
				if i > 0 {
					r.write(", ")
				}
				r.renderExpr(v)
			}
		}
		r.write(")")
	case *ast.BetweenExpr:
		r.renderExpr(e.Expr)
		if e.Not {
			r.write(" NOT")
		}
		r.write(" BETWEEN ")
		r.renderExpr(e.Lower)
		r.write(" AND ")
		r.renderExpr(e.Upper)
	case *ast.LikeExpr:
		r.renderExpr(e.Expr)
		if e.Not {
			r.write(" NOT")
		}
		r.write(" LIKE ")
		r.renderExpr(e.Pattern)
	case *ast.IsNullExpr:
		r.renderExpr(e.Expr)
		r.write(" IS")
		if e.Not {
			r.write(" NOT")
		}
		r.write(" NULL")
	case *ast.ExistsExpr:
		if e.Not {
			r.write("NOT ")
		}
		r.write("EXISTS (")
		r.renderSelect(e.Subquery)
		r.write(")")
	case *ast.CaseExpr:
		r.write("CASE ")
		if e.Operand != nil {
			r.renderExpr(e.Operand)
			r.write(" ")
		}
		for _, clause := range e.When {
			r.write("WHEN ")
			r.renderExpr(clause.Condition)
			r.write(" THEN ")
			r.renderExpr(clause.Result)
			r.write(" ")
		}
		if e.Else != nil {
			r.write("ELSE ")
			r.renderExpr(e.Else)
			r.write(" ")
		}
		r.write("END")
	case *ast.SubqueryExpr:
		r.write("(")
		r.renderSelect(e.Select)
		r.write(")")
	default:
		r.errs = append(r.errs, fmt.Errorf("render: unsupported expression %T", e))
	}
}
