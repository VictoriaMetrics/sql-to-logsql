package logsql

import (
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/render"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store"
)

type translationContext struct {
	sp   *store.Provider
	ctes map[string]string
}

func TranslateSelectStatementToLogsQL(stmt ast.Statement, sp *store.Provider) (string, error) {
	return translateSelectStatementToLogsQLWithContext(stmt, translationContext{sp: sp})
}

func translateSelectStatementToLogsQLWithContext(stmt ast.Statement, ctx translationContext) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("translator: nil statement")
	}

	t := &selectTranslatorVisitor{sp: ctx.sp}
	if len(ctx.ctes) > 0 {
		t.availableCTEs = maps.Clone(ctx.ctes)
	}
	stmt.Accept(t)
	if t.err != nil {
		return "", t.err
	}
	return t.result, nil
}

var (
	safeBareLiteral        = regexp.MustCompile(`^[A-Za-z0-9_.:/-]+$`)
	safeWildcardLiteral    = regexp.MustCompile(`^[A-Za-z0-9_.:/-]*\*?[A-Za-z0-9_.:/-]*$`)
	safeFormatFieldLiteral = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

type selectTranslatorVisitor struct {
	result string
	err    error

	sp *store.Provider

	bindings           map[string]*tableBinding
	baseAlias          string
	pendingLeftFilter  []ast.Expr
	aggResults         map[string]string
	groupExprAliases   map[string]string
	availableCTEs      map[string]string
	baseUsesPipeline   bool
	basePipeline       string
	baseFilter         string
	filterComputations map[string]*filterComputation
	filterOrder        []string
	filterDelete       []string
	filterDeleteSet    map[string]struct{}
}

type tableBinding struct {
	alias  string
	isBase bool
}

type tableSpec struct {
	filter   string
	pipeline string
}

type filterComputation struct {
	alias    string
	rawAlias string
	pipes    []string
}

func newTableSpec(expr string) tableSpec {
	value := strings.TrimSpace(expr)
	if value == "" || value == "*" {
		return tableSpec{filter: "*"}
	}
	if strings.Contains(value, "|") {
		return tableSpec{pipeline: value}
	}
	return tableSpec{filter: value}
}

func (v *selectTranslatorVisitor) lookupTableSpec(nameLower string) (tableSpec, bool) {
	expr, ok := v.sp.TableStore().GetTableQuery(nameLower)
	if !ok {
		return tableSpec{}, false
	}
	return newTableSpec(expr), true
}

func (v *selectTranslatorVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil || v.err != nil {
		return v
	}

	switch n := node.(type) {
	case *ast.SelectStatement:
		v.result, v.err = v.translateSelect(n)
	default:
		v.err = &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported root node %T", n),
		}
	}

	return nil
}

func (v *selectTranslatorVisitor) translateSimpleSelect(stmt *ast.SelectStatement) (string, error) {
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		if stmt.With.Recursive {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: recursive CTEs are not supported",
			}
		}
		if v.availableCTEs == nil {
			v.availableCTEs = make(map[string]string)
		}
		for _, cte := range stmt.With.CTEs {
			if cte.Name == nil || len(cte.Name.Parts) == 0 {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: CTE missing name",
				}
			}
			if cte.Select == nil {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: CTE %s has nil select", strings.Join(cte.Name.Parts, ".")),
				}
			}
			name := strings.ToLower(cte.Name.Parts[len(cte.Name.Parts)-1])
			if _, exists := v.availableCTEs[name]; exists {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: duplicate CTE name %q", name),
				}
			}
			query, err := translateSelectStatementToLogsQLWithContext(cte.Select, translationContext{
				sp:   v.sp,
				ctes: v.availableCTEs,
			})
			if err != nil {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: failed to translate CTE %s: %s", name, err),
					Err:     err,
				}
			}
			v.availableCTEs[name] = query
		}
	}
	distinct := stmt.Distinct
	if len(stmt.SetOps) > 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: set operations are not supported",
		}
	}

	v.bindings = make(map[string]*tableBinding)
	v.pendingLeftFilter = nil
	v.aggResults = nil
	v.baseAlias = ""
	v.groupExprAliases = nil
	v.baseUsesPipeline = false
	v.basePipeline = ""
	v.baseFilter = ""
	v.filterComputations = nil
	v.filterOrder = nil
	v.filterDelete = nil
	v.filterDeleteSet = nil

	joinPipes, err := v.processFrom(stmt.From)
	if err != nil {
		return "", err
	}

	filters := make([]string, 0)
	if stmt.Where != nil {
		if err := v.ensureBaseAliasesOnly(stmt.Where); err != nil {
			return "", err
		}
		whereStr, err := v.translateExpr(stmt.Where)
		if err != nil {
			return "", err
		}
		filters = append(filters, whereStr)
	}
	for _, lf := range v.pendingLeftFilter {
		if err := v.ensureBaseAliasesOnly(lf); err != nil {
			return "", err
		}
		lfStr, err := v.translateExpr(lf)
		if err != nil {
			return "", err
		}
		filters = append(filters, lfStr)
	}
	if baseFilter := strings.TrimSpace(v.baseFilter); baseFilter != "" && baseFilter != "*" {
		filters = append([]string{baseFilter}, filters...)
	}

	filter := "*"
	if len(filters) == 1 {
		filter = filters[0]
	} else if len(filters) > 1 {
		filter = "(" + strings.Join(filters, " AND ") + ")"
	}

	preFilterPipes := v.collectFilterPrefilters()
	needsFilterPipeline := v.baseUsesPipeline || len(preFilterPipes) > 0
	base := filter
	pipes := make([]string, 0)

	if needsFilterPipeline {
		if v.baseUsesPipeline {
			base = v.basePipeline
		} else {
			base = "*"
		}
		if len(preFilterPipes) > 0 {
			pipes = append(pipes, preFilterPipes...)
		}
		if filter != "*" {
			pipes = append(pipes, "filter "+filter)
		}
		if cleanup := v.collectFilterCleanup(); len(cleanup) > 0 {
			pipes = append(pipes, cleanup...)
		}
	}
	pipes = append(pipes, joinPipes...)

	statsPipes, aggregated, err := v.buildStatsPipe(stmt)
	if err != nil {
		return "", err
	}
	if len(statsPipes) > 0 {
		pipes = append(pipes, statsPipes...)
	}

	if stmt.Having != nil {
		if !aggregated {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: HAVING requires GROUP BY with aggregates",
			}
		}
		havingStr, err := v.translateExpr(stmt.Having)
		if err != nil {
			return "", err
		}
		pipes = append(pipes, "filter "+havingStr)
	}

	projectionPipes, projectionFields, err := v.buildProjectionPipes(stmt.Columns, aggregated)
	if err != nil {
		return "", err
	}
	pipes = append(pipes, projectionPipes...)

	if distinct {
		distinctPipe, err := v.buildDistinctPipe(projectionFields, aggregated)
		if err != nil {
			return "", err
		}
		if distinctPipe != "" {
			pipes = append(pipes, distinctPipe)
		}
	}

	if len(stmt.OrderBy) > 0 {
		orderPipe, err := v.translateOrderBy(stmt.OrderBy, aggregated)
		if err != nil {
			return "", err
		}
		pipes = append(pipes, orderPipe)
	}

	if stmt.Limit != nil {
		limitPipes, err := v.translateLimit(stmt.Limit)
		if err != nil {
			return "", err
		}
		pipes = append(pipes, limitPipes...)
	}

	if len(pipes) == 0 {
		return base, nil
	}
	return base + " | " + strings.Join(pipes, " | "), nil
}

func (v *selectTranslatorVisitor) collectFilterPrefilters() []string {
	if len(v.filterOrder) == 0 {
		return nil
	}
	pipes := make([]string, 0)
	for _, key := range v.filterOrder {
		if comp, ok := v.filterComputations[key]; ok {
			pipes = append(pipes, comp.pipes...)
		}
	}
	return pipes
}

func (v *selectTranslatorVisitor) collectFilterCleanup() []string {
	if len(v.filterDelete) == 0 {
		return nil
	}
	return []string{"delete " + strings.Join(v.filterDelete, ", ")}
}

func (v *selectTranslatorVisitor) translateSelect(stmt *ast.SelectStatement) (string, error) {
	if len(stmt.SetOps) == 0 {
		return v.translateSimpleSelect(stmt)
	}

	baseCopy := *stmt
	baseCopy.SetOps = nil
	base, err := v.translateSimpleSelect(&baseCopy)
	if err != nil {
		return "", err
	}

	result := base
	for _, op := range stmt.SetOps {
		if op.Operator != ast.SetOpUnion {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: set operator %s is not supported", op.Operator),
			}
		}
		if !op.All {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: UNION without ALL is not supported",
			}
		}
		if op.Select == nil {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: UNION missing right-hand select",
			}
		}
		rhs, err := translateSelectStatementToLogsQLWithContext(op.Select, translationContext{
			sp:   v.sp,
			ctes: v.availableCTEs,
		})
		if err != nil {
			return "", err
		}
		result = result + " | union (" + rhs + ")"
	}

	return result, nil
}

func (v *selectTranslatorVisitor) buildDistinctPipe(fields []string, aggregated bool) (string, error) {
	if aggregated {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DISTINCT with aggregates is not supported",
		}
	}
	if len(fields) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DISTINCT requires explicit column list",
		}
	}
	return "uniq by (" + strings.Join(fields, ", ") + ")", nil
}

func (v *selectTranslatorVisitor) processFrom(from ast.TableExpr) ([]string, error) {
	if from == nil {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: FROM clause is required",
		}
	}

	switch t := from.(type) {
	case *ast.TableName:
		if err := v.registerBaseTable(t); err != nil {
			return nil, err
		}
		return nil, nil
	case *ast.JoinExpr:
		return v.processJoin(t)
	default:
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported FROM clause %T", t),
		}
	}
}

func (v *selectTranslatorVisitor) registerBaseTable(table *ast.TableName) error {
	if table == nil || table.Name == nil || len(table.Name.Parts) == 0 {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid table reference",
		}
	}
	name := table.Name.Parts[len(table.Name.Parts)-1]
	nameLower := strings.ToLower(name)

	alias := strings.TrimSpace(table.Alias)
	if alias == "" {
		alias = name
	}
	aliasLower := strings.ToLower(alias)

	if v.baseAlias != "" && v.baseAlias != aliasLower {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: multiple base tables are not supported",
		}
	}

	if v.availableCTEs != nil {
		if query, ok := v.availableCTEs[nameLower]; ok {
			v.baseAlias = aliasLower
			v.baseUsesPipeline = true
			v.basePipeline = query
			v.registerBinding(aliasLower, true)
			v.registerBinding(nameLower, true)
			return nil
		}
	}

	var viewAttempted bool
	var viewDisplay string
	if v.sp.ViewStore() != nil {
		viewQuery, display, found, err := v.sp.ViewStore().Load(table.Name.Parts)
		if err != nil {
			return err
		}
		viewAttempted = true
		viewDisplay = display
		if found {
			v.baseAlias = aliasLower
			v.baseUsesPipeline = true
			v.basePipeline = viewQuery
			v.baseFilter = ""
			v.registerBinding(aliasLower, true)
			v.registerBinding(nameLower, true)
			return nil
		}
	}

	spec, ok := v.lookupTableSpec(nameLower)
	if !ok {
		if viewAttempted {
			return &TranslationError{
				Code:    http.StatusNotFound,
				Message: fmt.Sprintf("translator: view %s not found", viewDisplay),
			}
		}
		available := v.sp.TableStore().ListTables()
		return &TranslationError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("translator: table %q is not configured (available: %s)", strings.Join(table.Name.Parts, "."), strings.Join(available, ", ")),
		}
	}

	v.baseAlias = aliasLower
	v.baseFilter = spec.filter
	v.baseUsesPipeline = spec.pipeline != ""
	v.basePipeline = spec.pipeline
	v.registerBinding(aliasLower, true)
	v.registerBinding(nameLower, true)
	return nil
}

func (v *selectTranslatorVisitor) registerBinding(alias string, isBase bool) {
	key := strings.ToLower(alias)
	if key == "" {
		return
	}
	v.bindings[key] = &tableBinding{alias: key, isBase: isBase}
}

func (v *selectTranslatorVisitor) processJoin(join *ast.JoinExpr) ([]string, error) {
	if join == nil {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid JOIN clause",
		}
	}
	if join.Type != ast.JoinInner && join.Type != ast.JoinLeft {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: only INNER and LEFT JOIN are supported",
		}
	}

	leftTable, ok := join.Left.(*ast.TableName)
	if !ok {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JOIN left side must be table reference",
		}
	}
	if err := v.registerBaseTable(leftTable); err != nil {
		return nil, err
	}

	var rightAlias string
	var rightQuery string
	var rightSimple bool
	var rightBaseFilters []string

	switch rt := join.Right.(type) {
	case *ast.TableName:
		if rt == nil || rt.Name == nil || len(rt.Name.Parts) == 0 {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: invalid JOIN table",
			}
		}
		name := rt.Name.Parts[len(rt.Name.Parts)-1]
		nameLower := strings.ToLower(name)
		alias := strings.TrimSpace(rt.Alias)
		if alias == "" {
			alias = name
		}
		rightAlias = strings.ToLower(alias)
		if _, exists := v.bindings[rightAlias]; exists {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: duplicate table alias %q", alias),
			}
		}
		if v.availableCTEs != nil {
			if query, ok := v.availableCTEs[nameLower]; ok {
				rightQuery = query
				v.registerBinding(rightAlias, false)
				v.registerBinding(nameLower, false)
				rightSimple = false
				break
			}
		}
		var viewAttempted bool
		var viewDisplay string
		if v.sp.ViewStore() != nil {
			viewQuery, display, found, err := v.sp.ViewStore().Load(rt.Name.Parts)
			if err != nil {
				return nil, err
			}
			viewAttempted = true
			viewDisplay = display
			if found {
				rightQuery = viewQuery
				v.registerBinding(rightAlias, false)
				v.registerBinding(nameLower, false)
				rightSimple = false
				break
			}
		}
		spec, ok := v.lookupTableSpec(nameLower)
		if !ok {
			if viewAttempted {
				return nil, &TranslationError{
					Code:    http.StatusNotFound,
					Message: fmt.Sprintf("translator: view %s not found", viewDisplay),
				}
			}
			available := v.sp.TableStore().ListTables()
			return nil, &TranslationError{
				Code:    http.StatusNotFound,
				Message: fmt.Sprintf("translator: JOIN table %q is not configured (available: %s)", strings.Join(rt.Name.Parts, "."), strings.Join(available, ", ")),
			}
		}
		if strings.TrimSpace(rt.Alias) == "" {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: JOINed table requires alias",
			}
		}
		v.registerBinding(rightAlias, false)
		v.registerBinding(nameLower, false)
		if spec.pipeline != "" {
			rightQuery = spec.pipeline
			rightSimple = false
		} else {
			rightSimple = true
			if spec.filter != "" && spec.filter != "*" {
				rightBaseFilters = append(rightBaseFilters, spec.filter)
			}
		}
	case *ast.SubqueryTable:
		alias := strings.TrimSpace(rt.Alias)
		if alias == "" {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: JOIN subquery requires alias",
			}
		}
		rightAlias = strings.ToLower(alias)
		if _, exists := v.bindings[rightAlias]; exists {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: duplicate table alias %q", alias),
			}
		}
		v.registerBinding(rightAlias, false)
		subQuery, err := translateSelectStatementToLogsQLWithContext(rt.Select, translationContext{
			sp:   v.sp,
			ctes: v.availableCTEs,
		})
		if err != nil {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: failed to translate JOIN subquery: %s", err),
				Err:     err,
			}
		}
		rightQuery = subQuery
	default:
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported JOIN right side %T", rt),
		}
	}

	joinKeys, leftFilters, rightFilters, err := v.extractJoinSpec(join.Condition, rightAlias)
	if err != nil {
		return nil, err
	}
	if len(joinKeys) == 0 {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JOIN requires equality condition between tables",
		}
	}

	v.pendingLeftFilter = append(v.pendingLeftFilter, leftFilters...)

	parts := make([]string, 0, len(rightBaseFilters)+len(rightFilters))
	parts = append(parts, rightBaseFilters...)
	for _, expr := range rightFilters {
		if err := v.ensureAliases(expr, map[string]struct{}{rightAlias: {}}); err != nil {
			return nil, err
		}
		part, err := v.translateExpr(expr)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	combined := "*"
	if len(parts) == 1 {
		combined = parts[0]
	} else if len(parts) > 1 {
		combined = "(" + strings.Join(parts, " AND ") + ")"
	}
	if rightSimple {
		rightQuery = combined
	} else if combined != "*" {
		rightQuery = rightQuery + " | filter " + combined
	}
	if rightSimple && combined == "*" {
		rightQuery = "*"
	}

	suffix := ""
	if join.Type == ast.JoinInner {
		suffix = " inner"
	}

	joinClause := fmt.Sprintf("join by (%s) (%s)%s", strings.Join(joinKeys, ", "), rightQuery, suffix)
	return []string{joinClause}, nil
}

func (v *selectTranslatorVisitor) extractJoinSpec(cond ast.JoinCondition, rightAlias string) ([]string, []ast.Expr, []ast.Expr, error) {
	if cond.On == nil {
		return nil, nil, nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: JOIN must include ON clause",
		}
	}

	conjuncts := flattenAnd(cond.On)
	joinKeys := make([]string, 0)
	leftFilters := make([]ast.Expr, 0)
	rightFilters := make([]ast.Expr, 0)

	for _, expr := range conjuncts {
		bin, ok := expr.(*ast.BinaryExpr)
		if !ok {
			return nil, nil, nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported JOIN condition %T", expr),
			}
		}

		switch bin.Operator {
		case "=":
			leftIdent, leftIsIdent := bin.Left.(*ast.Identifier)
			rightIdent, rightIsIdent := bin.Right.(*ast.Identifier)

			switch {
			case leftIsIdent && rightIsIdent:
				leftQual := v.qualifierForIdentifier(leftIdent)
				rightQual := v.qualifierForIdentifier(rightIdent)
				if leftQual == v.baseAlias && rightQual == rightAlias {
					leftField, err := v.normalizeIdentifier(leftIdent)
					if err != nil {
						return nil, nil, nil, err
					}
					rightField, err := v.normalizeIdentifier(rightIdent)
					if err != nil {
						return nil, nil, nil, err
					}
					if leftField != rightField {
						return nil, nil, nil, &TranslationError{
							Code:    http.StatusBadRequest,
							Message: fmt.Sprintf("translator: JOIN keys must use identical field names (%s vs %s)", leftField, rightField),
						}
					}
					joinKeys = append(joinKeys, leftField)
					continue
				}
				if leftQual == rightAlias && rightQual == v.baseAlias {
					leftField, err := v.normalizeIdentifier(leftIdent)
					if err != nil {
						return nil, nil, nil, err
					}
					rightField, err := v.normalizeIdentifier(rightIdent)
					if err != nil {
						return nil, nil, nil, err
					}
					if leftField != rightField {
						return nil, nil, nil, &TranslationError{
							Code:    http.StatusBadRequest,
							Message: fmt.Sprintf("translator: JOIN keys must use identical field names (%s vs %s)", leftField, rightField),
						}
					}
					joinKeys = append(joinKeys, leftField)
					continue
				}
			}
		}

		leftAliases := v.aliasesForExpr(bin.Left)
		rightAliases := v.aliasesForExpr(bin.Right)

		if v.isAliasOnly(leftAliases, v.baseAlias) && len(rightAliases) == 0 {
			leftFilters = append(leftFilters, expr)
			continue
		}
		if v.isAliasOnly(rightAliases, v.baseAlias) && len(leftAliases) == 0 {
			leftFilters = append(leftFilters, expr)
			continue
		}
		if v.isAliasOnly(leftAliases, rightAlias) && len(rightAliases) == 0 {
			rightFilters = append(rightFilters, expr)
			continue
		}
		if v.isAliasOnly(rightAliases, rightAlias) && len(leftAliases) == 0 {
			rightFilters = append(rightFilters, expr)
			continue
		}

		if v.isAliasOnly(leftAliases, v.baseAlias) && v.isAliasOnly(rightAliases, rightAlias) {
			return nil, nil, nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: JOIN condition %v must be simple equality between tables", expr),
			}
		}

		return nil, nil, nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported JOIN condition %v", expr),
		}
	}

	return joinKeys, leftFilters, rightFilters, nil
}

func flattenAnd(expr ast.Expr) []ast.Expr {
	if expr == nil {
		return nil
	}
	bin, ok := expr.(*ast.BinaryExpr)
	if ok && bin.Operator == "AND" {
		left := flattenAnd(bin.Left)
		right := flattenAnd(bin.Right)
		return append(left, right...)
	}
	return []ast.Expr{expr}
}

func (v *selectTranslatorVisitor) qualifierForIdentifier(ident *ast.Identifier) string {
	if ident == nil || len(ident.Parts) == 0 {
		return v.baseAlias
	}
	first := strings.ToLower(ident.Parts[0])
	if _, ok := v.bindings[first]; ok {
		return first
	}
	return v.baseAlias
}

func (v *selectTranslatorVisitor) aliasesForExpr(expr ast.Expr) map[string]struct{} {
	aliases := make(map[string]struct{})
	walkExpr(expr, func(e ast.Expr) {
		if id, ok := e.(*ast.Identifier); ok {
			aliases[v.qualifierForIdentifier(id)] = struct{}{}
		}
	})
	delete(aliases, "")
	return aliases
}

func walkExpr(expr ast.Expr, fn func(ast.Expr)) {
	if expr == nil {
		return
	}
	fn(expr)
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		walkExpr(e.Left, fn)
		walkExpr(e.Right, fn)
	case *ast.UnaryExpr:
		walkExpr(e.Expr, fn)
	case *ast.InExpr:
		walkExpr(e.Expr, fn)
		for _, item := range e.List {
			walkExpr(item, fn)
		}
		if e.Subquery != nil {
			walkExpr(&ast.SubqueryExpr{Select: e.Subquery}, fn)
		}
	case *ast.BetweenExpr:
		walkExpr(e.Expr, fn)
		walkExpr(e.Lower, fn)
		walkExpr(e.Upper, fn)
	case *ast.LikeExpr:
		walkExpr(e.Expr, fn)
		walkExpr(e.Pattern, fn)
	case *ast.IsNullExpr:
		walkExpr(e.Expr, fn)
	case *ast.FuncCall:
		for i := range e.Args {
			walkExpr(e.Args[i], fn)
		}
	}
}

func (v *selectTranslatorVisitor) isAliasOnly(aliases map[string]struct{}, alias string) bool {
	if len(aliases) == 0 {
		return false
	}
	if alias == "" {
		return false
	}
	if len(aliases) == 1 {
		_, ok := aliases[alias]
		return ok
	}
	return false
}

func (v *selectTranslatorVisitor) ensureBaseAliasesOnly(expr ast.Expr) error {
	allowed := map[string]struct{}{v.baseAlias: {}}
	return v.ensureAliases(expr, allowed)
}

func (v *selectTranslatorVisitor) ensureAliases(expr ast.Expr, allowed map[string]struct{}) error {
	aliases := v.aliasesForExpr(expr)
	for alias := range aliases {
		if alias == "" {
			continue
		}
		if _, ok := allowed[alias]; !ok {
			return &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: expression references unsupported alias %q", alias),
			}
		}
	}
	return nil
}

func (v *selectTranslatorVisitor) buildStatsPipe(stmt *ast.SelectStatement) ([]string, bool, error) {
	hasGroup := len(stmt.GroupBy) > 0
	aggregates := make([]aggItem, 0)
	groupFields := make([]string, 0)
	groupLookup := make(map[string]struct{})
	preGroupPipes := make([]string, 0)

	if hasGroup {
		v.groupExprAliases = make(map[string]string)
		for i, expr := range stmt.GroupBy {
			exprKey, err := render.Render(expr)
			if err != nil {
				return nil, false, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: failed to normalize GROUP BY expression: %s", err),
					Err:     err,
				}
			}
			if existing, ok := v.groupExprAliases[exprKey]; ok {
				groupFields = append(groupFields, existing)
				groupLookup[existing] = struct{}{}
				continue
			}
			fieldName, pipes, err := v.prepareGroupByField(expr, i)
			if err != nil {
				return nil, false, err
			}
			groupFields = append(groupFields, fieldName)
			groupLookup[fieldName] = struct{}{}
			if len(pipes) > 0 {
				preGroupPipes = append(preGroupPipes, pipes...)
			}
			v.groupExprAliases[exprKey] = fieldName
		}
	} else {
		v.groupExprAliases = nil
	}

	for _, col := range stmt.Columns {
		switch expr := col.Expr.(type) {
		case *ast.StarExpr:
			if len(stmt.Columns) > 1 {
				return nil, false, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: SELECT * cannot be mixed with other columns",
				}
			}
			if hasGroup {
				return nil, false, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: SELECT * not supported with GROUP BY",
				}
			}
			return nil, false, nil
		case *ast.Identifier:
			if !hasGroup {
				continue
			}
			field, err := v.normalizeIdentifier(expr)
			if err != nil {
				return nil, false, err
			}
			if _, ok := groupLookup[field]; !ok {
				return nil, false, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: column %s must appear in GROUP BY", field),
				}
			}
		case *ast.FuncCall:
			if expr.Over != nil {
				if hasGroup {
					return nil, false, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: "translator: window functions are not supported with GROUP BY",
					}
				}
				continue
			}
			if isAggregateFunction(expr) {
				item, err := v.analyzeAggregate(expr, col.Alias)
				if err != nil {
					return nil, false, err
				}
				aggregates = append(aggregates, item)
			} else if hasGroup {
				if _, ok, err := v.lookupGroupExpr(expr); err != nil {
					return nil, false, err
				} else if !ok {
					rendered, renderErr := render.Render(expr)
					if renderErr != nil {
						rendered = fmt.Sprintf("%T", expr)
					}
					return nil, false, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: fmt.Sprintf("translator: non-aggregate function %s must appear in GROUP BY", rendered),
					}
				}
			}
		case *ast.BinaryExpr, *ast.UnaryExpr, *ast.NumericLiteral:
			if hasGroup {
				if _, ok, err := v.lookupGroupExpr(expr); err != nil {
					return nil, false, err
				} else if !ok {
					rendered, renderErr := render.Render(expr)
					if renderErr != nil {
						rendered = fmt.Sprintf("%T", expr)
					}
					return nil, false, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: fmt.Sprintf("translator: expression %s must appear in GROUP BY", rendered),
					}
				}
			}
		default:
			if hasGroup {
				return nil, false, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: unsupported select expression %T", expr),
				}
			}
		}
	}

	if len(aggregates) == 0 {
		if hasGroup {
			return nil, false, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: GROUP BY requires aggregate expressions",
			}
		}
		return nil, false, nil
	}

	var builder strings.Builder
	builder.WriteString("stats")
	if len(groupFields) > 0 {
		builder.WriteString(" by (")
		builder.WriteString(strings.Join(groupFields, ", "))
		builder.WriteString(")")
	}

	funcs := make([]string, 0, len(aggregates))
	aggResults := make(map[string]string)
	for _, agg := range aggregates {
		funcs = append(funcs, agg.statsCall)
		aggResults[agg.key] = agg.resultName
	}
	builder.WriteString(" ")
	builder.WriteString(strings.Join(funcs, ", "))

	v.aggResults = aggResults
	pipes := append(preGroupPipes, builder.String())
	return pipes, true, nil
}

func (v *selectTranslatorVisitor) prepareGroupByField(expr ast.Expr, index int) (string, []string, error) {
	switch e := expr.(type) {
	case *ast.Identifier:
		field, err := v.normalizeIdentifier(e)
		if err != nil {
			return "", nil, err
		}
		return field, nil, nil
	case *ast.FuncCall:
		if isAggregateFunction(e) {
			return "", nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: aggregate functions are not allowed in GROUP BY",
			}
		}
		alias := fmt.Sprintf("group_%d", index+1)
		if pipes, aliasName, supported, err := v.translateStringFunction(e, alias); supported {
			if err != nil {
				return "", nil, err
			}
			return aliasName, append([]string(nil), pipes...), nil
		}
		mathPipe, aliasName, err := v.translateMathProjection(expr, alias)
		if err != nil {
			return "", nil, err
		}
		return aliasName, []string{mathPipe}, nil
	case *ast.BinaryExpr, *ast.UnaryExpr, *ast.NumericLiteral:
		alias := fmt.Sprintf("group_%d", index+1)
		mathPipe, aliasName, err := v.translateMathProjection(expr, alias)
		if err != nil {
			return "", nil, err
		}
		return aliasName, []string{mathPipe}, nil
	default:
		return "", nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported GROUP BY expression %T", expr),
		}
	}
}

func (v *selectTranslatorVisitor) lookupGroupExpr(expr ast.Expr) (string, bool, error) {
	if v.groupExprAliases == nil {
		return "", false, nil
	}
	key, err := render.Render(expr)
	if err != nil {
		return "", false, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: failed to normalize expression: %s", err),
			Err:     err,
		}
	}
	name, ok := v.groupExprAliases[key]
	return name, ok, nil
}

type aggItem struct {
	key        string
	statsCall  string
	resultName string
}

func (v *selectTranslatorVisitor) analyzeAggregate(fn *ast.FuncCall, alias string) (aggItem, error) {
	if fn == nil {
		return aggItem{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid aggregate",
		}
	}
	if fn.Distinct {
		return aggItem{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DISTINCT aggregates are not supported",
		}
	}

	if len(fn.Name.Parts) == 0 {
		return aggItem{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid aggregate function",
		}
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])

	var arg string
	switch name {
	case "COUNT":
		if len(fn.Args) == 0 {
			arg = "*"
		} else if len(fn.Args) == 1 {
			if _, ok := fn.Args[0].(*ast.StarExpr); ok {
				arg = "*"
			} else if ident, ok := fn.Args[0].(*ast.Identifier); ok {
				field, err := v.normalizeIdentifier(ident)
				if err != nil {
					return aggItem{}, err
				}
				arg = field
			} else {
				return aggItem{}, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: COUNT only supports identifiers or *",
				}
			}
		} else {
			return aggItem{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: COUNT expects single argument",
			}
		}
	case "SUM", "AVG", "MIN", "MAX":
		if len(fn.Args) != 1 {
			return aggItem{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects single argument", strings.ToLower(name)),
			}
		}
		ident, ok := fn.Args[0].(*ast.Identifier)
		if !ok {
			return aggItem{}, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s only supports identifiers", strings.ToLower(name)),
			}
		}
		field, err := v.normalizeIdentifier(ident)
		if err != nil {
			return aggItem{}, err
		}
		arg = field
	default:
		return aggItem{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported aggregate %s", name),
		}
	}

	key := aggregateKey(name, arg)
	fnCall := fmt.Sprintf("%s(%s)", strings.ToLower(name), formatAggregateArg(arg))
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return aggItem{key: key, statsCall: fnCall, resultName: fnCall}, nil
	}
	formattedAlias := formatFieldName(alias)
	call := fmt.Sprintf("%s %s", fnCall, formattedAlias)
	return aggItem{key: key, statsCall: call, resultName: formattedAlias}, nil
}

func isAggregateFunction(fn *ast.FuncCall) bool {
	if fn == nil || len(fn.Name.Parts) == 0 {
		return false
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func aggregateKey(name, arg string) string {
	if arg == "" || arg == "*" {
		return strings.ToUpper(name) + "(*)"
	}
	return strings.ToUpper(name) + "(" + strings.ToLower(arg) + ")"
}

func formatAggregateArg(arg string) string {
	if arg == "" || arg == "*" {
		return ""
	}
	return arg
}

func (v *selectTranslatorVisitor) translateStringFunction(fn *ast.FuncCall, alias string) ([]string, string, bool, error) {
	if fn == nil || len(fn.Name.Parts) == 0 {
		return nil, "", false, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid function expression",
		}
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])
	switch name {
	case "UPPER", "LOWER":
		if len(fn.Args) != 1 {
			return nil, "", true, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects single argument", strings.ToLower(name)),
			}
		}
		ident, ok := fn.Args[0].(*ast.Identifier)
		if !ok {
			return nil, "", true, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s only supports identifiers", strings.ToLower(name)),
			}
		}
		rawField, err := v.rawFieldName(ident)
		if err != nil {
			return nil, "", true, err
		}
		aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), strings.ToLower(name), rawField)
		if err != nil {
			return nil, "", true, err
		}
		modifier := "uc"
		if name == "LOWER" {
			modifier = "lc"
		}
		pattern := fmt.Sprintf("<%s:%s>", modifier, rawField)
		pipe := fmt.Sprintf("format \"%s\" as %s", escapeFormatPattern(pattern), aliasName)
		return []string{pipe}, aliasName, true, nil
	case "TRIM", "LTRIM", "RTRIM":
		if len(fn.Args) != 1 {
			return nil, "", true, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects single argument", strings.ToLower(name)),
			}
		}
		ident, ok := fn.Args[0].(*ast.Identifier)
		if !ok {
			return nil, "", true, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s only supports identifiers", strings.ToLower(name)),
			}
		}
		pipes, aliasName, err := v.translateTrimFunction(name, ident, alias)
		return pipes, aliasName, true, err
	case "SUBSTR", "SUBSTRING":
		pipes, aliasName, err := v.translateSubstringFunction(fn, alias)
		return pipes, aliasName, true, err
	case "CONCAT":
		pipes, aliasName, err := v.translateConcatFunction(fn, alias)
		return pipes, aliasName, true, err
	case "REPLACE":
		pipes, aliasName, err := v.translateReplaceFunction(fn, alias)
		return pipes, aliasName, true, err
	case "CURRENT_TIMESTAMP":
		pipes, aliasName, err := v.translateCurrentTimestamp(alias)
		return pipes, aliasName, true, err
	case "CURRENT_DATE":
		pipes, aliasName, err := v.translateCurrentDate(alias)
		return pipes, aliasName, true, err
	default:
		return nil, "", false, nil
	}
}

func makeProjectionAlias(provided, prefix, field string) (string, error) {
	if provided != "" {
		if !safeBareLiteral.MatchString(provided) {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: alias %q contains unsupported characters", provided),
			}
		}
		return provided, nil
	}
	sanitized := sanitizeAliasFromField(field)
	alias := fmt.Sprintf("%s_%s", prefix, sanitized)
	if !safeBareLiteral.MatchString(alias) {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: failed to build alias for %s", field),
		}
	}
	return alias, nil
}

func makeSimpleAlias(provided, fallback string) (string, error) {
	if provided != "" {
		if !safeBareLiteral.MatchString(provided) {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: alias %q contains unsupported characters", provided),
			}
		}
		return provided, nil
	}
	if fallback == "" {
		fallback = "expr"
	}
	if !safeBareLiteral.MatchString(fallback) {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: alias %q contains unsupported characters", fallback),
		}
	}
	return fallback, nil
}

func (v *selectTranslatorVisitor) translateTrimFunction(kind string, ident *ast.Identifier, alias string) ([]string, string, error) {
	rawField, err := v.rawFieldName(ident)
	if err != nil {
		return nil, "", err
	}
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), strings.ToLower(kind), rawField)
	if err != nil {
		return nil, "", err
	}
	pattern, err := buildTrimPattern(kind, aliasName)
	if err != nil {
		return nil, "", err
	}
	pipe := fmt.Sprintf("extract_regexp '%s' from %s", escapeSingleQuotes(pattern), rawField)
	return []string{pipe}, aliasName, nil
}

func sanitizeAliasFromField(field string) string {
	replacer := strings.NewReplacer(
		".", "_",
		"-", "_",
		":", "_",
		"/", "_",
		"+", "_",
		"*", "_",
		"%", "_",
		"^", "_",
		"(", "_",
		")", "_",
		",", "_",
		" ", "_",
		"'", "_",
		"\"", "_",
	)
	value := replacer.Replace(field)
	value = strings.ToLower(value)
	for strings.Contains(value, "__") {
		value = strings.ReplaceAll(value, "__", "_")
	}
	value = strings.Trim(value, "_")
	if value == "" {
		return "col"
	}
	return value
}

func escapeFormatPattern(pattern string) string {
	pattern = strings.ReplaceAll(pattern, "\\", "\\\\")
	pattern = strings.ReplaceAll(pattern, "\"", "\\\"")
	return pattern
}

func escapeSingleQuotes(pattern string) string {
	return strings.ReplaceAll(pattern, "'", "\\'")
}

func buildTrimPattern(kind, alias string) (string, error) {
	switch strings.ToUpper(kind) {
	case "TRIM":
		return fmt.Sprintf("(?s)^\\s*(?P<%s>.*?\\S)?\\s*$", alias), nil
	case "LTRIM":
		return fmt.Sprintf("(?s)^\\s*(?P<%s>.*)$", alias), nil
	case "RTRIM":
		return fmt.Sprintf("(?s)^(?P<%s>.*?\\S)?\\s*$", alias), nil
	default:
		return "", fmt.Errorf("translator: unsupported trim function %s", kind)
	}
}

func (v *selectTranslatorVisitor) translateSubstringFunction(fn *ast.FuncCall, alias string) ([]string, string, error) {
	if len(fn.Args) < 2 || len(fn.Args) > 3 {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: substr expects two or three arguments",
		}
	}
	ident, ok := fn.Args[0].(*ast.Identifier)
	if !ok {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: substr only supports identifiers as first argument",
		}
	}
	rawField, err := v.rawFieldName(ident)
	if err != nil {
		return nil, "", err
	}
	start, err := parseSubstringIntArg(fn.Args[1], "start")
	if err != nil {
		return nil, "", err
	}
	if start < 1 {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: substr start must be >= 1",
		}
	}
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), "substr", rawField)
	if err != nil {
		return nil, "", err
	}
	startIndex := start - 1
	var pattern string
	if len(fn.Args) == 3 {
		length, err := parseSubstringIntArg(fn.Args[2], "length")
		if err != nil {
			return nil, "", err
		}
		if length < 0 {
			return nil, "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: substr length must be >= 0",
			}
		}
		pattern = fmt.Sprintf("(?s)^.{%d}(?P<%s>.{0,%d})", startIndex, aliasName, length)
	} else {
		pattern = fmt.Sprintf("(?s)^.{%d}(?P<%s>.*)$", startIndex, aliasName)
	}
	pipe := fmt.Sprintf("extract_regexp '%s' from %s", escapeSingleQuotes(pattern), rawField)
	return []string{pipe}, aliasName, nil
}

func parseSubstringIntArg(expr ast.Expr, name string) (int, error) {
	lit, ok := expr.(*ast.NumericLiteral)
	if !ok {
		return 0, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: substr %s must be integer literal", name),
		}
	}
	clean := strings.ReplaceAll(strings.TrimSpace(lit.Value), "_", "")
	if clean == "" {
		return 0, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: substr %s must be integer literal", name),
		}
	}
	if strings.ContainsAny(clean, ".eE") {
		return 0, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: substr %s must be integer literal", name),
		}
	}
	val, err := strconv.Atoi(clean)
	if err != nil {
		return 0, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: substr %s must be integer literal", name),
		}
	}
	return val, nil
}

func (v *selectTranslatorVisitor) translateConcatFunction(fn *ast.FuncCall, alias string) ([]string, string, error) {
	if len(fn.Args) == 0 {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: concat expects at least one argument",
		}
	}
	var aliasSource string
	if firstIdent, ok := fn.Args[0].(*ast.Identifier); ok {
		src, err := v.rawFieldName(firstIdent)
		if err != nil {
			return nil, "", err
		}
		aliasSource = src
	} else {
		aliasSource = "expr"
	}
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), "concat", aliasSource)
	if err != nil {
		return nil, "", err
	}
	segments := make([]string, 0, len(fn.Args))
	for _, arg := range fn.Args {
		segment, err := v.concatSegment(arg)
		if err != nil {
			return nil, "", err
		}
		segments = append(segments, segment)
	}
	pattern := strings.Join(segments, "")
	pipe := fmt.Sprintf("format \"%s\" as %s", escapeFormatPattern(pattern), aliasName)
	return []string{pipe}, aliasName, nil
}

func (v *selectTranslatorVisitor) concatSegment(expr ast.Expr) (string, error) {
	switch e := expr.(type) {
	case *ast.StringLiteral:
		return e.Value, nil
	case *ast.NumericLiteral:
		return e.Value, nil
	case *ast.BooleanLiteral:
		if e.Value {
			return "true", nil
		}
		return "false", nil
	case *ast.NullLiteral:
		return "", nil
	case *ast.Identifier:
		field, err := v.rawFieldName(e)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("<%s>", field), nil
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: CONCAT argument %T is not supported", expr),
		}
	}
}

func (v *selectTranslatorVisitor) translateReplaceFunction(fn *ast.FuncCall, alias string) ([]string, string, error) {
	if len(fn.Args) != 3 {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: replace expects three arguments",
		}
	}
	ident, ok := fn.Args[0].(*ast.Identifier)
	if !ok {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: replace only supports identifiers as first argument",
		}
	}
	rawField, err := v.rawFieldName(ident)
	if err != nil {
		return nil, "", err
	}
	searchLit, err := literalFromExpr(fn.Args[1])
	if err != nil {
		return nil, "", err
	}
	replaceLit, err := literalFromExpr(fn.Args[2])
	if err != nil {
		return nil, "", err
	}
	searchVal := searchLit.value
	replaceVal := replaceLit.value
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), "replace", rawField)
	if err != nil {
		return nil, "", err
	}
	pattern := fmt.Sprintf("<%s>", rawField)
	copyPipe := fmt.Sprintf("format \"%s\" as %s", escapeFormatPattern(pattern), aliasName)
	replacePipe := fmt.Sprintf("replace ('%s', '%s') at %s", escapeSingleQuotes(searchVal), escapeSingleQuotes(replaceVal), aliasName)
	return []string{copyPipe, replacePipe}, aliasName, nil
}

func (v *selectTranslatorVisitor) translateWindowFunction(fn *ast.FuncCall, alias string) ([]string, string, error) {
	if fn == nil || fn.Over == nil {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid window function",
		}
	}
	if fn.Distinct {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DISTINCT window functions are not supported",
		}
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])
	var (
		statsCall   string
		aliasSource string
	)
	switch name {
	case "SUM", "MIN", "MAX":
		if len(fn.Args) != 1 {
			return nil, "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s window function expects single argument", strings.ToLower(name)),
			}
		}
		if err := v.ensureBaseAliasesOnly(fn.Args[0]); err != nil {
			return nil, "", err
		}
		ident, ok := fn.Args[0].(*ast.Identifier)
		if !ok {
			return nil, "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s window function requires identifier argument", strings.ToLower(name)),
			}
		}
		field, err := v.normalizeIdentifier(ident)
		if err != nil {
			return nil, "", err
		}
		statsCall = fmt.Sprintf("%s(%s)", strings.ToLower(name), field)
		aliasSource = field
	case "COUNT":
		if len(fn.Args) == 0 {
			statsCall = "count()"
			aliasSource = strings.ToLower(name)
		} else if len(fn.Args) == 1 {
			switch arg := fn.Args[0].(type) {
			case *ast.StarExpr:
				statsCall = "count()"
				aliasSource = strings.ToLower(name)
			case *ast.Identifier:
				if err := v.ensureBaseAliasesOnly(arg); err != nil {
					return nil, "", err
				}
				field, err := v.normalizeIdentifier(arg)
				if err != nil {
					return nil, "", err
				}
				statsCall = fmt.Sprintf("count(%s)", field)
				aliasSource = field
			default:
				return nil, "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: COUNT window function only supports identifiers or *",
				}
			}
		} else {
			return nil, "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: COUNT window function expects zero or one argument",
			}
		}
	default:
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: window function %s is not supported", name),
		}
	}
	if strings.TrimSpace(aliasSource) == "" {
		aliasSource = strings.ToLower(name)
	}
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), strings.ToLower(name), aliasSource)
	if err != nil {
		return nil, "", err
	}
	partitionClause := ""
	if len(fn.Over.PartitionBy) > 0 {
		fields := make([]string, 0, len(fn.Over.PartitionBy))
		for _, expr := range fn.Over.PartitionBy {
			if err := v.ensureBaseAliasesOnly(expr); err != nil {
				return nil, "", err
			}
			ident, ok := expr.(*ast.Identifier)
			if !ok {
				return nil, "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: PARTITION BY only supports identifiers",
				}
			}
			field, err := v.normalizeIdentifier(ident)
			if err != nil {
				return nil, "", err
			}
			fields = append(fields, field)
		}
		partitionClause = " by (" + strings.Join(fields, ", ") + ")"
	}
	pipes := make([]string, 0)
	if len(fn.Over.OrderBy) > 0 {
		for _, item := range fn.Over.OrderBy {
			if err := v.ensureBaseAliasesOnly(item.Expr); err != nil {
				return nil, "", err
			}
		}
		orderPipe, err := v.translateOrderBy(fn.Over.OrderBy, false)
		if err != nil {
			return nil, "", err
		}
		pipes = append(pipes, orderPipe)
	}
	statsPipe := fmt.Sprintf("running_stats%s %s as %s", partitionClause, statsCall, aliasName)
	pipes = append(pipes, statsPipe)
	return pipes, aliasName, nil
}

func (v *selectTranslatorVisitor) translateCurrentTimestamp(alias string) ([]string, string, error) {
	aliasName, err := makeSimpleAlias(strings.TrimSpace(alias), "current_timestamp")
	if err != nil {
		return nil, "", err
	}
	tmpField := aliasName + "_nanos"
	if !safeFormatFieldLiteral.MatchString(tmpField) {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: alias %s produces unsupported field name", aliasName),
		}
	}
	pipes := []string{
		fmt.Sprintf("math now() as %s", tmpField),
		fmt.Sprintf("format '<time:%s>' as %s", tmpField, aliasName),
		fmt.Sprintf("delete %s", tmpField),
	}
	return pipes, aliasName, nil
}

func (v *selectTranslatorVisitor) translateCurrentDate(alias string) ([]string, string, error) {
	aliasName, err := makeSimpleAlias(strings.TrimSpace(alias), "current_date")
	if err != nil {
		return nil, "", err
	}
	nanosField := aliasName + "_nanos"
	formattedField := aliasName + "_formatted"
	if !safeFormatFieldLiteral.MatchString(nanosField) || !safeFormatFieldLiteral.MatchString(formattedField) {
		return nil, "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: alias %s produces unsupported field name", aliasName),
		}
	}
	pattern := fmt.Sprintf("^(?P<%s>[0-9]{4}-[0-9]{2}-[0-9]{2})", aliasName)
	pipes := []string{
		fmt.Sprintf("math now() as %s", nanosField),
		fmt.Sprintf("format '<time:%s>' as %s", nanosField, formattedField),
		fmt.Sprintf("extract_regexp '%s' from %s", escapeSingleQuotes(pattern), formattedField),
		fmt.Sprintf("delete %s, %s", nanosField, formattedField),
	}
	return pipes, aliasName, nil
}

func (v *selectTranslatorVisitor) translateMathProjection(expr ast.Expr, alias string) (string, string, error) {
	mathExpr, err := v.mathExprToString(expr)
	if err != nil {
		return "", "", err
	}
	aliasName, err := makeProjectionAlias(strings.TrimSpace(alias), "expr", mathExpr)
	if err != nil {
		return "", "", err
	}
	pipe := fmt.Sprintf("math %s as %s", mathExpr, aliasName)
	return pipe, aliasName, nil
}

func (v *selectTranslatorVisitor) mathExprToString(expr ast.Expr) (string, error) {
	switch e := expr.(type) {
	case *ast.NumericLiteral:
		return e.Value, nil
	case *ast.Identifier:
		return v.rawFieldName(e)
	case *ast.UnaryExpr:
		op := strings.ToUpper(e.Operator)
		if op != "-" {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported unary operator %q in math expression", e.Operator),
			}
		}
		inner, err := v.mathExprToString(e.Expr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("-(%s)", inner), nil
	case *ast.BinaryExpr:
		op := strings.ToUpper(e.Operator)
		if !isMathOperator(op) {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported operator %q in math expression", e.Operator),
			}
		}
		left, err := v.mathExprToString(e.Left)
		if err != nil {
			return "", err
		}
		right, err := v.mathExprToString(e.Right)
		if err != nil {
			return "", err
		}
		operator := e.Operator
		return fmt.Sprintf("(%s %s %s)", left, operator, right), nil
	case *ast.FuncCall:
		return v.mathFuncToString(e)
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported math expression %T", e),
		}
	}
}

func (v *selectTranslatorVisitor) mathFuncToString(fn *ast.FuncCall) (string, error) {
	if fn == nil || len(fn.Name.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid math function",
		}
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])
	lower := strings.ToLower(name)
	switch name {
	case "ABS", "CEIL", "FLOOR", "EXP", "LN":
		if len(fn.Args) != 1 {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects single argument", lower),
			}
		}
		arg, err := v.mathExprToString(fn.Args[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s(%s)", lower, arg), nil
	case "ROUND":
		if len(fn.Args) == 0 || len(fn.Args) > 2 {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: round expects one or two arguments",
			}
		}
		arg, err := v.mathExprToString(fn.Args[0])
		if err != nil {
			return "", err
		}
		if len(fn.Args) == 1 {
			return fmt.Sprintf("round(%s)", arg), nil
		}
		nearest, err := v.mathExprToString(fn.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("round(%s, %s)", arg, nearest), nil
	case "POWER", "POW":
		if len(fn.Args) != 2 {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects two arguments", lower),
			}
		}
		base, err := v.mathExprToString(fn.Args[0])
		if err != nil {
			return "", err
		}
		exponent, err := v.mathExprToString(fn.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s ^ %s)", base, exponent), nil
	case "MAX", "MIN", "GREATEST", "LEAST":
		if len(fn.Args) == 0 {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects at least one argument", lower),
			}
		}
		parts := make([]string, 0, len(fn.Args))
		for _, argExpr := range fn.Args {
			arg, err := v.mathExprToString(argExpr)
			if err != nil {
				return "", err
			}
			parts = append(parts, arg)
		}
		funcName := lower
		switch name {
		case "GREATEST":
			funcName = "max"
		case "LEAST":
			funcName = "min"
		}
		return fmt.Sprintf("%s(%s)", funcName, strings.Join(parts, ", ")), nil
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported function %s in math expression", name),
		}
	}
}

func isMathOperator(op string) bool {
	switch op {
	case "+", "-", "*", "/", "%", "^":
		return true
	default:
		return false
	}
}

func (v *selectTranslatorVisitor) buildProjectionPipes(columns []ast.SelectItem, aggregated bool) ([]string, []string, error) {
	if len(columns) == 1 {
		if _, ok := columns[0].Expr.(*ast.StarExpr); ok {
			return nil, nil, nil
		}
	}

	computedPipes := make([]string, 0)
	renamePairs := make([]string, 0)
	fields := make([]string, 0, len(columns))

	for _, col := range columns {
		switch expr := col.Expr.(type) {
		case *ast.Identifier:
			if alias := strings.ToUpper(strings.Join(expr.Parts, ".")); alias == "CURRENT_TIMESTAMP" || alias == "CURRENT_DATE" {
				var (
					pipes     []string
					aliasName string
					err       error
				)
				if alias == "CURRENT_TIMESTAMP" {
					pipes, aliasName, err = v.translateCurrentTimestamp(col.Alias)
				} else {
					pipes, aliasName, err = v.translateCurrentDate(col.Alias)
				}
				if err != nil {
					return nil, nil, err
				}
				computedPipes = append(computedPipes, pipes...)
				fields = append(fields, formatFieldName(aliasName))
				continue
			}
			field, err := v.normalizeIdentifier(expr)
			if err != nil {
				return nil, nil, err
			}
			finalName := field
			if alias := strings.TrimSpace(col.Alias); alias != "" {
				formattedAlias := formatFieldName(alias)
				renamePairs = append(renamePairs, fmt.Sprintf("%s as %s", field, formattedAlias))
				finalName = formattedAlias
			}
			fields = append(fields, finalName)
		case *ast.FuncCall:
			if expr.Over != nil {
				if aggregated {
					return nil, nil, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: "translator: window functions are not supported with GROUP BY",
					}
				}
				windowPipes, aliasName, err := v.translateWindowFunction(expr, col.Alias)
				if err != nil {
					return nil, nil, err
				}
				computedPipes = append(computedPipes, windowPipes...)
				fields = append(fields, formatFieldName(aliasName))
				continue
			}
			if aggregated && isAggregateFunction(expr) {
				if alias := strings.TrimSpace(col.Alias); alias != "" {
					fields = append(fields, formatFieldName(alias))
				} else {
					key, err := v.aggregateKeyFromFunc(expr)
					if err != nil {
						return nil, nil, err
					}
					if name, ok := v.aggResults[key]; ok {
						fields = append(fields, name)
					} else {
						fields = append(fields, key)
					}
				}
				continue
			}

			if aggregated {
				groupField, ok, err := v.lookupGroupExpr(expr)
				if err != nil {
					return nil, nil, err
				}
				if ok {
					finalName := groupField
					if alias := strings.TrimSpace(col.Alias); alias != "" {
						formattedAlias := formatFieldName(alias)
						if formattedAlias != groupField {
							renamePairs = append(renamePairs, fmt.Sprintf("%s as %s", groupField, formattedAlias))
						}
						finalName = formattedAlias
					}
					fields = append(fields, finalName)
					continue
				}
				return nil, nil, &TranslationError{
					Code:    http.StatusBadRequest,
					Message: fmt.Sprintf("translator: unsupported function %T in aggregated select", expr),
				}
			}

			if pipes, aliasName, supported, err := v.translateStringFunction(expr, col.Alias); supported {
				if err != nil {
					return nil, nil, err
				}
				computedPipes = append(computedPipes, pipes...)
				fields = append(fields, formatFieldName(aliasName))
				break
			}
			mathPipe, aliasName, err := v.translateMathProjection(expr, col.Alias)
			if err != nil {
				return nil, nil, err
			}
			computedPipes = append(computedPipes, mathPipe)
			fields = append(fields, formatFieldName(aliasName))
		case *ast.BinaryExpr, *ast.UnaryExpr, *ast.NumericLiteral:
			if aggregated {
				groupField, ok, err := v.lookupGroupExpr(col.Expr)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					return nil, nil, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: fmt.Sprintf("translator: unsupported expression %T in aggregated select", expr),
					}
				}
				finalName := groupField
				if alias := strings.TrimSpace(col.Alias); alias != "" {
					formattedAlias := formatFieldName(alias)
					if formattedAlias != groupField {
						renamePairs = append(renamePairs, fmt.Sprintf("%s as %s", groupField, formattedAlias))
					}
					finalName = formattedAlias
				}
				fields = append(fields, finalName)
				continue
			}
			mathPipe, aliasName, err := v.translateMathProjection(col.Expr, col.Alias)
			if err != nil {
				return nil, nil, err
			}
			computedPipes = append(computedPipes, mathPipe)
			fields = append(fields, formatFieldName(aliasName))
		case *ast.StarExpr:
			return nil, nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: SELECT * cannot be combined with other projections",
			}
		default:
			return nil, nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported projection expression %T", expr),
			}
		}
	}

	pipes := make([]string, 0, len(computedPipes)+2)
	pipes = append(pipes, computedPipes...)
	if len(renamePairs) > 0 {
		pipes = append(pipes, "rename "+strings.Join(renamePairs, ", "))
	}
	if len(fields) > 0 && !aggregated {
		pipes = append(pipes, "fields "+strings.Join(fields, ", "))
	}
	return pipes, fields, nil
}

func (v *selectTranslatorVisitor) translateOrderBy(items []ast.OrderItem, aggregated bool) (string, error) {
	clauses := make([]string, 0, len(items))
	for _, item := range items {
		var field string
		switch expr := item.Expr.(type) {
		case *ast.Identifier:
			var err error
			field, err = v.normalizeIdentifier(expr)
			if err != nil {
				return "", err
			}
		case *ast.FuncCall:
			if !aggregated {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: ORDER BY function requires aggregation",
				}
			}
			key, err := v.aggregateKeyFromFunc(expr)
			if err != nil {
				return "", err
			}
			name, ok := v.aggResults[key]
			if !ok {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: ORDER BY references unknown aggregate",
				}
			}
			field = name
		default:
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: ORDER BY expression %T not supported", expr),
			}
		}
		if item.Direction == ast.Descending {
			clauses = append(clauses, field+" desc")
		} else {
			clauses = append(clauses, field)
		}
	}
	return "sort by (" + strings.Join(clauses, ", ") + ")", nil
}

func (v *selectTranslatorVisitor) aggregateKeyFromFunc(fn *ast.FuncCall) (string, error) {
	if fn == nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid aggregate in ORDER BY",
		}
	}
	if len(fn.Name.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid aggregate in ORDER BY",
		}
	}
	name := strings.ToUpper(fn.Name.Parts[len(fn.Name.Parts)-1])
	var arg string
	switch name {
	case "COUNT":
		if len(fn.Args) == 0 {
			arg = "*"
		} else if len(fn.Args) == 1 {
			if _, ok := fn.Args[0].(*ast.StarExpr); ok {
				arg = "*"
			} else if ident, ok := fn.Args[0].(*ast.Identifier); ok {
				field, err := v.normalizeIdentifier(ident)
				if err != nil {
					return "", err
				}
				arg = field
			} else {
				return "", &TranslationError{
					Code:    http.StatusBadRequest,
					Message: "translator: COUNT only supports identifiers or *",
				}
			}
		} else {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: COUNT expects single argument",
			}
		}
	case "SUM", "AVG", "MIN", "MAX":
		if len(fn.Args) != 1 {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s expects single argument", strings.ToLower(name)),
			}
		}
		ident, ok := fn.Args[0].(*ast.Identifier)
		if !ok {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: %s only supports identifiers", strings.ToLower(name)),
			}
		}
		field, err := v.normalizeIdentifier(ident)
		if err != nil {
			return "", err
		}
		arg = field
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported aggregate %s", name),
		}
	}
	return aggregateKey(name, arg), nil
}

func (v *selectTranslatorVisitor) translateLimit(limit *ast.LimitClause) ([]string, error) {
	if limit == nil {
		return nil, nil
	}
	pipes := make([]string, 0, 2)
	if limit.Offset != nil {
		offsetLit, err := literalFromExpr(limit.Offset)
		if err != nil {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: OFFSET expects numeric literal: %s", err),
				Err:     err,
			}
		}
		if offsetLit.kind != literalNumber {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: OFFSET expects numeric literal",
			}
		}
		pipes = append(pipes, "offset "+offsetLit.value)
	}
	if limit.Count != nil {
		countLit, err := literalFromExpr(limit.Count)
		if err != nil {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: LIMIT expects numeric literal: %s", err),
				Err:     err,
			}
		}
		if countLit.kind != literalNumber {
			return nil, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: LIMIT expects numeric literal",
				Err:     err,
			}
		}
		pipes = append(pipes, "limit "+countLit.value)
	}
	if len(pipes) == 0 {
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: LIMIT/OFFSET clause is empty",
		}
	}
	return pipes, nil
}

func (v *selectTranslatorVisitor) translateExpr(expr ast.Expr) (string, error) {
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		switch e.Operator {
		case "AND", "OR":
			left, err := v.translateExpr(e.Left)
			if err != nil {
				return "", err
			}
			right, err := v.translateExpr(e.Right)
			if err != nil {
				return "", err
			}
			return "(" + left + " " + e.Operator + " " + right + ")", nil
		case "=":
			return v.translateComparison(e.Left, e.Right, comparisonEqual)
		case "!=", "<>":
			return v.translateComparison(e.Left, e.Right, comparisonNotEqual)
		case ">":
			return v.translateComparison(e.Left, e.Right, comparisonGreater)
		case ">=":
			return v.translateComparison(e.Left, e.Right, comparisonGreaterEqual)
		case "<":
			return v.translateComparison(e.Left, e.Right, comparisonLess)
		case "<=":
			return v.translateComparison(e.Left, e.Right, comparisonLessEqual)
		default:
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: unsupported operator %q", e.Operator),
			}
		}
	case *ast.UnaryExpr:
		if strings.EqualFold(e.Operator, "NOT") {
			inner, err := v.translateExpr(e.Expr)
			if err != nil {
				return "", err
			}
			return "-(" + inner + ")", nil
		}
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported unary operator %q", e.Operator),
		}
	case *ast.InExpr:
		return v.translateInExpr(e)
	case *ast.LikeExpr:
		return v.translateLikeExpr(e)
	case *ast.IsNullExpr:
		return v.translateIsNullExpr(e)
	case *ast.BetweenExpr:
		return v.translateBetweenExpr(e)
	case *ast.FuncCall:
		if v.aggResults != nil {
			key, err := v.aggregateKeyFromFunc(e)
			if err != nil {
				return "", err
			}
			if name, ok := v.aggResults[key]; ok {
				return name, nil
			}
		}
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: unsupported function expression in filter",
		}
	case *ast.Identifier:
		return v.normalizeIdentifier(e)
	case *ast.StringLiteral:
		return formatString(e.Value), nil
	case *ast.NumericLiteral:
		return e.Value, nil
	case *ast.BooleanLiteral:
		if e.Value {
			return "true", nil
		}
		return "false", nil
	case *ast.NullLiteral:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: NULL literal is not supported in this context",
		}
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported expression %T", e),
		}
	}
}

func (v *selectTranslatorVisitor) translateComparison(left, right ast.Expr, cmp comparisonKind) (string, error) {
	field, lit, flipped, err := v.extractFieldAndLiteral(left, right)
	if err != nil {
		return "", err
	}

	switch cmp {
	case comparisonEqual:
		clause := field + ":" + lit.format()
		return clause, nil
	case comparisonNotEqual:
		clause := field + ":" + lit.format()
		return "-" + clause, nil
	case comparisonGreater, comparisonGreaterEqual, comparisonLess, comparisonLessEqual:
		if flipped {
			return "", &TranslationError{
				Code:    http.StatusBadRequest,
				Message: "translator: comparisons must have identifier on left side",
			}
		}
		var op string
		switch cmp {
		case comparisonGreater:
			op = ">"
		case comparisonGreaterEqual:
			op = ">="
		case comparisonLess:
			op = "<"
		case comparisonLessEqual:
			op = "<="
		}
		clause := field + ":" + op + lit.format()
		return clause, nil
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: unsupported comparison kind",
		}
	}
}

func (v *selectTranslatorVisitor) translateBetweenExpr(expr *ast.BetweenExpr) (string, error) {
	if expr == nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid BETWEEN expression",
		}
	}
	field, err := v.filterFieldFromExpr(expr.Expr)
	if err != nil {
		return "", err
	}
	lower, err := literalFromExpr(expr.Lower)
	if err != nil {
		return "", err
	}
	upper, err := literalFromExpr(expr.Upper)
	if err != nil {
		return "", err
	}
	clause := field + ":[" + lower.format() + ", " + upper.format() + "]"
	if expr.Not {
		return "-" + clause, nil
	}
	return clause, nil
}

func (v *selectTranslatorVisitor) translateInExpr(expr *ast.InExpr) (string, error) {
	if expr.Subquery != nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: IN subqueries are not supported yet",
		}
	}
	field, err := v.filterFieldFromExpr(expr.Expr)
	if err != nil {
		return "", err
	}
	if len(expr.List) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: IN list cannot be empty",
		}
	}
	values := make([]string, 0, len(expr.List))
	for _, item := range expr.List {
		lit, err := literalFromExpr(item)
		if err != nil {
			return "", err
		}
		values = append(values, lit.format())
	}
	clause := field + ":(" + strings.Join(values, " OR ") + ")"
	if expr.Not {
		return "-" + clause, nil
	}
	return clause, nil
}

func (v *selectTranslatorVisitor) translateLikeExpr(expr *ast.LikeExpr) (string, error) {
	field, err := v.filterFieldFromExpr(expr.Expr)
	if err != nil {
		return "", err
	}
	lit, err := literalFromExpr(expr.Pattern)
	if err != nil {
		return "", err
	}
	if lit.kind != literalString {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: LIKE expects string literal",
		}
	}

	pattern := lit.value
	translated, err := convertLikePattern(pattern)
	if err != nil {
		return "", err
	}

	clause := field + ":" + translated
	if expr.Not {
		return "-" + clause, nil
	}
	return clause, nil
}

func (v *selectTranslatorVisitor) translateIsNullExpr(expr *ast.IsNullExpr) (string, error) {
	field, err := v.filterFieldFromExpr(expr.Expr)
	if err != nil {
		return "", err
	}
	if expr.Not {
		return field + ":*", nil
	}
	return field + ":\"\"", nil
}

func (v *selectTranslatorVisitor) ensureFilterFunctionAlias(fn *ast.FuncCall) (string, error) {
	if fn == nil || len(fn.Name.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid function expression",
		}
	}
	if isAggregateFunction(fn) {
		name := strings.ToLower(fn.Name.Parts[len(fn.Name.Parts)-1])
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: aggregate function %s is not supported in this context", name),
		}
	}
	key, err := render.Render(fn)
	if err != nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: failed to normalize function expression: %s", err),
			Err:     err,
		}
	}
	if v.filterComputations == nil {
		v.filterComputations = make(map[string]*filterComputation)
	}
	if comp, ok := v.filterComputations[key]; ok {
		return comp.alias, nil
	}
	aliasBase := fmt.Sprintf("__filter_expr_%d", len(v.filterOrder)+1)
	pipes, aliasName, supported, err := v.translateStringFunction(fn, aliasBase)
	if !supported {
		name := strings.ToLower(fn.Name.Parts[len(fn.Name.Parts)-1])
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: function %s is not supported in filter", name),
			Err:     err,
		}
	}
	if err != nil {
		return "", err
	}
	comp := &filterComputation{
		alias:    formatFieldName(aliasName),
		rawAlias: aliasName,
		pipes:    append([]string(nil), pipes...),
	}
	v.filterComputations[key] = comp
	v.filterOrder = append(v.filterOrder, key)
	if v.filterDeleteSet == nil {
		v.filterDeleteSet = make(map[string]struct{})
	}
	if _, exists := v.filterDeleteSet[aliasName]; !exists {
		v.filterDeleteSet[aliasName] = struct{}{}
		v.filterDelete = append(v.filterDelete, aliasName)
	}
	return comp.alias, nil
}

func (v *selectTranslatorVisitor) filterFieldFromExpr(expr ast.Expr) (string, error) {
	field, ok, err := v.fieldNameFromExpr(expr)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: expected identifier, got %T", expr),
		}
	}
	return field, nil
}

func (v *selectTranslatorVisitor) normalizeIdentifier(ident *ast.Identifier) (string, error) {
	if ident == nil || len(ident.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid identifier",
		}
	}
	parts := make([]string, len(ident.Parts))
	copy(parts, ident.Parts)

	if len(parts) > 1 {
		first := strings.ToLower(parts[0])
		if _, ok := v.bindings[first]; ok {
			parts = parts[1:]
		}
	}
	field := strings.Join(parts, ".")
	if field == "" {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid identifier",
		}
	}
	if safeBareLiteral.MatchString(field) {
		return field, nil
	}
	return quoteString(field), nil
}

func (v *selectTranslatorVisitor) rawFieldName(ident *ast.Identifier) (string, error) {
	if ident == nil || len(ident.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid identifier",
		}
	}
	parts := make([]string, len(ident.Parts))
	copy(parts, ident.Parts)
	if len(parts) > 1 {
		first := strings.ToLower(parts[0])
		if _, ok := v.bindings[first]; ok {
			parts = parts[1:]
		}
	}
	if len(parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: invalid identifier",
		}
	}
	field := strings.Join(parts, ".")
	if !safeFormatFieldLiteral.MatchString(field) {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: field %s cannot be used in scalar function", field),
		}
	}
	return field, nil
}

func (v *selectTranslatorVisitor) extractFieldAndLiteral(left, right ast.Expr) (string, literalValue, bool, error) {
	if name, ok, err := v.fieldNameFromExpr(left); err != nil {
		return "", literalValue{}, false, err
	} else if ok {
		lit, err := literalFromExpr(right)
		if err != nil {
			return "", literalValue{}, false, err
		}
		return name, lit, false, nil
	}
	if name, ok, err := v.fieldNameFromExpr(right); err != nil {
		return "", literalValue{}, false, err
	} else if ok {
		lit, err := literalFromExpr(left)
		if err != nil {
			return "", literalValue{}, false, err
		}
		return name, lit, true, nil
	}
	return "", literalValue{}, false, &TranslationError{
		Code:    http.StatusBadRequest,
		Message: "translator: comparison requires identifier and literal",
	}
}

func (v *selectTranslatorVisitor) fieldNameFromExpr(expr ast.Expr) (string, bool, error) {
	switch e := expr.(type) {
	case *ast.Identifier:
		name, err := v.normalizeIdentifier(e)
		if err != nil {
			return "", false, err
		}
		return name, true, nil
	case *ast.FuncCall:
		if v.aggResults != nil {
			if isAggregateFunction(e) {
				key, err := v.aggregateKeyFromFunc(e)
				if err != nil {
					return "", false, err
				}
				name, ok := v.aggResults[key]
				if !ok {
					return "", false, &TranslationError{
						Code:    http.StatusBadRequest,
						Message: "translator: unknown aggregate referenced",
					}
				}
				return name, true, nil
			}
			if groupField, ok, err := v.lookupGroupExpr(e); err != nil {
				return "", false, err
			} else if ok {
				return formatFieldName(groupField), true, nil
			}
			return "", false, &TranslationError{
				Code:    http.StatusBadRequest,
				Message: fmt.Sprintf("translator: function %T is not supported in aggregated context", e),
			}
		}
		alias, err := v.ensureFilterFunctionAlias(e)
		if err != nil {
			return "", false, err
		}
		return alias, true, nil
	default:
		return "", false, nil
	}
}

type comparisonKind int

const (
	comparisonEqual comparisonKind = iota
	comparisonNotEqual
	comparisonGreater
	comparisonGreaterEqual
	comparisonLess
	comparisonLessEqual
)

type literalKind int

const (
	literalString literalKind = iota
	literalNumber
	literalBoolean
)

type literalValue struct {
	kind  literalKind
	value string
}

func literalFromExpr(expr ast.Expr) (literalValue, error) {
	switch v := expr.(type) {
	case *ast.StringLiteral:
		return literalValue{kind: literalString, value: v.Value}, nil
	case *ast.NumericLiteral:
		return literalValue{kind: literalNumber, value: v.Value}, nil
	case *ast.BooleanLiteral:
		if v.Value {
			return literalValue{kind: literalBoolean, value: "true"}, nil
		}
		return literalValue{kind: literalBoolean, value: "false"}, nil
	default:
		return literalValue{}, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("unsupported literal %T", expr),
		}
	}
}

func (l literalValue) format() string {
	switch l.kind {
	case literalString:
		return formatString(l.value)
	case literalNumber, literalBoolean:
		return l.value
	default:
		return l.value
	}
}

func formatString(val string) string {
	if val == "" {
		return quoteString(val)
	}
	if safeBareLiteral.MatchString(val) {
		return val
	}
	if safeWildcardLiteral.MatchString(val) {
		return val
	}
	return quoteString(val)
}

func quoteString(val string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range val {
		if r == '\\' || r == '"' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

func formatFieldName(name string) string {
	if safeBareLiteral.MatchString(name) {
		return name
	}
	return quoteString(name)
}

func convertLikePattern(pattern string) (string, error) {
	percentCount := strings.Count(pattern, "%")
	underscore := strings.Contains(pattern, "_")

	switch {
	case percentCount == 0 && !underscore:
		return formatString(pattern), nil
	case percentCount == 1 && strings.HasSuffix(pattern, "%") && !underscore:
		prefix := pattern[:len(pattern)-1]
		if prefix == "" {
			return "*", nil
		}
		return formatWildcard(prefix + "*"), nil
	case percentCount == 1 && strings.HasPrefix(pattern, "%") && !underscore:
		suffix := pattern[1:]
		if suffix == "" {
			return "*", nil
		}
		return formatWildcard("*" + suffix), nil
	case percentCount == 2 && strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") && !underscore && strings.Count(pattern[1:len(pattern)-1], "%") == 0:
		substr := pattern[1 : len(pattern)-1]
		if substr == "" {
			return "*", nil
		}
		return formatWildcard("*" + substr + "*"), nil
	default:
		regex := likeToRegex(pattern)
		return "~" + quoteString(regex), nil
	}
}

func formatWildcard(val string) string {
	if needsQuoteForPattern(val) {
		return quoteString(val)
	}
	return val
}

func needsQuoteForPattern(val string) bool {
	for _, r := range val {
		if r != '*' && r != '_' && r != '-' && r != ':' && r != '/' && r != '.' && (r < '0' || r > '9') && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') {
			return true
		}
	}
	return false
}

func likeToRegex(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(string(pattern[i])))
			} else {
				b.WriteString("\\")
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	b.WriteString("$")
	return b.String()
}
