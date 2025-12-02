package parser

import (
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/lexer"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/token"
)

const (
	// MaxParserDepth limits recursion depth to prevent stack overflow
	MaxParserDepth = 100
	// MaxExpressionCount limits number of expressions in lists
	MaxExpressionCount = 1000
)

// Parser consumes SQL tokens and produces AST nodes for a core ANSI subset.
type Parser struct {
	l      *lexer.Lexer
	errors []error

	curToken   token.Token
	peekToken  token.Token
	peekToken2 token.Token

	depth int // Current recursion depth
}

// New returns a parser over the provided lexer.
func New(l *lexer.Lexer) *Parser {
	p := &Parser{l: l, errors: make([]error, 0)}
	p.curToken = token.Token{}
	p.peekToken = p.l.NextToken()
	p.peekToken2 = p.l.NextToken()
	p.nextToken()
	return p
}

// Errors exposes parsing errors encountered so far.
func (p *Parser) Errors() []error {
	return p.errors
}

func (p *Parser) addError(pos token.Position, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.errors = append(p.errors, &SyntaxError{Pos: pos, Msg: msg})
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.peekToken2
	p.peekToken2 = p.l.NextToken()
}

func (p *Parser) curTokenIs(t token.Type) bool      { return p.curToken.Type == t }
func (p *Parser) peekTokenIs(t token.Type) bool     { return p.peekToken.Type == t }
func (p *Parser) peekPeekTokenIs(t token.Type) bool { return p.peekToken2.Type == t }

func (p *Parser) expectPeek(t token.Type) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.addError(p.peekToken.Pos, "expected %s, got %s", t, p.peekToken.Type)
	return false
}

// ParseStatement parses a top-level SQL statement.
func (p *Parser) ParseStatement() ast.Statement {
	var stmt ast.Statement

	switch p.curToken.Type {
	case token.WITH:
		with := p.parseWithClause()
		if p.curTokenIs(token.SELECT) {
			selectStmt := p.parseSelectStatement()
			if selectStmt != nil {
				selectStmt.With = with
			}
			stmt = selectStmt
		}
	case token.SELECT:
		stmt = p.parseSelectStatement()
	case token.INSERT:
		stmt = p.parseInsertStatement()
	case token.UPDATE:
		stmt = p.parseUpdateStatement()
	case token.DELETE:
		stmt = p.parseDeleteStatement()
	case token.CREATE:
		stmt = p.parseCreateViewStatement()
	case token.DROP:
		stmt = p.parseDropViewStatement()
	case token.DESCRIBE:
		stmt = p.parseDescribeStatement()
	case token.SHOW:
		stmt = p.parseShowStatement()
	default:
		p.addError(p.curToken.Pos, "unsupported statement starting with %s", p.curToken.Type)
	}

	consumedSemicolon := p.consumeSemicolons()
	if !p.peekTokenIs(token.EOF) {
		tok := p.peekToken
		if consumedSemicolon {
			tok = p.curToken
		}
		p.addError(tok.Pos, "unexpected token %s after statement", tok.Type)
	}

	return stmt
}

func (p *Parser) consumeSemicolons() bool {
	consumed := false
	for p.curTokenIs(token.SEMICOLON) || p.peekTokenIs(token.SEMICOLON) {
		consumed = true
		p.nextToken()
	}
	return consumed
}

func (p *Parser) parseWithClause() *ast.WithClause {
	clause := &ast.WithClause{}
	if p.peekTokenIs(token.RECURSIVE) {
		p.nextToken()
		clause.Recursive = true
	}

	for {
		if !p.expectPeek(token.IDENT) {
			return clause
		}
		cte := ast.CommonTableExpression{Name: p.parseIdentifier()}

		if p.peekTokenIs(token.LPAREN) {
			p.expectPeek(token.LPAREN)
			if p.expectPeek(token.IDENT) {
				cte.Columns = append(cte.Columns, p.parseIdentifier())
				for p.peekTokenIs(token.COMMA) {
					p.nextToken()
					if !p.expectPeek(token.IDENT) {
						return clause
					}
					cte.Columns = append(cte.Columns, p.parseIdentifier())
				}
			}
			if !p.expectPeek(token.RPAREN) {
				return clause
			}
		}

		if !p.expectPeek(token.AS) {
			return clause
		}
		if !p.expectPeek(token.LPAREN) {
			return clause
		}

		p.nextToken()
		switch p.curToken.Type {
		case token.WITH:
			innerWith := p.parseWithClause()
			if !p.curTokenIs(token.SELECT) {
				p.addError(p.curToken.Pos, "WITH subquery must start with SELECT, got %s", p.curToken.Type)
				return clause
			}
			cte.Select = p.parseSelectStatement()
			if cte.Select != nil {
				cte.Select.With = innerWith
			}
		case token.SELECT:
			cte.Select = p.parseSelectStatement()
		default:
			p.addError(p.curToken.Pos, "WITH subquery must start with SELECT, got %s", p.curToken.Type)
			return clause
		}

		if !p.expectPeek(token.RPAREN) {
			return clause
		}

		clause.CTEs = append(clause.CTEs, cte)

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
			continue
		}
		break
	}

	if !p.expectPeek(token.SELECT) {
		return clause
	}
	return clause
}

func (p *Parser) parseDescribeStatement() ast.Statement {
	stmt := &ast.DescribeStatement{}

	switch {
	case p.peekTokenIs(token.TABLE):
		p.nextToken()
		stmt.Target = ast.DescribeTable
	case p.peekTokenIs(token.VIEW):
		p.nextToken()
		stmt.Target = ast.DescribeView
	default:
		p.addError(p.peekToken.Pos, "DESCRIBE expects TABLE or VIEW, got %s", p.peekToken.Type)
		return stmt
	}

	if !p.expectPeek(token.IDENT) {
		return stmt
	}
	stmt.Name = p.parseQualifiedName()
	return stmt
}

func (p *Parser) parseShowStatement() ast.Statement {
	switch {
	case p.peekTokenIs(token.TABLES):
		p.nextToken()
		stmt := &ast.ShowTablesStatement{}
		p.nextToken()
		return stmt
	case p.peekTokenIs(token.VIEWS):
		p.nextToken()
		stmt := &ast.ShowViewsStatement{}
		p.nextToken()
		return stmt
	default:
		p.addError(p.peekToken.Pos, "SHOW expects TABLES or VIEWS, got %s", p.peekToken.Type)
		return nil
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {
	p.depth++
	if p.depth > MaxParserDepth {
		p.addError(p.curToken.Pos, "maximum nesting depth exceeded")
		p.depth--
		return nil
	}
	defer func() { p.depth-- }()

	stmt := p.parseSelectCore()
	if stmt == nil {
		return nil
	}
	return p.parseSetOperations(stmt)
}

func (p *Parser) parseSelectCore() *ast.SelectStatement {
	stmt := &ast.SelectStatement{}

	if p.peekTokenIs(token.DISTINCT) {
		p.nextToken()
		stmt.Distinct = true
	}

	p.nextToken()
	stmt.Columns = p.parseSelectList()

	if p.peekTokenIs(token.FROM) {
		p.expectPeek(token.FROM)
		p.nextToken()
		stmt.From = p.parseTableExpression()
	}

	if p.peekTokenIs(token.WHERE) {
		p.expectPeek(token.WHERE)
		p.nextToken()
		stmt.Where = p.parseExpression(lowest)
	}

	if p.peekTokenIs(token.GROUP) {
		p.expectPeek(token.GROUP)
		if p.expectPeek(token.BY) {
			p.nextToken()
			stmt.GroupBy = p.parseExpressionList()
		}
	}

	if p.peekTokenIs(token.HAVING) {
		p.expectPeek(token.HAVING)
		p.nextToken()
		stmt.Having = p.parseExpression(lowest)
	}

	if p.peekTokenIs(token.ORDER) {
		p.expectPeek(token.ORDER)
		if p.expectPeek(token.BY) {
			p.nextToken()
			stmt.OrderBy = p.parseOrderList()
		}
	}

	if p.peekTokenIs(token.LIMIT) {
		p.expectPeek(token.LIMIT)
		p.nextToken()
		limit := &ast.LimitClause{Count: p.parseExpression(lowest)}
		if p.peekTokenIs(token.OFFSET) {
			p.expectPeek(token.OFFSET)
			p.nextToken()
			limit.Offset = p.parseExpression(lowest)
		}
		stmt.Limit = limit
	} else if p.peekTokenIs(token.OFFSET) {
		p.expectPeek(token.OFFSET)
		p.nextToken()
		stmt.Limit = &ast.LimitClause{Offset: p.parseExpression(lowest)}
	}

	return stmt
}

func (p *Parser) parseSetOperations(stmt *ast.SelectStatement) *ast.SelectStatement {
	for {
		op, ok := p.peekSetOperator()
		if !ok {
			return stmt
		}

		p.nextToken()
		operator := op
		all := false
		if p.peekTokenIs(token.ALL) {
			p.nextToken()
			all = true
		}

		var right *ast.SelectStatement
		if p.peekTokenIs(token.LPAREN) {
			p.expectPeek(token.LPAREN)
			p.nextToken()
			switch p.curToken.Type {
			case token.WITH:
				with := p.parseWithClause()
				if !p.curTokenIs(token.SELECT) {
					return stmt
				}
				right = p.parseSelectStatement()
				if right != nil {
					right.With = with
				}
			case token.SELECT:
				right = p.parseSelectStatement()
			default:
				p.addError(p.curToken.Pos, "set operator requires SELECT, got %s", p.curToken.Type)
				return stmt
			}
			if !p.expectPeek(token.RPAREN) {
				return stmt
			}
		} else {
			if !p.expectPeek(token.SELECT) {
				return stmt
			}
			right = p.parseSelectStatement()
		}

		stmt.SetOps = append(stmt.SetOps, ast.SetOperation{Operator: operator, All: all, Select: right})
	}
}

func (p *Parser) peekSetOperator() (ast.SetOperator, bool) {
	switch p.peekToken.Type {
	case token.UNION:
		return ast.SetOpUnion, true
	case token.INTERSECT:
		return ast.SetOpIntersect, true
	case token.EXCEPT:
		return ast.SetOpExcept, true
	default:
		return "", false
	}
}

func (p *Parser) parseSelectList() []ast.SelectItem {
	items := make([]ast.SelectItem, 0)

	for {
		var expr ast.Expr
		switch p.curToken.Type {
		case token.STAR:
			expr = &ast.StarExpr{}
		default:
			expr = p.parseExpression(lowest)
		}

		alias := p.parseAliasIfPresent()
		items = append(items, ast.SelectItem{Expr: expr, Alias: alias})

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
			p.nextToken()
			continue
		}
		break
	}

	return items
}

func (p *Parser) parseOrderList() []ast.OrderItem {
	items := make([]ast.OrderItem, 0)

	for {
		expr := p.parseExpression(lowest)
		direction := ast.Ascending
		if p.peekTokenIs(token.DESC) || p.peekTokenIs(token.ASC) {
			p.nextToken()
			if p.curTokenIs(token.DESC) {
				direction = ast.Descending
			}
		}
		items = append(items, ast.OrderItem{Expr: expr, Direction: direction})
		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
			p.nextToken()
			continue
		}
		break
	}

	return items
}

func (p *Parser) parseExpressionList() []ast.Expr {
	exprs := []ast.Expr{p.parseExpression(lowest)}
	for p.peekTokenIs(token.COMMA) {
		if len(exprs) >= MaxExpressionCount {
			p.addError(p.peekToken.Pos, "maximum expression count exceeded")
			break
		}
		p.nextToken()
		p.nextToken()
		exprs = append(exprs, p.parseExpression(lowest))
	}
	return exprs
}

func (p *Parser) parseWindowSpecification() *ast.WindowSpecification {
	spec := &ast.WindowSpecification{}
	if !p.expectPeek(token.LPAREN) {
		return spec
	}
	if p.peekTokenIs(token.RPAREN) {
		p.nextToken()
		return spec
	}
	p.nextToken()
	parsedPartition := false
	parsedOrder := false
	for !p.curTokenIs(token.RPAREN) && !p.curTokenIs(token.EOF) {
		switch p.curToken.Type {
		case token.PARTITION:
			if parsedPartition {
				p.addError(p.curToken.Pos, "duplicate PARTITION clause in window specification")
				return spec
			}
			parsedPartition = true
			if !p.expectPeek(token.BY) {
				return spec
			}
			p.nextToken()
			expr := p.parseExpression(lowest)
			spec.PartitionBy = append(spec.PartitionBy, expr)
			for p.peekTokenIs(token.COMMA) {
				p.nextToken()
				p.nextToken()
				spec.PartitionBy = append(spec.PartitionBy, p.parseExpression(lowest))
			}
			if p.peekTokenIs(token.RPAREN) {
				p.nextToken()
				return spec
			}
			p.nextToken()
		case token.ORDER:
			if parsedOrder {
				p.addError(p.curToken.Pos, "duplicate ORDER clause in window specification")
				return spec
			}
			parsedOrder = true
			if !p.expectPeek(token.BY) {
				return spec
			}
			p.nextToken()
			spec.OrderBy = p.parseOrderList()
			if p.peekTokenIs(token.RPAREN) {
				p.nextToken()
				return spec
			}
			p.nextToken()
		case token.RPAREN:
			return spec
		default:
			p.addError(p.curToken.Pos, "unexpected token %s in window specification", p.curToken.Type)
			return spec
		}
	}
	return spec
}

func (p *Parser) parseAliasIfPresent() string {
	if p.peekTokenIs(token.AS) {
		p.nextToken()
		if !p.expectPeek(token.IDENT) {
			return ""
		}
		return p.curToken.Literal
	}
	if p.peekTokenIs(token.IDENT) && !isClauseBoundary(p.peekToken.Type) {
		p.nextToken()
		return p.curToken.Literal
	}
	return ""
}

func isClauseBoundary(t token.Type) bool {
	switch t {
	case token.FROM, token.WHERE, token.GROUP, token.BY, token.HAVING, token.ORDER, token.LIMIT, token.OFFSET,
		token.JOIN, token.INNER, token.LEFT, token.RIGHT, token.FULL, token.CROSS,
		token.UNION, token.INTERSECT, token.EXCEPT:
		return true
	default:
		return false
	}
}

func (p *Parser) parseTableExpression() ast.TableExpr {
	left := p.parseTableFactor()

	for {
		joinType, ok := p.peekJoinType()
		if !ok {
			return left
		}

		p.nextToken()
		switch p.curToken.Type {
		case token.JOIN:
			// implicit INNER
		case token.INNER:
			joinType = ast.JoinInner
			p.expectPeek(token.JOIN)
		case token.LEFT:
			joinType = ast.JoinLeft
			if p.peekTokenIs(token.OUTER) {
				p.nextToken()
			}
			p.expectPeek(token.JOIN)
		case token.RIGHT:
			joinType = ast.JoinRight
			if p.peekTokenIs(token.OUTER) {
				p.nextToken()
			}
			p.expectPeek(token.JOIN)
		case token.FULL:
			joinType = ast.JoinFull
			if p.peekTokenIs(token.OUTER) {
				p.nextToken()
			}
			p.expectPeek(token.JOIN)
		case token.CROSS:
			joinType = ast.JoinCross
			p.expectPeek(token.JOIN)
		}

		p.nextToken()
		right := p.parseTableFactor()
		join := &ast.JoinExpr{Left: left, Right: right, Type: joinType}
		if p.peekTokenIs(token.ON) {
			p.expectPeek(token.ON)
			p.nextToken()
			join.Condition.On = p.parseExpression(lowest)
		}
		left = join
	}
}

func (p *Parser) peekJoinType() (ast.JoinType, bool) {
	switch p.peekToken.Type {
	case token.JOIN:
		return ast.JoinInner, true
	case token.INNER:
		return ast.JoinInner, true
	case token.LEFT:
		return ast.JoinLeft, true
	case token.RIGHT:
		return ast.JoinRight, true
	case token.FULL:
		return ast.JoinFull, true
	case token.CROSS:
		return ast.JoinCross, true
	default:
		return "", false
	}
}

func (p *Parser) parseTableFactor() ast.TableExpr {
	switch p.curToken.Type {
	case token.IDENT:
		ident := p.parseQualifiedName()
		tbl := &ast.TableName{Name: ident}
		if alias := p.parseAliasIfPresent(); alias != "" {
			tbl.Alias = alias
		}
		return tbl
	case token.LPAREN:
		p.nextToken()
		switch p.curToken.Type {
		case token.WITH:
			with := p.parseWithClause()
			if !p.curTokenIs(token.SELECT) {
				return nil
			}
			sub := p.parseSelectStatement()
			if sub != nil {
				sub.With = with
			}
			if !p.expectPeek(token.RPAREN) {
				return nil
			}
			alias := ""
			if p.peekTokenIs(token.AS) || (p.peekTokenIs(token.IDENT) && !isClauseBoundary(p.peekToken.Type)) {
				alias = p.parseAliasIfPresent()
			}
			return &ast.SubqueryTable{Select: sub, Alias: alias}
		case token.SELECT:
			sub := p.parseSelectStatement()
			if !p.expectPeek(token.RPAREN) {
				return nil
			}
			alias := ""
			if p.peekTokenIs(token.AS) || (p.peekTokenIs(token.IDENT) && !isClauseBoundary(p.peekToken.Type)) {
				alias = p.parseAliasIfPresent()
			}
			return &ast.SubqueryTable{Select: sub, Alias: alias}
		default:
			nested := p.parseTableExpression()
			if !p.expectPeek(token.RPAREN) {
				return nested
			}
			return nested
		}
	default:
		p.addError(p.curToken.Pos, "unexpected token %s in FROM clause", p.curToken.Type)
		return nil
	}
}

func (p *Parser) parseIdentifier() *ast.Identifier {
	return &ast.Identifier{Parts: []string{p.curToken.Literal}}
}

func (p *Parser) parseQualifiedName() *ast.Identifier {
	parts := []string{p.curToken.Literal}
	for p.peekTokenIs(token.DOT) {
		if p.peekPeekTokenIs(token.STAR) {
			break
		}
		p.nextToken()
		if !p.expectPeek(token.IDENT) {
			return &ast.Identifier{Parts: parts}
		}
		parts = append(parts, p.curToken.Literal)
	}
	return &ast.Identifier{Parts: parts}
}

const (
	_ int = iota
	lowest
	precedenceOr
	precedenceAnd
	precedenceComparison
	precedenceSum
	precedenceProduct
	precedencePrefix
	precedenceCall
)

var precedences = map[token.Type]int{
	token.OR:      precedenceOr,
	token.AND:     precedenceAnd,
	token.NOT:     precedenceComparison,
	token.EQ:      precedenceComparison,
	token.NEQ:     precedenceComparison,
	token.LT:      precedenceComparison,
	token.LTE:     precedenceComparison,
	token.GT:      precedenceComparison,
	token.GTE:     precedenceComparison,
	token.IN:      precedenceComparison,
	token.BETWEEN: precedenceComparison,
	token.LIKE:    precedenceComparison,
	token.IS:      precedenceComparison,
	token.PLUS:    precedenceSum,
	token.MINUS:   precedenceSum,
	token.STAR:    precedenceProduct,
	token.SLASH:   precedenceProduct,
	token.PERCENT: precedenceProduct,
	token.DOT:     precedenceCall,
	token.LPAREN:  precedenceCall,
	token.OVER:    precedenceCall,
}

func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peekToken.Type]; ok {
		return prec
	}
	return lowest
}

func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.curToken.Type]; ok {
		return prec
	}
	return lowest
}

func (p *Parser) parseExpression(precedence int) ast.Expr {
	p.depth++
	if p.depth > MaxParserDepth {
		p.addError(p.curToken.Pos, "expression nesting too deep")
		p.depth--
		return nil
	}
	defer func() { p.depth-- }()

	var left ast.Expr

	switch p.curToken.Type {
	case token.IDENT:
		left = p.parseQualifiedName()
	case token.REPLACE:
		left = &ast.Identifier{Parts: []string{p.curToken.Literal}}
	case token.CASE:
		left = p.parseCaseExpression()
	case token.NUMBER:
		left = &ast.NumericLiteral{Value: p.curToken.Literal}
	case token.STRING:
		left = &ast.StringLiteral{Value: p.curToken.Literal}
	case token.TRUE:
		left = &ast.BooleanLiteral{Value: true}
	case token.FALSE:
		left = &ast.BooleanLiteral{Value: false}
	case token.NULL:
		left = &ast.NullLiteral{}
	case token.PLACEHOLDER:
		left = &ast.Placeholder{Symbol: p.curToken.Literal}
	case token.STAR:
		left = &ast.StarExpr{}
	case token.MINUS:
		p.nextToken()
		expr := p.parseExpression(precedencePrefix)
		left = &ast.UnaryExpr{Operator: "-", Expr: expr}
	case token.NOT:
		p.nextToken()
		expr := p.parseExpression(precedencePrefix)
		left = &ast.UnaryExpr{Operator: "NOT", Expr: expr}
	case token.LPAREN:
		p.nextToken()
		expr := p.parseExpression(lowest)
		if !p.expectPeek(token.RPAREN) {
			return expr
		}
		left = expr
	case token.EXISTS:
		left = p.parseExistsExpression(false)
	default:
		p.addError(p.curToken.Pos, "unexpected token %s", p.curToken.Type)
		return nil
	}

	for !terminatesExpression(p.peekToken.Type) {
		prec := p.peekPrecedence()
		if precedence >= prec {
			break
		}

		p.nextToken()
		left = p.parseInfixExpression(left)
	}

	return left
}

func terminatesExpression(t token.Type) bool {
	switch t {
	case token.SEMICOLON, token.COMMA, token.RPAREN, token.GROUP, token.ORDER, token.LIMIT, token.OFFSET,
		token.HAVING, token.UNION, token.INTERSECT, token.EXCEPT, token.THEN, token.ELSE, token.END:
		return true
	default:
		return false
	}
}

func (p *Parser) parseCaseExpression() ast.Expr {
	caseExpr := &ast.CaseExpr{}

	if !p.peekTokenIs(token.WHEN) {
		p.nextToken()
		caseExpr.Operand = p.parseExpression(lowest)
		if !p.expectPeek(token.WHEN) {
			return caseExpr
		}
	} else {
		p.nextToken()
	}

	for p.curTokenIs(token.WHEN) {
		p.nextToken()
		cond := p.parseExpression(lowest)
		if !p.expectPeek(token.THEN) {
			return caseExpr
		}
		p.nextToken()
		result := p.parseExpression(lowest)
		caseExpr.When = append(caseExpr.When, ast.WhenClause{
			Condition: cond,
			Result:    result,
		})
		if p.peekTokenIs(token.WHEN) {
			p.nextToken()
		} else {
			break
		}
	}

	if p.peekTokenIs(token.ELSE) {
		p.nextToken()
		p.nextToken()
		caseExpr.Else = p.parseExpression(lowest)
	}

	p.expectPeek(token.END)
	return caseExpr
}

func (p *Parser) parseExistsExpression(negate bool) ast.Expr {
	if !p.expectPeek(token.LPAREN) {
		return nil
	}
	p.nextToken()
	sub := p.parseSelectStatement()
	if !p.expectPeek(token.RPAREN) {
		return nil
	}
	return &ast.ExistsExpr{Not: negate, Subquery: sub}
}

func (p *Parser) parseInfixExpression(left ast.Expr) ast.Expr {
	switch p.curToken.Type {
	case token.PLUS, token.MINUS, token.STAR, token.SLASH, token.PERCENT,
		token.EQ, token.NEQ, token.LT, token.LTE, token.GT, token.GTE,
		token.AND, token.OR:
		operator := strings.ToUpper(p.curToken.Literal)
		precedence := p.curPrecedence()
		p.nextToken()
		right := p.parseExpression(precedence)
		return &ast.BinaryExpr{Left: left, Operator: operator, Right: right}
	case token.IN:
		return p.parseInExpression(left, false)
	case token.LIKE:
		return p.parseLikeExpression(left, false)
	case token.BETWEEN:
		return p.parseBetweenExpression(left, false)
	case token.IS:
		return p.parseIsNullExpression(left)
	case token.NOT:
		switch {
		case p.peekTokenIs(token.IN):
			p.nextToken()
			return p.parseInExpression(left, true)
		case p.peekTokenIs(token.LIKE):
			p.nextToken()
			return p.parseLikeExpression(left, true)
		case p.peekTokenIs(token.BETWEEN):
			p.nextToken()
			return p.parseBetweenExpression(left, true)
		case p.peekTokenIs(token.EXISTS):
			p.nextToken()
			return p.parseExistsExpression(true)
		default:
			operator := "NOT"
			precedence := p.curPrecedence()
			p.nextToken()
			right := p.parseExpression(precedence)
			return &ast.BinaryExpr{Left: left, Operator: operator, Right: right}
		}
	case token.LPAREN:
		ident, ok := left.(*ast.Identifier)
		if !ok {
			return left
		}
		call := &ast.FuncCall{Name: *ident}
		if p.peekTokenIs(token.RPAREN) {
			p.expectPeek(token.RPAREN)
			return call
		}
		p.nextToken()
		if p.curTokenIs(token.DISTINCT) {
			call.Distinct = true
			p.nextToken()
		}
		call.Args = append(call.Args, p.parseExpression(lowest))
		for p.peekTokenIs(token.COMMA) {
			p.nextToken()
			p.nextToken()
			call.Args = append(call.Args, p.parseExpression(lowest))
		}
		p.expectPeek(token.RPAREN)
		return call
	case token.OVER:
		call, ok := left.(*ast.FuncCall)
		if !ok {
			p.addError(p.curToken.Pos, "OVER requires preceding function call")
			return left
		}
		call.Over = p.parseWindowSpecification()
		return call
	case token.DOT:
		ident, ok := left.(*ast.Identifier)
		if !ok {
			return left
		}
		p.nextToken()
		if p.curTokenIs(token.STAR) {
			return &ast.StarExpr{Table: ident}
		}
		if !p.curTokenIs(token.IDENT) {
			p.addError(p.curToken.Pos, "expected identifier after '.', got %s", p.curToken.Type)
			return left
		}
		parts := append(append([]string{}, ident.Parts...), p.curToken.Literal)
		return &ast.Identifier{Parts: parts}
	default:
		return left
	}
}

func (p *Parser) parseInExpression(left ast.Expr, not bool) ast.Expr {
	expr := &ast.InExpr{Expr: left, Not: not}
	if !p.expectPeek(token.LPAREN) {
		return expr
	}
	p.nextToken()
	if p.curTokenIs(token.SELECT) {
		expr.Subquery = p.parseSelectStatement()
		if !p.expectPeek(token.RPAREN) {
			return expr
		}
		return expr
	}
	expr.List = append(expr.List, p.parseExpression(lowest))
	for p.peekTokenIs(token.COMMA) {
		p.nextToken()
		p.nextToken()
		expr.List = append(expr.List, p.parseExpression(lowest))
	}
	p.expectPeek(token.RPAREN)
	return expr
}

func (p *Parser) parseLikeExpression(left ast.Expr, not bool) ast.Expr {
	p.nextToken()
	pattern := p.parseExpression(precedenceComparison)
	return &ast.LikeExpr{Expr: left, Not: not, Pattern: pattern}
}

func (p *Parser) parseBetweenExpression(left ast.Expr, not bool) ast.Expr {
	between := &ast.BetweenExpr{Expr: left, Not: not}
	p.nextToken()
	between.Lower = p.parseExpression(precedenceComparison)
	if !p.expectPeek(token.AND) {
		return between
	}
	p.nextToken()
	between.Upper = p.parseExpression(precedenceComparison)
	return between
}

func (p *Parser) parseIsNullExpression(left ast.Expr) ast.Expr {
	not := false
	if p.peekTokenIs(token.NOT) {
		p.nextToken()
		not = true
	}
	if !p.expectPeek(token.NULL) {
		return left
	}
	return &ast.IsNullExpr{Expr: left, Not: not}
}

func (p *Parser) parseInsertStatement() *ast.InsertStatement {
	stmt := &ast.InsertStatement{}
	if !p.expectPeek(token.INTO) {
		return stmt
	}
	p.nextToken()
	table := p.parseTableFactor()
	if tbl, ok := table.(*ast.TableName); ok {
		stmt.Table = tbl
	}
	if p.peekTokenIs(token.LPAREN) {
		p.nextToken()
		p.nextToken()
		stmt.Columns = append(stmt.Columns, p.parseQualifiedName())
		for p.peekTokenIs(token.COMMA) {
			p.nextToken()
			p.nextToken()
			stmt.Columns = append(stmt.Columns, p.parseQualifiedName())
		}
		p.expectPeek(token.RPAREN)
	}
	if p.peekTokenIs(token.VALUES) {
		p.expectPeek(token.VALUES)
		for p.expectPeek(token.LPAREN) {
			p.nextToken()
			row := []ast.Expr{p.parseExpression(lowest)}
			for p.peekTokenIs(token.COMMA) {
				p.nextToken()
				p.nextToken()
				row = append(row, p.parseExpression(lowest))
			}
			stmt.Rows = append(stmt.Rows, row)
			if !p.expectPeek(token.RPAREN) {
				break
			}
			if p.peekTokenIs(token.COMMA) {
				p.nextToken()
				continue
			}
			break
		}
	} else if p.peekTokenIs(token.SELECT) {
		p.nextToken()
		stmt.Select = p.parseSelectStatement()
	}
	return stmt
}

func (p *Parser) parseUpdateStatement() *ast.UpdateStatement {
	stmt := &ast.UpdateStatement{}
	p.nextToken()
	stmt.Table = p.parseTableExpression()
	if !p.expectPeek(token.SET) {
		return stmt
	}
	p.nextToken()
	stmt.Assignments = append(stmt.Assignments, p.parseAssignment())
	for p.peekTokenIs(token.COMMA) {
		p.nextToken()
		p.nextToken()
		stmt.Assignments = append(stmt.Assignments, p.parseAssignment())
	}
	if p.peekTokenIs(token.WHERE) {
		p.expectPeek(token.WHERE)
		p.nextToken()
		stmt.Where = p.parseExpression(lowest)
	}
	return stmt
}

func (p *Parser) parseAssignment() ast.Assignment {
	name := p.parseQualifiedName()
	if !p.expectPeek(token.EQ) {
		return ast.Assignment{Column: name}
	}
	p.nextToken()
	value := p.parseExpression(lowest)
	return ast.Assignment{Column: name, Value: value}
}

func (p *Parser) parseDeleteStatement() *ast.DeleteStatement {
	stmt := &ast.DeleteStatement{}
	if !p.expectPeek(token.FROM) {
		return stmt
	}
	p.nextToken()
	stmt.Table = p.parseTableExpression()
	if p.peekTokenIs(token.WHERE) {
		p.expectPeek(token.WHERE)
		p.nextToken()
		stmt.Where = p.parseExpression(lowest)
	}
	return stmt
}

func (p *Parser) parseCreateViewStatement() *ast.CreateViewStatement {
	stmt := &ast.CreateViewStatement{}

	if p.peekTokenIs(token.OR) {
		p.expectPeek(token.OR)
		if !p.expectPeek(token.REPLACE) {
			return stmt
		}
		stmt.OrReplace = true
	}

	if p.peekTokenIs(token.MATERIALIZED) {
		p.expectPeek(token.MATERIALIZED)
		stmt.Materialized = true
	}

	if !p.expectPeek(token.VIEW) {
		return stmt
	}

	if p.peekTokenIs(token.IF) {
		p.expectPeek(token.IF)
		if !p.expectPeek(token.NOT) {
			return stmt
		}
		if !p.expectPeek(token.EXISTS) {
			return stmt
		}
		stmt.IfNotExists = true
	}

	if !p.expectPeek(token.IDENT) {
		return stmt
	}
	stmt.Name = p.parseQualifiedName()

	if p.peekTokenIs(token.LPAREN) {
		p.expectPeek(token.LPAREN)
		if !p.expectPeek(token.IDENT) {
			return stmt
		}
		stmt.Columns = append(stmt.Columns, p.parseQualifiedName())
		for p.peekTokenIs(token.COMMA) {
			p.nextToken()
			if !p.expectPeek(token.IDENT) {
				return stmt
			}
			stmt.Columns = append(stmt.Columns, p.parseQualifiedName())
		}
		if !p.expectPeek(token.RPAREN) {
			return stmt
		}
	}

	if !p.expectPeek(token.AS) {
		return stmt
	}

	p.nextToken()
	switch p.curToken.Type {
	case token.WITH:
		withClause := p.parseWithClause()
		if !p.curTokenIs(token.SELECT) {
			p.addError(p.curToken.Pos, "CREATE VIEW requires SELECT after WITH, got %s", p.curToken.Type)
			return stmt
		}
		stmt.Select = p.parseSelectStatement()
		if stmt.Select != nil {
			stmt.Select.With = withClause
		}
	case token.SELECT:
		stmt.Select = p.parseSelectStatement()
	default:
		p.addError(p.curToken.Pos, "CREATE VIEW requires SELECT, got %s", p.curToken.Type)
	}

	return stmt
}

func (p *Parser) parseDropViewStatement() *ast.DropViewStatement {
	stmt := &ast.DropViewStatement{}

	if p.peekTokenIs(token.MATERIALIZED) {
		if !p.expectPeek(token.MATERIALIZED) {
			return stmt
		}
		stmt.Materialized = true
	}

	if !p.expectPeek(token.VIEW) {
		return stmt
	}

	if p.peekTokenIs(token.IF) {
		if !p.expectPeek(token.IF) {
			return stmt
		}
		if !p.expectPeek(token.EXISTS) {
			return stmt
		}
		stmt.IfExists = true
	}

	if !p.expectPeek(token.IDENT) {
		return stmt
	}
	stmt.Name = p.parseQualifiedName()

	return stmt
}
