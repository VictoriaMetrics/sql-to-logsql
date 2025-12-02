package token

// Type identifies the lexical class of a token.
type Type string

// Position points to a location in the source SQL (1-based indices).
type Position struct {
	Line   int
	Column int
}

// Token holds the type, literal representation, and source location.
type Token struct {
	Type    Type
	Literal string
	Pos     Position
}

// Token types supported by the SQL parser.
const (
	ILLEGAL Type = "ILLEGAL"
	EOF     Type = "EOF"

	IDENT       Type = "IDENT"
	NUMBER      Type = "NUMBER"
	STRING      Type = "STRING"
	PLACEHOLDER Type = "PLACEHOLDER"

	COMMA     Type = ","
	SEMICOLON Type = ";"
	LPAREN    Type = "("
	RPAREN    Type = ")"
	LBRACKET  Type = "["
	RBRACKET  Type = "]"
	DOT       Type = "."
	STAR      Type = "*"
	PLUS      Type = "+"
	MINUS     Type = "-"
	SLASH     Type = "/"
	PERCENT   Type = "%"
	CARET     Type = "^"
	EQ        Type = "="
	NEQ       Type = "NEQ"
	LT        Type = "<"
	LTE       Type = "<="
	GT        Type = ">"
	GTE       Type = ">="

	// Keywords
	SELECT       Type = "SELECT"
	CREATE       Type = "CREATE"
	DROP         Type = "DROP"
	SHOW         Type = "SHOW"
	VIEW         Type = "VIEW"
	VIEWS        Type = "VIEWS"
	TABLE        Type = "TABLE"
	TABLES       Type = "TABLES"
	DESCRIBE     Type = "DESCRIBE"
	REPLACE      Type = "REPLACE"
	MATERIALIZED Type = "MATERIALIZED"
	INSERT       Type = "INSERT"
	UPDATE       Type = "UPDATE"
	DELETE       Type = "DELETE"
	CASE         Type = "CASE"
	INTO         Type = "INTO"
	VALUES       Type = "VALUES"
	SET          Type = "SET"
	FROM         Type = "FROM"
	WHERE        Type = "WHERE"
	GROUP        Type = "GROUP"
	BY           Type = "BY"
	HAVING       Type = "HAVING"
	ORDER        Type = "ORDER"
	LIMIT        Type = "LIMIT"
	OFFSET       Type = "OFFSET"
	AS           Type = "AS"
	WHEN         Type = "WHEN"
	THEN         Type = "THEN"
	ELSE         Type = "ELSE"
	END          Type = "END"
	IF           Type = "IF"
	DISTINCT     Type = "DISTINCT"
	WITH         Type = "WITH"
	RECURSIVE    Type = "RECURSIVE"
	OVER         Type = "OVER"
	PARTITION    Type = "PARTITION"

	JOIN  Type = "JOIN"
	INNER Type = "INNER"
	LEFT  Type = "LEFT"
	RIGHT Type = "RIGHT"
	FULL  Type = "FULL"
	OUTER Type = "OUTER"
	CROSS Type = "CROSS"
	ON    Type = "ON"

	AND     Type = "AND"
	OR      Type = "OR"
	NOT     Type = "NOT"
	NULL    Type = "NULL"
	TRUE    Type = "TRUE"
	FALSE   Type = "FALSE"
	IN      Type = "IN"
	EXISTS  Type = "EXISTS"
	BETWEEN Type = "BETWEEN"
	LIKE    Type = "LIKE"
	IS      Type = "IS"
	DESC    Type = "DESC"
	ASC     Type = "ASC"

	UNION     Type = "UNION"
	INTERSECT Type = "INTERSECT"
	EXCEPT    Type = "EXCEPT"
	ALL       Type = "ALL"
)

var keywords = map[string]Type{
	"SELECT":       SELECT,
	"CREATE":       CREATE,
	"DROP":         DROP,
	"SHOW":         SHOW,
	"VIEW":         VIEW,
	"VIEWS":        VIEWS,
	"TABLE":        TABLE,
	"TABLES":       TABLES,
	"DESCRIBE":     DESCRIBE,
	"REPLACE":      REPLACE,
	"MATERIALIZED": MATERIALIZED,
	"INSERT":       INSERT,
	"UPDATE":       UPDATE,
	"DELETE":       DELETE,
	"CASE":         CASE,
	"INTO":         INTO,
	"VALUES":       VALUES,
	"SET":          SET,
	"FROM":         FROM,
	"WHERE":        WHERE,
	"GROUP":        GROUP,
	"BY":           BY,
	"HAVING":       HAVING,
	"ORDER":        ORDER,
	"LIMIT":        LIMIT,
	"OFFSET":       OFFSET,
	"AS":           AS,
	"WHEN":         WHEN,
	"THEN":         THEN,
	"ELSE":         ELSE,
	"END":          END,
	"IF":           IF,
	"DISTINCT":     DISTINCT,
	"WITH":         WITH,
	"RECURSIVE":    RECURSIVE,
	"OVER":         OVER,
	"PARTITION":    PARTITION,
	"JOIN":         JOIN,
	"INNER":        INNER,
	"LEFT":         LEFT,
	"RIGHT":        RIGHT,
	"FULL":         FULL,
	"OUTER":        OUTER,
	"CROSS":        CROSS,
	"ON":           ON,
	"AND":          AND,
	"OR":           OR,
	"NOT":          NOT,
	"NULL":         NULL,
	"TRUE":         TRUE,
	"FALSE":        FALSE,
	"IN":           IN,
	"EXISTS":       EXISTS,
	"BETWEEN":      BETWEEN,
	"LIKE":         LIKE,
	"IS":           IS,
	"DESC":         DESC,
	"ASC":          ASC,
	"UNION":        UNION,
	"INTERSECT":    INTERSECT,
	"EXCEPT":       EXCEPT,
	"ALL":          ALL,
}

// Lookup returns the keyword token if the identifier matches a reserved word.
func Lookup(ident string) Type {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}
