// The one statement the connector adds to Spark SQL:
//   CALL milvus.system.<name>( positional, name => constant, `option.key` => constant )
// Design: docs/design/architecture/procedure.html. Values are constants only,
// so this grammar does not depend on Spark's expression grammar and one file
// serves every Spark line (decision 17).
grammar MilvusCall;

statement
    : CALL procedureName '(' (argument (',' argument)*)? ')' EOF
    ;

procedureName
    : identifier '.' identifier '.' identifier
    ;

argument
    : constant                      # positionalArgument
    | identifier '=>' constant      # namedArgument
    ;

constant
    : STRING                        # stringConstant
    | INTEGER                       # integerConstant
    | DECIMAL                       # decimalConstant
    | (TRUE | FALSE)                # booleanConstant
    | NULL                          # nullConstant
    ;

identifier
    : IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    ;

CALL  : [Cc][Aa][Ll][Ll];
TRUE  : [Tt][Rr][Uu][Ee];
FALSE : [Ff][Aa][Ll][Ss][Ee];
NULL  : [Nn][Uu][Ll][Ll];

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) | '\'\'' )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER
    : '-'? DIGIT+
    ;

DECIMAL
    : '-'? DIGIT+ '.' DIGIT*
    | '-'? '.' DIGIT+
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT  : [0-9];
fragment LETTER : [A-Za-z];

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
