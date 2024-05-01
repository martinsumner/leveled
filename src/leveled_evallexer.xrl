%% Lexer for eval expressions

Definitions.
WhiteSpace  = ([\t\f\v\r\n\s]+)

Rules.

{WhiteSpace} : skip_token.

\(      : {token, {'(', TokenLine}}.
\)      : {token, {')', TokenLine}}.
,       : {token, {',', TokenLine}}.
\|      : {token, {'PIPE', TokenLine}}.

delim        : {token, {delim, TokenLine}}.
join         : {token, {join, TokenLine}}.
split        : {token, {split, TokenLine}}.
slice        : {token, {slice, TokenLine}}.
index        : {token, {index, TokenLine}}.
kvsplit      : {token, {kvsplit, TokenLine}}.
regex        : {token, {regex, TokenLine}}.
to_integer   : {token, {to_integer, TokenLine}}.
to_string    : {token, {to_string, TokenLine}}.
add          : {token, {add, TokenLine}}.
subtract     : {token, {subtract, TokenLine}}.
map          : {token, {map, TokenLine}}.
pcre         : {token, {pcre, TokenLine}}.
re2          : {token, {re2, TokenLine}}.

=       : {token, {comparator, '=', TokenLine}}.
<       : {token, {comparator, '<', TokenLine}}.
>       : {token, {comparator, '>', TokenLine}}.
<>      : {token, {comparator, '<>', TokenLine}}.
<=      : {token, {comparator, '<=', TokenLine}}.
>=      : {token, {comparator, '>=', TokenLine}}.

\$[a-zA-Z_][a-zA-Z_0-9]* : {token, {identifier, TokenLine, strip_identifier(TokenChars)}}.
\:[a-zA-Z_][a-zA-Z_0-9]* : {token, {substitution, TokenLine, strip_substitution(TokenChars)}}.
\"[^"]*\"                : {token, {string, TokenLine, strip_string(TokenChars)}}. %"
[0-9]+                   : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.

Erlang code.

strip_string(TokenChars) ->
    list_to_binary(lists:sublist(TokenChars, 2, length(TokenChars) -2)).

strip_identifier(TokenChars) ->
    [36|StrippedChars] = TokenChars,
    list_to_binary(StrippedChars).

strip_substitution(TokenChars) ->
    [58|StrippedChars] = TokenChars,
    list_to_binary(StrippedChars).