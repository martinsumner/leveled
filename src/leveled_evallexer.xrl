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

=       : {token, {comparator, '=', TokenLine}}.
<       : {token, {comparator, '<', TokenLine}}.
>       : {token, {comparator, '>', TokenLine}}.
<=      : {token, {comparator, '<=', TokenLine}}.
>=      : {token, {comparator, '>=', TokenLine}}.

\$[a-zA-Z_][a-zA-Z_0-9]* : {token, {identifier, TokenLine, strip_identifier(TokenChars)}}.
\:[a-zA-Z_][a-zA-Z_0-9]* : {token, {substitution, TokenLine, strip_substitution(TokenChars)}}.
[1-9][0-9]*              : {token, {pos_integer, TokenLine, list_to_integer(TokenChars)}}.
0                        : {token, {zero, TokenLine, list_to_integer(TokenChars)}}.
\-[0-9]+                 : {token, {neg_integer, TokenLine, list_to_integer(TokenChars)}}.
\"[^"]+\"                : {token, {string, TokenLine, strip_string(TokenChars)}}. %"

Erlang code.

strip_string(TokenChars) ->
    unicode:characters_to_binary(lists:droplast(tl(TokenChars))).

strip_identifier(TokenChars) ->
    [36|StrippedChars] = TokenChars,
    unicode:characters_to_binary(StrippedChars).

strip_substitution(TokenChars) ->
    [58|StrippedChars] = TokenChars,
    unicode:characters_to_binary(StrippedChars).

