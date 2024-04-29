%% Lexer for eval expressions

Definitions.
WhiteSpace  = ([\t\f\v\r\n\s]+)

Rules.

{WhiteSpace} : skip_token.

\( : {token, {'(', TokenLine}}.
\) : {token, {')', TokenLine}}.
,  : {token, {',', TokenLine}}.
\| : {token, {'PIPE', TokenLine}}.

delim       : {token, {delim, TokenLine}}.
join        : {token, {join, TokenLine}}.
split       : {token, {split, TokenLine}}.
slice       : {token, {slice, TokenLine}}.
index       : {token, {index, TokenLine}}.
to_integer  : {token, {to_integer, TokenLine}}.

\$[a-zA-Z_][a-zA-Z_0-9]* : {token, {identifier, TokenLine, strip_identifier(TokenChars)}}.
\"[^"]*\"                : {token, {string, TokenLine, strip_string(TokenChars)}}. %"
[0-9]+                   : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.

Erlang code.

strip_string(TokenChars) ->
    list_to_binary(lists:sublist(TokenChars, 2, length(TokenChars) -2)).

strip_identifier(TokenChars) ->
    [36|StrippedChars] = TokenChars,
    list_to_binary(StrippedChars).