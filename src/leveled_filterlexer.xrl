%% Lexer for filter and conditional expressions
%% Author: Thomas Arts

Definitions.
WhiteSpace  = ([\t\f\v\r\n\s]+)

Rules.

{WhiteSpace} : skip_token.

\( : {token, {'(', TokenLine}}.
\) : {token, {')', TokenLine}}.

,       : {token, {',', TokenLine}}.
NOT     : {token, {'NOT', TokenLine}}.
AND     : {token, {'AND', TokenLine}}.
OR      : {token, {'OR', TokenLine}}.
BETWEEN : {token, {'BETWEEN', TokenLine}}.
IN      : {token, {'IN', TokenLine}}.
=       : {token, {comparator, '=', TokenLine}}.
<       : {token, {comparator, '<', TokenLine}}.
>       : {token, {comparator, '>', TokenLine}}.
<>      : {token, {comparator, '<>', TokenLine}}.
<=      : {token, {comparator, '<=', TokenLine}}.
>=      : {token, {comparator, '>=', TokenLine}}.

contains             : {token, {contains, TokenLine}}.
begins_with          : {token, {begins_with, TokenLine}}.
attribute_exists     : {token, {attribute_exists, TokenLine}}.
attribute_not_exists : {token, {attribute_not_exists, TokenLine}}.
attribute_empty      : {token, {attribute_empty, TokenLine}}.

\$[a-zA-Z_][a-zA-Z_0-9]* : {token, {identifier, TokenLine, strip_identifier(TokenChars)}}.
\:[a-zA-Z_][a-zA-Z_0-9]* : {token, {substitution, TokenLine, strip_substitution(TokenChars)}}.
\-[0-9]+                 : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
[0-9]+                   : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
\"[^"]*\"                : {token, {string, TokenLine, strip_string(TokenChars)}}. %"

Erlang code.

strip_string(TokenChars) ->
    unicode:characters_to_binary(lists:droplast(tl(TokenChars))).

strip_identifier(TokenChars) ->
    [36|StrippedChars] = TokenChars,
    unicode:characters_to_binary(StrippedChars).

strip_substitution(TokenChars) ->
    [58|StrippedChars] = TokenChars,
    unicode:characters_to_binary(StrippedChars).
