Definitions.
WhiteSpace  = ([\t\f\v\r\n\s]+)

Rules.

{WhiteSpace} : skip_token.

\(               : {token, {'(', TokenLine}}.
\)               : {token, {')', TokenLine}}.

UNION            : {token, {'UNION', TokenLine}}.
INTERSECT        : {token, {'INTERSECT', TokenLine}}.
SUBTRACT         : {token, {'SUBTRACT', TokenLine}}.

\$[1-9][0-9]*    : {token, {set_id, TokenLine, strip_identifier(TokenChars)}}.

Erlang code.

strip_identifier(TokenChars) ->
    [36|StrippedChars] = TokenChars,
    list_to_integer(StrippedChars).