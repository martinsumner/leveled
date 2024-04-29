%% Grammar for eval expressions

Nonterminals
top_level eval identifiers identifier_list.

Terminals
'(' ')' ','
identifier string integer
'PIPE'
delim join split slice index to_integer.

Rootsymbol top_level.

top_level -> eval: {eval, '$1'}.

eval -> eval 'PIPE' eval                                                     : {'PIPE', '$1', 'INTO', '$3'}.
eval -> delim '(' identifier ',' string ',' identifier_list ')'              : {delim, '$3', '$5', '$7'}.
eval -> join '(' identifier_list ',' string ',' identifier ')'               : {join, '$3', '$5', '$7'}.
eval -> split '(' identifier ',' string ',' identifier ')'                   : {split, '$3', '$5', '$7'}.
eval -> slice '(' identifier ',' integer ',' identifier ')'                  : {slice, '$3', '$5', '$7'}.
eval -> index '(' identifier ',' integer ',' integer ',' 'identifier' ')'    : {index, '$3', '$5', '$7', '$9'}.
eval -> to_integer '(' identifier ',' 'identifier' ')'                       : {to_integer, '$3', '$5'}.

identifier_list -> '(' identifiers ')' : strip_ids('$2').

identifiers -> identifier ',' identifiers : ['$1' | '$3'].
identifiers -> identifier                 : ['$1'].

Endsymbol '$end'.

Right 100 'PIPE'.

Erlang code.

strip_ids(IDL) ->
    lists:map(
        fun(ID) -> element(3, ID) end,
        lists:flatten(IDL)
    ). 