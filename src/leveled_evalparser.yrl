%% Grammar for eval expressions

Nonterminals
top_level eval
operand math_operand
integer non_neg_integer
regex_method
mapping mappings mappings_list
identifiers identifier_list.

Terminals
'(' ')' ','
identifier string 
pos_integer neg_integer zero
comparator
'PIPE'
delim join split slice index kvsplit regex map
add subtract
to_integer to_string
pcre.

Rootsymbol top_level.

top_level -> eval: {eval, '$1'}.

eval -> eval 'PIPE' eval                                                                     : {'PIPE', '$1', 'INTO', '$3'}.
eval -> delim '(' identifier ',' string ',' identifier_list ')'                              : {delim, '$3', '$5', '$7'}.
eval -> join '(' identifier_list ',' string ',' identifier ')'                               : {join, '$3', '$5', '$7'}.
eval -> split '(' identifier ',' string ',' identifier ')'                                   : {split, '$3', '$5', '$7'}.
eval -> slice '(' identifier ',' pos_integer ',' identifier ')'                              : {slice, '$3', '$5', '$7'}.
eval -> index '(' identifier ',' non_neg_integer ',' pos_integer ',' 'identifier' ')'        : {index, '$3', '$5', '$7', '$9'}.
eval -> kvsplit '(' identifier ',' string ',' string ')'                                     : {kvsplit, '$3', '$5', '$7'}.
eval -> regex '(' identifier ',' string ',' regex_method ',' identifier_list ')'             : {regex, '$3', re_compile('$5', '$7'), '$9'}.
eval -> regex '(' identifier ',' string ',' identifier_list ')'                              : {regex, '$3', re_compile('$5'), '$7'}.
eval -> map '(' identifier ',' comparator ',' mappings_list ',' operand ',' identifier ')'   : {map, '$3', '$5', '$7', '$9', '$11'}.
eval -> to_integer '(' identifier ',' identifier ')'                                         : {to_integer, '$3', '$5'}.
eval -> to_string '(' identifier ',' identifier ')'                                          : {to_string, '$3', '$5'}.
eval -> subtract '(' math_operand ',' math_operand ',' identifier ')'                        : {subtract, '$3', '$5', '$7'}.
eval -> add '(' math_operand ',' math_operand ',' identifier ')'                             : {add, '$3', '$5', '$7'}.

mappings_list -> '(' mappings ')'         : '$2'.

mappings -> mapping ',' mappings          : ['$1' | '$3'].
mappings -> mapping                       : ['$1'].

mapping -> '(' operand ',' operand ')' : {mapping, '$2', '$4'}.

non_neg_integer -> pos_integer  : '$1'.
non_neg_integer -> zero         : '$1'.    

integer -> non_neg_integer      : '$1'.
integer -> neg_integer          : '$1'.

operand -> string               : '$1'.
operand -> integer              : '$1'.

math_operand -> integer         : '$1'.
math_operand -> identifier      : '$1'.

regex_method -> pcre       : '$1'.

identifier_list -> '(' identifiers ')'    : strip_ids('$2').

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

re_compile(RegexStr) ->
    re_compile(RegexStr, {pcre, element(2, RegexStr)}).

re_compile({string, _LN, Regex}, Method) ->
    {ok, CRE} = leveled_util:regex_compile(Regex, element(1, Method)),
    CRE.