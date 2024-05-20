%% Grammar for filter expressions
%% Author: Thomas Arts

Nonterminals
top_level condition operand str_list strings.


Terminals
'(' ')' comparator identifier string integer
','
'NOT' 'AND' 'OR' 'IN' 'BETWEEN'
contains begins_with
attribute_exists attribute_not_exists attribute_empty.


Rootsymbol top_level.

top_level -> condition: {condition, '$1'}.

condition -> operand comparator operand                   : {'$2', '$1', '$3'}.
condition -> operand 'BETWEEN' operand 'AND' operand      : {'BETWEEN', '$1', '$3', '$5'}.
condition -> identifier 'IN' str_list                     : {'IN', '$1', '$3'}.
condition -> string 'IN' identifier                       : {'IN', '$1', '$3'}.  

condition -> contains '(' identifier ',' string ')'       : {contains, '$3', '$5'}.
condition -> begins_with '(' identifier ',' string ')'    : {begins_with, '$3', '$5'}.
condition -> attribute_exists '(' identifier ')'          : {attribute_exists, '$3'}.
condition -> attribute_not_exists '(' identifier ')'      : {attribute_not_exists, '$3'}.
condition -> attribute_empty '(' identifier ')'           : {attribute_empty, '$3'}.

condition -> condition 'AND' condition                    : {'AND', '$1', '$3'}.
condition -> condition 'OR' condition                     : {'OR', '$1', '$3'}.
condition -> 'NOT' condition                              : {'NOT', '$2'}.
condition -> '(' condition ')'             : '$2'.

operand -> identifier       : '$1'.
operand -> integer          : '$1'.
operand -> string           : '$1'.

str_list -> '(' strings ')' : '$2'.

strings -> string ',' strings : ['$1' | '$3'].
strings -> string             : ['$1'].

Endsymbol '$end'.

Right 200 'NOT'.
Nonassoc 200 comparator.
Left 150 'AND'.
Left 100 'OR'.

Erlang code.
