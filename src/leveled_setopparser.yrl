%% Grammar for key set operations

Nonterminals
top_level condition.


Terminals
'(' ')' set_id
'UNION' 'INTERSECT' 'SUBTRACT'.


Rootsymbol top_level.

top_level -> condition: {setop, '$1'}.

condition -> condition 'UNION' condition         : {'UNION', '$1', '$3'}.
condition -> condition 'INTERSECT' condition     : {'INTERSECT', '$1', '$3'}.
condition -> condition 'SUBTRACT' condition      : {'SUBTRACT', '$1', '$3'}.
condition -> '(' condition ')'                   : '$2'.
condition -> set_id                              : '$1'.

Endsymbol '$end'.

Right 200 'SUBTRACT'.
Left 150 'INTERSECT'.
Left 100 'UNION'.

Erlang code.
