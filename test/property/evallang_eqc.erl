-module(evallang_eqc).

-ifdef(EQC).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_prop1_test_() ->
  {timeout,
      ?EQC_TIME_BUDGET + 10,
      ?_assertEqual(
          true,
          eqc:quickcheck(
              eqc:testing_time(?EQC_TIME_BUDGET, ?QC_OUT(prop_lang()))))}.

eqc_prop2_test_() ->
  {timeout,
      ?EQC_TIME_BUDGET + 10,
      ?_assertEqual(
          true,
          eqc:quickcheck(
              eqc:testing_time(?EQC_TIME_BUDGET, ?QC_OUT(prop_negative()))))}.

identifier() ->
  FirstChars = lists:seq($a,$z)++lists:seq($A,$Z)++["_"],
  OtherChars = FirstChars++lists:seq($0,$9),
  ?LET({X, Xs}, {oneof(FirstChars), list(elements(OtherChars))}, unicode:characters_to_binary([X|Xs])).

identifier(Context, Type) ->
  ?LET(TypedVars, vars(Context, Type),
    ?LET(V, oneof([identifier() || TypedVars == []] ++ TypedVars), [ws(), "$", V, ws()])).

vars(Context, Type) ->
  fault([ V || {V, {T, _}} <- Context, T /= Type ],
        [ V || {V, {T, _}} <- Context, T == Type ]).

%% No quotes in strings
%% Filter the quote with `re` instead of string:find to
%% be compatible with lexer
string() ->
  ?SUCHTHAT(String, non_empty(utf8()), re:run(String, "\"") == nomatch).

typed_context() ->
  ?SUCHTHAT(KVs, list({identifier(), oneof([{int, int()}, {string, string()}])}),
                 unique([K || {K, _} <- KVs])).

unique(Elems) ->
  lists:usort(Elems) == lists:sort(Elems).

ppvalue(string) ->
  ppstring();
ppvalue(Int) ->
  ppint(Int).

ppregex() ->
  [ws(), "\"", regex(), "\"", ws()].

regex() ->
  elements(["a", ".*", "[^0]*"]).

ws() ->
  ?SHRINK(list(elements(" \t\f\v\r\n\s")), " ").

comparator() ->
  oneof([">", "<", "=", "<=", ">="]).

ppint(Kind) ->
  Gen = case Kind of
            pos -> ?LET(N, nat(), N+1);
            nat -> nat();
            neg -> ?LET(N, nat(), -N);
            _ -> int()
        end,
  [ws(), ?LET(X, fault(int(), Gen), integer_to_list(X)), ws()].

ppstring() ->
  [ws(), "\"", string(), "\"", ws()].

operand(_Context) ->
  oneof([ ppint(any), ppstring() ]).

math_operand(Context) ->
  oneof([ identifier(Context, int) || Context /= []] ++
        [  ppint(any) ]).

pplist(Gen) ->
  ?LET(List, non_empty(list(Gen)),
       [ws(), "("] ++ lists:join(",", List) ++ [")", ws()]).


identifier_list(Context, Type) ->
  pplist(identifier(Context, Type)).

mapping(int, string) ->
  [ ws(), "(", ppint(any), ", ", ppstring(), ws(), ")" ];
mapping(string, string) ->
  [ ws(), "(", ppstring(), ", ", ppstring(), ws(), ")" ];
mapping(string, int) ->
  [ ws(), "(", ppstring(), ", ", ppint(any), ws(), ")" ];
mapping(int, int) ->
  [ ws(), "(", ppint(any), ", ", ppint(any), ws(), ")" ].

mappings(InType, OutType) ->
  pplist(mapping(InType, OutType)).

expr(0, Context) ->
  oneof([ [ "delim(", identifier(Context, string), ",", ppstring(), ",", identifier_list(Context, string), ")" ]
        , [ "join(", identifier_list(Context, string), ",", ppstring(), ",", identifier(Context, string), ")" ]
        , [ "split(", identifier(Context, string), ",", ppstring(), ",", identifier(Context, string), ")" ]
        , [ "slice(", identifier(Context, string), ",", ppint(pos), ",", identifier(Context, string), ")" ]
        , [ "index(", identifier(Context, string), ",", ppint(nat), ",", ppint(pos), ",", identifier(Context, string), ")" ]
        , [ "kvsplit(", identifier(Context, string), ",", ppstring(), ",", ppstring(), ")" ]
        , [ "regex(", identifier(Context, string), ",", ppregex() , ", pcre, ",  identifier_list(Context, string), ")"]
        , [ "regex(", identifier(Context, string), ",", ppregex() , ",",  identifier_list(Context, string), ")"]
        , [ "to_integer(", identifier(Context, string), ",", identifier(Context, int), ")" ]
        , [ "to_string(", identifier(Context, int), ",", identifier(Context, string), ")" ]
        , [ "subtract(", math_operand(Context), ",", math_operand(Context), ",", identifier(Context, int), ")" ]
        , [ "add(", math_operand(Context), ",", math_operand(Context), ",", identifier(Context, int), ")" ]
        ] ++
        [ [ "map(", lists:join(",", [identifier(Context, LHS), comparator(), mappings(LHS, RHS),
                                     ppvalue(LHS), identifier(Context, RHS)]),  ")" ]
          || LHS <- [int, string],
             RHS <- [int, string],
             Context /= [] ]
       );
expr(N, Context) ->
  oneof([ expr(0, Context)
        , ?LETSHRINK([E1, E2], [expr(N div 2, Context), expr(N div 2, Context)], [E1, "|", E2])
        ]).

%% A generator for syntactic and semantic correct expressions
evallang(Context) ->
  ?SIZED(Size, expr(Size, Context)).

%% The property.
%% The Context variables are used to replace ":x" substitution vars in the provided
%% tokens to parse.
prop_lang() ->
  eqc:dont_print_counterexample(
  ?FORALL(Context, typed_context(),
  ?FORALL(String, evallang(Context),
          ?WHENFAIL(eqc:format("Failing for\n~ts\nwith context ~p\n", [String, Context]),
          try Map = maps:from_list([{Var, Val} || {Var, {_Type, Val}} <- Context]),
              F = leveled_eval:generate_eval_function(unicode:characters_to_list(String), Map),
              is_map(F(<<"hello">>, <<"world">>))
          catch Error:Reason:St ->
                  eqc:format("~n~p Failed with ~p ~p~n~p~n", [String, Error, Reason, St]),
                  equals(Error, true)
          end)))).

prop_negative() ->
  fails(fault_rate(1, 10, prop_lang())).

-endif.