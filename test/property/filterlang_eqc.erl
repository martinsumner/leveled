-module(filterlang_eqc).

-ifdef(EQC).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-define(lazy_oneof(Gens), ?LAZY(oneof(Gens))).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
  {timeout,
      ?EQC_TIME_BUDGET + 10,
      ?_assertEqual(
          true,
          eqc:quickcheck(
              eqc:testing_time(?EQC_TIME_BUDGET, ?QC_OUT(prop_lang()))))}.

identifier() ->
  FirstChars = lists:seq($a,$z)++lists:seq($A,$Z)++["_"],
  OtherChars = FirstChars++lists:seq($0,$9),
  ?LET({X, Xs}, {oneof(FirstChars), list(elements(OtherChars))}, unicode:characters_to_binary([X|Xs])).

ppidentifier(Vars) ->
  ?LET(V, oneof([identifier() | Vars]), [ws(), "$", V, " ",ws()]).

%% No quotes in strings
%% Filter the quote with `re` instead of string:find to
%% be compatible with lexer
string() ->
  ?SUCHTHAT(String, non_empty(utf8()), re:run(String, "\"") == nomatch).

context() ->
  list({identifier(), oneof([int(), string()])}).

ws() ->
  ?SHRINK(list(elements(" \t\f\v\r\n\s")), [" "]).

comparator() ->
  oneof([">", "<", "=", "<>", "<=", ">="]).

ppint() ->
  [ws(), ?LET(X, int(), integer_to_list(X)), ws()].

ppstring() ->
  [ws(), "\"", string(), "\"", ws()].

pplist(Gen) ->
  ?LET(List, non_empty(list(Gen)),
       [ws(), "("] ++ lists:join(",", List) ++ [")", ws()]).

operand(Vars) ->
    oneof([ ppidentifier(Vars) ] ++
          [ [ws(), ":", oneof(Vars), " ", ws()]  || Vars /= []] ++
             %% Always in context, because
             %% should fail with error if substitution vars not in context
          [ ppint(), ppstring() ]).

operand_list(Vars) ->
  ?LET(OpList, non_empty(list(operand(Vars))),
       [ws(), "("] ++ lists:join(",", OpList) ++ [")", ws()]).

condition(0, Vars) ->
  oneof([ [ operand(Vars), comparator(), operand(Vars) ]
        , [ operand(Vars), "BETWEEN", operand(Vars), "AND", operand(Vars) ]
        , [ ppidentifier(Vars), " IN", pplist(ppstring()) ]
        , [ ppstring(), " IN", ppidentifier(Vars) ]
        , [ "contains(", ppidentifier(Vars), ", ", ppstring(), ")" ]
        , [ "begins_with(", ppidentifier(Vars), ", ", ppstring(), ")" ]
        , [ "attribute_exists(", ppidentifier(Vars), ")" ]
        , [ "attribute_not_exists(", ppidentifier(Vars), ")" ]
        , [ "attribute_empty(", ppidentifier(Vars), ")" ]
        ]);
condition(N, Vars) ->
  ?lazy_oneof([ condition(0, Vars)
        , ?LETSHRINK([C], [condition(N - 1, Vars)],
                     ?lazy_oneof([ ["NOT", C] , ["(", ws(), C, ws(), ")"] ]))
        , ?LETSHRINK([C1, C2], [condition(N div 2, Vars), condition(N div 2, Vars)],
                     ?lazy_oneof([ [C1, "AND", C2] , [C1, "OR", C2] ]))
        ]).

%% A generator for syntactic and semantic correct expressions
filterlang(Vars) ->
  ?SIZED(Size, filterlang(Size, Vars)).

filterlang(N, Vars) ->
  condition(N, Vars).

%% The property.
%% The Context variables are used to replace ":x" substitution vars in the provided
%% tokens to parse.
prop_lang() ->
  eqc:dont_print_counterexample(
  ?FORALL(Context, context(),
  ?FORALL(String, filterlang([V || {V, _} <- Context]),
          ?WHENFAIL(eqc:format("Failing for\n~ts\nwith context ~p\n", [String, Context]),
          try Map = maps:from_list(Context),
              {ok, Expr} = leveled_filter:generate_filter_expression(unicode:characters_to_list(String), Map),
              is_boolean(leveled_filter:apply_filter(Expr, Map))
          catch Error:Reason:St ->
                  eqc:format("~n~p Failed with ~p ~p~n~p~n", [String, Error, Reason, St]),
                  equals(Error, true)
          end)))).

-endif.
