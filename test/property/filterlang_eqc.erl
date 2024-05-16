%%
%% Observations: lexer has
%%\:[a-zA-Z_][a-zA-Z_0-9]* : {token, {substitution, TokenLine, strip_substitution(TokenChars)}}.
%% but there is no parser rule for it.
%% (Hence subs will fail)
%%
%% Negative integers are not allowed
%% Escaped quotes in strings are not handled
%%
-module(filterlang_eqc).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").

identifier() ->
  FirstChars = lists:seq($a,$z)++lists:seq($A,$Z)++["_"],
  OtherChars = FirstChars++lists:seq($0,$9),
  ?LET({X, Xs}, {oneof(FirstChars), list(elements(OtherChars))}, iolist_to_binary([X|Xs])).

identifier(Vars) ->
  ?LET(V, oneof([identifier() | Vars]), [ws(), "$", V, ws()]).

%% No nested escaped quotes in strings
string() ->
  list(oneof([choose(35, 255), choose(1, 33)])).

context() ->
  list({identifier(), int()}).

ws() ->
  list(fault("x", elements(" \t\f\v\r\n\s"))).

comparator() ->
  oneof([">", "<", "=", "<>", "<=", ">="]).

ppint() ->
  [ws(), ?LET(X, int(), integer_to_list(X)), ws()].

ppstring() ->
  [ws(), "\"", string(), "\"", ws()].

operand(Vars) ->
    oneof([ [ws(), "$", oneof(Vars), ws()] || Vars /= []] ++
          [ [ws(), ":", oneof(Vars), ws()] || Vars /= []] ++ %% will fail
          [ ppint(), ppstring() ]).

operand_list(Vars) ->
  ?LET(OpList, non_empty(list(operand(Vars))),
       [ws(), "("] ++ lists:join(",", OpList) ++ [")", ws()]).

condition(0, Vars) ->
  oneof([ [ identifier(Vars), " IN", operand_list(Vars) ]
        , [ operand(Vars), " IN", identifier(Vars) ]
        , [ operand(Vars), comparator(), operand(Vars) ]
        , [ operand(Vars), "BETWEEN", ppint(), "AND", ppint() ]
        ]);
condition(N, Vars) ->
  oneof([ condition(0, Vars)
        , ?LETSHRINK([C], [condition(N - 1, Vars)],
                     oneof([ ["NOT", C]
                           , ["(", ws(), C, ws(), ")"]
                           ]))
        , ?LETSHRINK([C1, C2], [condition(N div 2, Vars), condition(N div 2, Vars)],
                     oneof([ [C1, "AND", C2]
                           , [C1, "OR", C2]
                           ]))
        ]).

%% A generator for syntactic and semantic correct expressions
filterlang(Vars) ->
  ?SIZED(Size, ?LET(Str, filterlang(Size, Vars),
                    iolist_to_binary(Str))).

filterlang(N, Vars) ->
  condition(N, Vars).
  %%?LETSHRINK([C1, C2], filterlang(N div 2, Vars), {condition, C1, C2}).

%% The property.
prop_gen_fun() ->
  eqc:dont_print_counterexample(
  ?FORALL(Context, context(),
  ?FORALL(String, filterlang([V || {V, _} <- Context]),
          ?WHENFAIL(eqc:format("Failing for\n~s\nwith context ~p\n", [String, Context]),
          try Map = maps:from_list(Context),
              F = leveled_filter:generate_filter_function(binary_to_list(String), Map),
              is_boolean(F(Map))
          catch Error:Reason:St ->
                  eqc:format("~n~p Failed with ~p ~p~n~p~n", [String, Error, Reason, St]),
                  equals(Error, true)
          end)))).

