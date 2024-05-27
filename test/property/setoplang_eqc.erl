%%% File        : setoplang_eqc.erl
%%% Created     : 14 May 2024 by Thomas Arts
%%%
%%% Lexer does not accept binary strings it seems (in OTP26)
%%% 3> leveled_setoplexer:string("$7").
%%% {ok,[{set_id,1,7}],1}
%%% 4> leveled_setoplexer:string(<<"$7">>).
%%  ** exception error: no function clause matching lists:sublist(<<"$7">>,1) (lists.erl, line 394)
%%
-module(setoplang_eqc).

-ifdef(EQC).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").


set_id() ->
  ?LET(N, choose(1,20), integer_to_list(N)).

value() ->
  ?LET(Set, list(int()), sets:from_list(Set)).

%% This context is always enumartion.
%% Consider implementing a context in which keys are not consecutive
context() ->
  ?LET(Sets, list(value()), lists:enumerate(Sets)).
%context() ->
%  ?LET(Map, map(set_id(), value()),
%       lists:sort(maps:to_list(Map))).

ws() ->
  ?SHRINK(list(elements(" \t\f\v\r\n\s")), " ").

setoplang(Context) ->
  ?SIZED(Size, setoplang(Size, Context)).

setoplang(0, Vars) ->
  ["$", oneof(Vars), ws()];
setoplang(Size, Vars) ->
  ?LAZY(
  oneof([setoplang(0, Vars),
         ?LETSHRINK([Cond], [setoplang(Size - 1, Vars)],
                    ["(", ws(), Cond, ws(), " )"]),
         ?LETSHRINK([Cond1, Cond2],
                       [setoplang(Size div 2, Vars),
                        setoplang(Size div 2, Vars)],
                    [Cond1, ws(), oneof(["SUBTRACT", "UNION", "INTERSECT"]), ws(), Cond2])])).



%% -- Property ---------------------------------------------------------------

%% The property.
prop_gen_fun() ->
  ?FORALL(Context, non_empty(context()),
  ?FORALL(String, setoplang([integer_to_list(V) || {V, _} <- Context]),
          try F = leveled_setop:generate_setop_function(String),
              sets:is_set(F([Set || {_, Set} <- Context]))
          catch Error:Reason ->
                  eqc:format("~n~ts Failed with ~p ~p~n", [String, Error, Reason]),
                  equals(Error, true)
          end)).

prop_check_eval() ->
  ?FORALL(Context, non_empty(context()),
          begin
            Vars = [ "$"++integer_to_list(Id) || {Id,_} <- Context],
            String = "(" ++ lists:flatten(lists:join(" UNION ", Vars) ++ ") SUBTRACT " ++ hd(Vars)),
            ?WHENFAIL(eqc:format("setop ~ts~n", [String]),
                      begin
                         F = leveled_setop:generate_setop_function(String),
                         equal_sets(F([Set || {_, Set} <- Context]),
                                    sets:subtract(sets:union([Set || {_, Set} <- Context]),
                                                 element(2, hd(Context))))
                      end)
          end).

equal_sets(S1, S2) ->
  ?WHENFAIL(eqc:format("~p /= ~p", [sets:to_list(S1), sets:to_list(S2)]),
            sets:is_subset(S1, S2) andalso sets:is_subset(S2, S1)).

-endif.
