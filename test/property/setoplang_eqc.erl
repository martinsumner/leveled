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

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").


set_id() ->
  non_empty(list(fault(oneof("09"), oneof("12345678")))).

value() ->
  ?LET(Set, list(int()), sets:from_list(Set)).

context() ->
  list({set_id(), value()}).

ws() ->
  ?SHRINK(list(fault("x", elements(" \t\f\v\r\n\s"))), " ").

setoplang(Context) ->
  ?LET(Str, ?SIZED(Size, setoplang(Size, Context)),
       binary_to_list(iolist_to_binary(Str))).

setoplang(0, Vars) ->
  ["$", oneof(Vars), ws()];
setoplang(Size, Vars) ->
  oneof([setoplang(0, Vars),
         ?LETSHRINK([Cond], [setoplang(Size - 1, Vars)],
                    ["(", ws(), Cond, ws(), " )"]),
         ?LETSHRINK([Cond1, Cond2],
                       [setoplang(Size div 2, Vars),
                        setoplang(Size div 2, Vars)],
                    [Cond1, ws(), oneof(["AND", "OR", "NOT", "SUBTRACT", "UNION", "INTERSECT"]), ws(), Cond2])]).



%% -- Property ---------------------------------------------------------------

%% The property.
prop_gen_fun() ->
  ?FORALL(Context, non_empty(context()),
  %?FORALL(Context, non_empty([{[N], value()} || N <- "12345678"]),
  ?FORALL(String, setoplang([V || {V, _} <- Context]),
          try F = leveled_setop:generate_setop_function(String),
              sets:is_set(F([Set || {_, Set} <- Context]))
          catch Error:Reason:St ->
                  eqc:format("~n~p Failed with ~p ~p~n~p~n", [String, Error, Reason, St]),
                  equals(Error, true)
          end)).

prop_check_eval() ->
  ?FORALL(Context, [{"1", value()}, {"2", value()}, {"3", value()}],    %%non_empty(context()),
          begin
            Vars = [ "$"++Id || {Id,_} <- Context],
            String = "(" ++ lists:flatten(lists:join(" UNION ", Vars) ++ ") SUBTRACT " ++ hd(Vars)),
            ?WHENFAIL(eqc:format("setop ~p~n", [String]),
                      begin
                         F = leveled_setop:generate_setop_function(String),
                         equal_sets(F([Set || {_, Set} <- Context]),
                                    sets:subtract(sets:union([Set || {_, Set} <- Context]),
                                                 element(2, hd(Context))))
                      end)
          end).

prop_detect_faults() ->
  fails(fault_rate(1, 10, prop_gen_fun())).

equal_sets(S1, S2) ->
  ?WHENFAIL(eqc:format("~p /= ~p", [sets:to_list(S1), sets:to_list(S2)]),
            sets:is_subset(S1, S2) andalso sets:is_subset(S2, S1)).
