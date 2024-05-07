%% -------- Set Operations ---------
%%
%% Support for set operations (i.e on sets of keys) within leveled 
%%

-module(leveled_setop).

-export([generate_setop_function/1]).


%%%============================================================================
%%% External API
%%%============================================================================

-spec generate_setop_function(
        string()) -> fun((list(sets:set(binary()))) -> sets:set(binary())).
generate_setop_function(EvalString) ->
    {ok, ParsedEval} = generate_setop_expression(EvalString),
    fun(ListOfSets) ->
        apply_setop(ParsedEval, ListOfSets)
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

generate_setop_expression(EvalString) ->
    {ok, Tokens, _EndLine} = leveled_setoplexer:string(EvalString),
    leveled_setopparser:parse(Tokens).

apply_setop({setop, SetOp}, SetList) ->
    apply_setop(SetOp, SetList);
apply_setop(
        {SetFunctionName, {set_id, _, SetIDa}, {set_id, _, SetIDb}},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(lists:nth(SetIDa, SetList), lists:nth(SetIDb, SetList));
apply_setop(
        {SetFunctionName, {set_id, _, SetIDa}, Condition},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(lists:nth(SetIDa, SetList), apply_setop(Condition, SetList));
apply_setop(
        {SetFunctionName, Condition, {set_id, _, SetIDb}},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(apply_setop(Condition, SetList), lists:nth(SetIDb, SetList));
apply_setop({SetFunctionName, ConditionA, ConditionB}, SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(
        apply_setop(ConditionA, SetList), apply_setop(ConditionB, SetList)
    ).

set_function('UNION') ->
    fun(A, B) -> sets:union(A, B) end;
set_function('INTERSECT') ->
    fun(A, B) -> sets:intersection(A, B) end;
set_function('SUBTRACT') ->
    fun(A, B) -> sets:subtract(A, B) end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

parser_formal_test() ->
    Q1 = "($1 INTERSECT $2) UNION $3",
    Q2 = "($1 INTERSECT $2) UNION ($3 INTERSECT $4)",
    Q3 = "($1 INTERSECT $2 INTERSECT $5) UNION ($3 INTERSECT $4)",
    Q4 = "($1 INTERSECT $2 INTERSECT $5) UNION ($3 SUBTRACT $4)",
    parser_tester(Q1, Q2, Q3, Q4).

parser_alt_test() ->
    Q1 = "($1 AND $2) OR $3",
    Q2 = "($1 AND $2) OR ($3 AND $4)",
    Q3 = "($1 AND $2 AND $5) OR ($3 AND $4)",
    Q4 = "($1 AND $2 AND $5) OR ($3 NOT $4)",
    parser_tester(Q1, Q2, Q3, Q4).

parser_tester(Q1, Q2, Q3, Q4) ->
    S1 = sets:from_list([<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>, <<"K5">>]),
    S2 = sets:from_list([<<"K3">>, <<"K4">>, <<"K5">>, <<"K6">>, <<"K7">>]),
    S3 = sets:from_list([<<"K7">>, <<"K8">>, <<"K9">>]),
    S4 = sets:from_list([<<"K7">>, <<"K9">>, <<"K0">>]),
    S5 = sets:from_list([<<"K1">>, <<"K2">>, <<"K3">>, <<"K8">>, <<"K9">>]),

    F1 = generate_setop_function(Q1),
    F2 = generate_setop_function(Q2),
    F3 = generate_setop_function(Q3),
    F4 = generate_setop_function(Q4),

    R1 = lists:sort(sets:to_list(F1([S1, S2, S3]))),
    R2 = lists:sort(sets:to_list(F2([S1, S2, S3, S4]))),
    R3 = lists:sort(sets:to_list(F3([S1, S2, S3, S4, S5]))),
    R4 = lists:sort(sets:to_list(F4([S1, S2, S3, S4, S5]))),

    ?assertMatch(
        [<<"K3">>, <<"K4">>, <<"K5">>, <<"K7">>, <<"K8">>, <<"K9">>], R1),
    ?assertMatch(
        [<<"K3">>, <<"K4">>, <<"K5">>, <<"K7">>, <<"K9">>], R2),
    ?assertMatch(
        [<<"K3">>, <<"K7">>, <<"K9">>], R3),
    ?assertMatch(
        [<<"K3">>, <<"K8">>], R4).

-endif.