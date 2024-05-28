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
        string()) ->
            fun((#{non_neg_integer() => sets:set(binary())})
                -> sets:set(binary())
            ).
generate_setop_function(EvalString) ->
    {ok, ParsedEval} = generate_setop_expression(EvalString),
    fun(MapOfSets) ->
        apply_setop(ParsedEval, MapOfSets)
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

generate_setop_expression(EvalString) ->
    String = unicode:characters_to_list(EvalString),
    {ok, Tokens, _EndLine} = leveled_setoplexer:string(String),
    leveled_setopparser:parse(Tokens).

apply_setop({setop, SetOp}, SetList) ->
    apply_setop(SetOp, SetList);
apply_setop({set_id, _, SetID}, SetList) ->
    get_set(SetID, SetList);
apply_setop(
        {SetFunctionName, {set_id, _, SetIDa}, {set_id, _, SetIDb}},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(get_set(SetIDa, SetList), get_set(SetIDb, SetList));
apply_setop(
        {SetFunctionName, {set_id, _, SetIDa}, Condition},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(get_set(SetIDa, SetList), apply_setop(Condition, SetList));
apply_setop(
        {SetFunctionName, Condition, {set_id, _, SetIDb}},
        SetList) ->
    SetFunction = set_function(SetFunctionName),
    SetFunction(apply_setop(Condition, SetList), get_set(SetIDb, SetList));
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

%% Return empty set if index not present in given set
%% (That is, do not throw an error)
get_set(SetID, SetMap) ->
    maps:get(SetID, SetMap, sets:new()).


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

    R1 =
        lists:sort(
            sets:to_list(F1(#{1 => S1, 2 => S2, 3 => S3})
        )
    ),
    R2 =
        lists:sort(
            sets:to_list(F2(#{1 => S1, 2 => S2, 3 => S3, 4 => S4})
        )
    ),
    R3 =
        lists:sort(
            sets:to_list(F3(#{1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5})
        )
    ),
        R4 =
        lists:sort(
            sets:to_list(F4(#{1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5})
        )
    ),

    ?assertMatch(
        [<<"K3">>, <<"K4">>, <<"K5">>, <<"K7">>, <<"K8">>, <<"K9">>], R1),
    ?assertMatch(
        [<<"K3">>, <<"K4">>, <<"K5">>, <<"K7">>, <<"K9">>], R2),
    ?assertMatch(
        [<<"K3">>, <<"K7">>, <<"K9">>], R3),
    ?assertMatch(
        [<<"K3">>, <<"K8">>], R4).

minimal_test() ->
    S1 = sets:from_list([<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>, <<"K5">>]),
    F1 = generate_setop_function("$1"),
    R1 = lists:sort(sets:to_list(F1(#{1 => S1}))),
    ?assertMatch([<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>, <<"K5">>], R1),
    S2 = sets:from_list([<<"K3">>, <<"K4">>, <<"K5">>, <<"K6">>, <<"K7">>]),
    S3 = sets:from_list([<<"K1">>, <<"K2">>]),
    F2 = generate_setop_function("$1 INTERSECT ($2 UNION $3)"),
    R2  = lists:sort(sets:to_list(F2(#{1 => S1, 2 => S2, 3 => S3}))),
    ?assertMatch([<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>, <<"K5">>], R2),
    F3 = generate_setop_function("$1 INTERSECT ($2 UNION $2)"),
    R3  = lists:sort(sets:to_list(F3(#{1 => S1, 2 => S2}))),
    ?assertMatch([<<"K3">>, <<"K4">>, <<"K5">>], R3).


-endif.