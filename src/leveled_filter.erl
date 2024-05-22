%% -------- Filter Functions ---------
%%
%% Support for different filter expressions within leveled 
%%

-module(leveled_filter).

-export(
    [
        generate_filter_expression/2,
        apply_filter/2,
        substitute_items/3
    ]).

%%%============================================================================
%%% External API
%%%============================================================================


apply_filter({condition, Condition}, AttrMap) ->
    apply_filter(Condition, AttrMap);
apply_filter({'OR', P1, P2}, AttrMap) ->
    apply_filter(P1, AttrMap) orelse apply_filter(P2, AttrMap);
apply_filter({'AND', P1, P2}, AttrMap) ->
    apply_filter(P1, AttrMap) andalso apply_filter(P2, AttrMap);
apply_filter({'NOT', P1}, AttrMap) ->
    not apply_filter(P1, AttrMap);
apply_filter({'BETWEEN', {identifier, _, ID}, CmpA, CmpB}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_integer(V) ->
            apply_filter({'BETWEEN', {integer, 0, V}, CmpA, CmpB}, AttrMap);
        V when is_binary(V) ->
            apply_filter({'BETWEEN', {string, 0, V}, CmpA, CmpB}, AttrMap);
        _ ->
            false
    end;
apply_filter(
        {'BETWEEN', {Type, _, V0}, {Type, _, VL}, {Type, _, VH}}, _)
        when VL =< VH ->
    V0 >= VL andalso V0 =< VH;
apply_filter(
        {'BETWEEN', {integer, TL0, I0}, {identifier, _, ID}, CmpB}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_integer(V) ->
            apply_filter(
                {'BETWEEN', {integer, TL0, I0}, {integer, 0, V}, CmpB},
                AttrMap
            );
        _ ->
            false
    end;
apply_filter(
        {'BETWEEN',
            {integer, TL0, I0}, {integer, TLL, IL}, {identifier, _, ID}
        },
        AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_integer(V) ->
            apply_filter(
                {'BETWEEN',
                    {integer, TL0, I0}, {integer, TLL, IL}, {integer, 0, V}
                },
                AttrMap
            );
        _ ->
            false
    end;
apply_filter(
        {'BETWEEN', {string, TL0, S0}, {identifier, _, ID}, CmpB}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_binary(V) ->
            apply_filter(
                {'BETWEEN', {string, TL0, S0}, {string, 0, V}, CmpB}, AttrMap);
        _ ->
            false
    end;
apply_filter(
        {'BETWEEN',
            {string, TL0, S0}, {string, TLL, SL}, {identifier, _, ID}
        },
        AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_binary(V) ->
            apply_filter(
                {'BETWEEN',
                    {string, TL0, S0}, {string, TLL, SL}, {string, 0, V}
                },
                AttrMap
            );
        _ ->
            false
    end;
apply_filter({'BETWEEN', _, _, _}, _) ->
    false;
apply_filter({'IN', {string, _, TestString}, {identifier, _, ID}}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        CheckList when is_list(CheckList) ->
            lists:member(TestString, CheckList);
        _ ->
            false
    end;
apply_filter(
        {'IN', {identifier, _, ID}, CheckList}, AttrMap)
        when is_list(CheckList) ->
    case maps:get(ID, AttrMap, notfound) of
        notfound ->
            false;
        V ->
            lists:member(V, lists:map(fun(C) -> element(3, C) end, CheckList))
    end;
apply_filter({{comparator, Cmp, TLC}, {identifier, _ , ID}, CmpB}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        notfound ->
            false;
        V when is_integer(V) ->
            apply_filter(
                {{comparator, Cmp, TLC}, {integer, 0, V}, CmpB}, AttrMap
            );
        V when is_binary(V) ->
            apply_filter(
                {{comparator, Cmp, TLC}, {string, 0, V}, CmpB}, AttrMap
            )
    end;
apply_filter({{comparator, Cmp, TLC}, CmpA, {identifier, _, ID}}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        notfound ->
            false;
        V when is_integer(V) ->
            apply_filter(
                {{comparator, Cmp, TLC}, CmpA, {integer, 0, V}}, AttrMap
            );
        V when is_binary(V) ->
            apply_filter(
                {{comparator, Cmp, TLC}, CmpA, {string, 0, V}}, AttrMap
            )
    end;
apply_filter({{comparator, Cmp, _}, {Type, _, TL}, {Type, _, TR}}, _AttrMap) ->
    compare(Cmp, TL, TR);
apply_filter({{comparator, _, _}, _, _}, _AttrMap) ->
    false;
apply_filter({contains, {identifier, _, ID}, {string, _ , SubStr}}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_binary(V) ->
            case string:find(V, SubStr) of
                nomatch ->
                    false;
                _ ->
                    true
            end;
        _ ->
            false
    end;
apply_filter(
        {begins_with, {identifier, _, ID}, {string, _ , SubStr}}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        V when is_binary(V) ->
            case string:prefix(V, SubStr) of
                nomatch ->
                    false;
                _ ->
                    true
            end;
        _ ->
            false
    end;
apply_filter({attribute_exists, {identifier, _, ID}}, AttrMap) ->
    maps:is_key(ID, AttrMap);
apply_filter({attribute_not_exists, {identifier, _, ID}}, AttrMap) ->
    not maps:is_key(ID, AttrMap);
apply_filter({attribute_empty, {identifier, _, ID}}, AttrMap) ->
    case maps:get(ID, AttrMap, notfound) of
        <<>> ->
            true;
        _ ->
            false
    end.

generate_filter_expression(FilterString, Substitutions) ->
    {ok, Tokens, _EndLine} = leveled_filterlexer:string(FilterString),
    case substitute_items(Tokens, Substitutions, []) of
        {error, Error} ->
            {error, Error};
        UpdTokens ->
            leveled_filterparser:parse(UpdTokens)
    end.

substitute_items([], _Subs, UpdTokens) ->
    lists:reverse(UpdTokens);
substitute_items([{substitution, LN, ID}|Rest], Subs, UpdTokens) ->
    case maps:get(ID, Subs, notfound) of
        notfound ->
            {error,
                lists:flatten(
                    io_lib:format("Substitution ~p not found", [ID]))};
        Value when is_binary(Value) ->
            substitute_items(
                Rest, Subs, [{string, LN, Value}|UpdTokens]);
        Value when is_integer(Value) ->
            substitute_items(Rest, Subs, [{integer, LN, Value}|UpdTokens]);
        _UnexpectedValue ->
            {error,
                lists:flatten(
                    io_lib:format("Substitution ~p unexpected type", [ID]))}
    end;
substitute_items([Token|Rest], Subs, UpdTokens) ->
    substitute_items(Rest, Subs, [Token|UpdTokens]).

%%%============================================================================
%%% Internal functions
%%%============================================================================

compare('>', V, CmpA) -> V > CmpA;
compare('>=', V, CmpA) -> V >= CmpA;
compare('<', V, CmpA) -> V < CmpA;
compare('<=', V, CmpA) -> V =< CmpA;
compare('=', V, CmpA) -> V == CmpA;
compare('<>', V, CmpA) -> V =/= CmpA.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

invalid_filterexpression_test() ->
    FE1 = "($a BETWEEN \"A\" AND \"A12\") OR (($b >= \"30\") AND contains($c, :d))",
    SubsMissing = maps:from_list([{<<"a">>, <<"MA">>}]),
    ?assertMatch(
        {error, "Substitution <<\"d\">> not found"},
        generate_filter_expression(FE1, SubsMissing)
    ),
    SubsWrongType = maps:from_list([{<<"d">>, "42"}]),
    ?assertMatch(
        {error, "Substitution <<\"d\">> unexpected type"},
        generate_filter_expression(FE1, SubsWrongType)
    ),
    SubsPresent = maps:from_list([{<<"d">>, <<"MA">>}]),
    FE2 = "($a IN (\"A\", 12)) OR (($b >= \"30\") AND contains($c, :d))",
    ?assertMatch(
        {error, {1, leveled_filterparser,["syntax error before: ","12"]}},
        generate_filter_expression(FE2, SubsPresent)
    ),
    SubsWrongTypeForContains = maps:from_list([{<<"d">>, 42}]),
    FE4 = "($a BETWEEN 12 AND 12) OR (($b >= \"30\") AND contains($c, :d))",
    ?assertMatch(
        {error, {1, leveled_filterparser, ["syntax error before: ","42"]}},
        generate_filter_expression(FE4, SubsWrongTypeForContains)
    ).

filterexpression_test() ->
    FE1 = "($a BETWEEN \"A\" AND \"A12\") AND (($b >= 30) AND contains($c, :d))",
    SubsPresent = maps:from_list([{<<"d">>, <<"MA">>}]),
    {ok, Filter1} = generate_filter_expression(FE1, SubsPresent),
    M1 = #{<<"a">> => <<"A11">>, <<"b">> => 100, <<"c">> => <<"CARTMAN">>},
    ?assert(apply_filter(Filter1, M1)),
        % ok
    
    M2 = #{<<"a">> => <<"A11">>, <<"b">> => 10, <<"c">> => <<"CARTMAN">>},
    ?assertNot(apply_filter(Filter1, M2)),
        % $b < 30
    
    FE2 = "($a BETWEEN \"A\" AND \"A12\") AND (($b >= 30) OR contains($c, :d))",
    {ok, Filter2} = generate_filter_expression(FE2, SubsPresent),
    ?assert(apply_filter(Filter2, M2)),
        % OR used so ($b >= 30) = false is ok
    
    FE3 = "($a BETWEEN \"A12\" AND \"A\") AND (($b >= 30) OR contains($c, :d))",
    {ok, Filter3} = generate_filter_expression(FE3, SubsPresent),
    ?assertNot(apply_filter(Filter3, M2)),
        % swapping the low/high - not ok - between explicitly requires low/high
    
    M3 = #{<<"a">> => <<"A11">>, <<"b">> => <<"100">>, <<"c">> => <<"CARTMAN">>},
    ?assertNot(apply_filter(Filter1, M3)),
        % substitution b is not an integer
    M3A = #{<<"a">> => 11, <<"b">> => 100, <<"c">> => <<"CARTMAN">>},
    ?assertNot(apply_filter(Filter1, M3A)),
        % substitution a is an integer
    
    FE4 =
        "($dob BETWEEN \"19700101\" AND \"19791231\") "
        "AND (contains($gns, \"#Willow\") AND contains($pcs, \"#LS\"))",
    {ok, Filter4} = generate_filter_expression(FE4, maps:new()),
    M4 =
        #{
            <<"dob">> => <<"19751124">>,
            <<"gns">> => <<"#Mia#Willow#Chloe">>,
            <<"pcs">> => <<"#BD1 1DU#LS1 4BT">>
        },
    ?assert(apply_filter(Filter4, M4)),

    FE5 =
        "($dob >= \"19740301\" AND $dob <= \"19761030\")"
        " OR ($dod > \"20200101\" AND $dod < \"20230101\")",
    
    {ok, Filter5} = generate_filter_expression(FE5, maps:new()),
    F = fun(M) -> apply_filter(Filter5, M) end,

    M5 = maps:from_list([{<<"dob">>, <<"19750202">>}, {<<"dod">>, <<"20221216">>}]),
    M6 = maps:from_list([{<<"dob">>, <<"19750202">>}, {<<"dod">>, <<"20191216">>}]),
    M7 = maps:from_list([{<<"dob">>, <<"19790202">>}, {<<"dod">>, <<"20221216">>}]),
    M8 = maps:from_list([{<<"dob">>, <<"19790202">>}, {<<"dod">>, <<"20191216">>}]),
    M9 = maps:from_list([{<<"dob">>, <<"19790202">>}, {<<"dod">>, <<"20241216">>}]),
    M10 = maps:new(),
    ?assertMatch(true, F(M5)),
    ?assertMatch(true, F(M6)),
    ?assertMatch(true, F(M7)),
    ?assertMatch(false, F(M8)),
    ?assertMatch(false, F(M9)),
    ?assertMatch(false, F(M10)),

    FE5A =
        "($dob >= \"19740301\" AND $dob <= \"19761030\")"
        " AND ($dod = \"20221216\")",
    {ok, Filter5A} = generate_filter_expression(FE5A, maps:new()),
    ?assert(apply_filter(Filter5A, M5)),
    ?assertNot(apply_filter(Filter5A, M6)),
    FE5B =
        "$dob >= \"19740301\" AND $dob <= \"19761030\""
        " AND $dod = \"20221216\"",
    {ok, Filter5B} = generate_filter_expression(FE5B, maps:new()),
    ?assert(apply_filter(Filter5B, M5)),
    ?assertNot(apply_filter(Filter5B, M6)),

    FE6 =
        "(contains($gn, \"MA\") OR $fn BETWEEN \"SM\" AND \"SN\")"
        " OR $dob <> \"19993112\"",
    {ok, Filter6} = generate_filter_expression(FE6, maps:new()),
    M11 = maps:from_list([{<<"dob">>, <<"19993112">>}]),
    ?assertMatch(false, apply_filter(Filter6, M11)),

    FE7 =
        "(contains($gn, \"MA\") OR $fn BETWEEN \"SM\" AND \"SN\")"
        " OR $dob = \"19993112\"",
    {ok, Filter7} = generate_filter_expression(FE7, maps:new()),
    ?assert(apply_filter(Filter7, M11)),

    FE8 = "(contains($gn, \"MA\") OR $fn BETWEEN \"SM\" AND \"SN\")"
        " OR $dob IN (\"19910301\", \"19910103\")",
    {ok, Filter8} = generate_filter_expression(FE8, maps:new()),
    ?assert(apply_filter(Filter8, #{<<"dob">> => <<"19910301">>})),
    ?assert(apply_filter(Filter8, #{<<"dob">> => <<"19910103">>})),
    ?assertNot(apply_filter(Filter8, #{<<"dob">> => <<"19910102">>})),
    ?assertNot(apply_filter(Filter8, #{<<"gn">> => <<"Nikki">>})),

    FE9 = "(contains($gn, \"MA\") OR $fn BETWEEN \"SM\" AND \"SN\")"
        " OR $dob IN (\"19910301\", \"19910103\")",
        % Only match with a type match
    {ok, Filter9} = generate_filter_expression(FE9, maps:new()),
    ?assert(apply_filter(Filter9, #{<<"dob">> => <<"19910301">>})),
    ?assert(apply_filter(Filter9, #{<<"dob">> => <<"19910103">>})),
    ?assertNot(apply_filter(Filter9, #{<<"dob">> => <<"19910401">>})),
    ?assertNot(apply_filter(Filter9, #{<<"dob">> => <<"19910104">>})),

    FE10 = "NOT contains($gn, \"MA\") AND "
            "(NOT $dob IN (\"19910301\", \"19910103\"))",
    {ok, Filter10} = generate_filter_expression(FE10, maps:new()),
    ?assert(
        apply_filter(
            Filter10,
            #{<<"gn">> => <<"JAMES">>, <<"dob">> => <<"19910201">>})),
    ?assertNot(
        apply_filter(
            Filter10,
            #{<<"gn">> => <<"EMMA">>, <<"dob">> => <<"19910201">>})),
    ?assertNot(
        apply_filter(
            Filter10,
            #{<<"gn">> => <<"JAMES">>, <<"dob">> => <<"19910301">>})),
    
    FE11 = "NOT contains($gn, \"MA\") AND "
        "NOT $dob IN (\"19910301\", \"19910103\")",
    {ok, Filter11} = generate_filter_expression(FE11, maps:new()),
    ?assert(
        apply_filter(
            Filter11,
            #{<<"gn">> => <<"JAMES">>, <<"dob">> => <<"19910201">>})),
    ?assertNot(
        apply_filter(
            Filter11,
            #{<<"gn">> => <<"EMMA">>, <<"dob">> => <<"19910201">>})),
    ?assertNot(
        apply_filter(
            Filter11,
            #{<<"gn">> => <<"JAMES">>, <<"dob">> => <<"19910301">>})),
    
    FE12 = "begins_with($gn, \"MA\") AND begins_with($fn, :fn)",
    {ok, Filter12} = generate_filter_expression(FE12, #{<<"fn">> => <<"SU">>}),
    ?assert(
        apply_filter(
            Filter12,
            #{<<"gn">> => <<"MATTY">>, <<"fn">> => <<"SUMMER">>})),
    ?assertNot(
        apply_filter(
            Filter12,
            #{<<"gn">> => <<"MITTY">>, <<"fn">> => <<"SUMMER">>})),
    ?assertNot(
        apply_filter(
            Filter12,
            #{<<"gn">> => <<"MATTY">>, <<"fn">> => <<"SIMMS">>})),
    ?assertNot(
        apply_filter(
            Filter12,
            #{<<"gn">> => 42, <<"fn">> => <<"SUMMER">>})),

    FE13 = "attribute_exists($dob) AND attribute_not_exists($consent) "
            "AND attribute_empty($dod)",
    {ok, Filter13} = generate_filter_expression(FE13, maps:new()),
    ?assert(
        apply_filter(
            Filter13,
            #{<<"dob">> => <<"19440812">>, <<"dod">> => <<>>})),
    ?assertNot(
        apply_filter(
            Filter13,
            #{<<"dod">> => <<>>})),
    ?assertNot(
        apply_filter(
            Filter13,
            #{<<"dob">> => <<"19440812">>,
                <<"consent">> => <<>>,
                <<"dod">> => <<>>})),
    ?assertNot(
        apply_filter(
            Filter13,
            #{<<"dob">> => <<"19440812">>, <<"dod">> => <<"20240213">>})),

    FE14 = "\"M1\" IN $gns",
    {ok, Filter14} = generate_filter_expression(FE14, maps:new()),
    ?assert(
        apply_filter(
            Filter14,
            #{<<"gns">> => [<<"MA">>, <<"M1">>, <<"A0">>]})),
    ?assertNot(
        apply_filter(
            Filter14,
            #{<<"gns">> => [<<"MA">>, <<"M2">>, <<"A0">>]})),
    ?assertNot(
        apply_filter(
            Filter14,
            #{<<"gns">> => <<"M1">>})),

    FE15 =
        "(attribute_empty($dod) AND $dob < :date)"
        "OR :date BETWEEN $dob AND $dod",
    {ok, Filter15} =
        generate_filter_expression(FE15, #{<<"date">> => <<"20200101">>}),
    ?assert(
        apply_filter(
            Filter15,
            #{<<"dob">> => <<"199900303">>, <<"dod">> => <<>>}
        )
    ),
    ?assert(
        apply_filter(
            Filter15,
            #{<<"dob">> => <<"199900303">>, <<"dod">> => <<"20210105">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter15,
            #{<<"dob">> => <<"20210303">>, <<"dod">> => <<"20230105">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter15,
            #{<<"dob">> => <<"196900303">>, <<"dod">> => <<"19890105">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter15,
            #{<<"dob">> => 199900303, <<"dod">> => <<>>}
        )
    ),

    FE15A =
        "(attribute_empty($dod) AND :date > $dob)"
        "OR :date BETWEEN $dob AND $dod",
    {ok, Filter15A} =
        generate_filter_expression(FE15A, #{<<"date">> => <<"20200101">>}),
    ?assert(
        apply_filter(
            Filter15A,
            #{<<"dob">> => <<"199900303">>, <<"dod">> => <<>>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter15A,
            #{<<"dob">> => <<"202300303">>, <<"dod">> => <<>>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter15A,
            #{<<"dob">> => <<"202300303">>}
        )
    ),
    ?assert(
        apply_filter(
            Filter15A,
            #{<<"dob">> => <<"199900303">>, <<"dod">> => <<"20210105">>}
        )
    ),

    FE16 = ":response_time BETWEEN $low_point AND $high_point",
    {ok, Filter16} =
        generate_filter_expression(
            FE16,
            #{<<"response_time">> => 346}
        ),
    ?assert(
        apply_filter(
            Filter16,
            #{<<"low_point">> => 200, <<"high_point">> => 420}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"low_point">> => 360, <<"high_point">> => 420}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"low_point">> => 210, <<"high_point">> => 320}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"low_point">> => <<"200">>, <<"high_point">> => 420}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"low_point">> => 200, <<"high_point">> => <<"420">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"high_point">> => 420}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter16,
            #{<<"low_point">> => 200}
        )
    ),

    FE17 = ":response_time > $high_point",
    {ok, Filter17} =
        generate_filter_expression(
            FE17,
            #{<<"response_time">> => 350}
        ),
    ?assert(
        apply_filter(
            Filter17,
            #{<<"high_point">> => 310}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter17,
            #{<<"high_point">> => <<"310">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter17,
            #{}
        )
    ),

    FE18 = "$dod BETWEEN $dob AND :today",
    {ok, Filter18} =
        generate_filter_expression(FE18, #{<<"today">> => <<"20240520">>}),
    ?assert(
        apply_filter(
            Filter18,
            #{<<"dob">> => <<"19900505">>, <<"dod">> => <<"20231015">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter18,
            #{<<"dob">> => <<"19900505">>, <<"dod">> => <<"20261015">>}
        )
    ),
    ?assertNot(
        apply_filter(
            Filter18,
            #{<<"dob">> => <<"19900505">>}
        )
    )
    .

-endif.
