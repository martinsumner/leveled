%% -------- TREE ---------
%%
%% There are two trees supported within leveled, tree and idxt - however idxt
%% is the only one used at present.
%%
%% The tree type is a gb_trees:tree(), and the idxt is a variation whereby 1 in
%% 12 keys go into a gb_trees:tree(), and there is a tuple of sublists 12-wide
%% kept to one side.  All operations in idxt are a tree walk followed by a
%% check against the relevant sublists.
%%
%% The indexed keys in idxt are the end keys in each sublist.
%%
%% The idxt approach is faster to convert to/from a list.  It is also faster
%% to find matching ranges.  It is slower at lookups, but only marginally
%% slower.
%%
%% There are timing tests within the eunit test suite for this module to
%% demonstrate the difference.

-module(leveled_tree).

-export([
    from_orderedlist/2,
    from_orderedset/2,
    from_orderedlist/3,
    from_orderedset/3,
    to_list/1,
    match_range/3,
    search_range/4,
    match/2,
    search/3,
    tsize/1,
    empty/1
]).

-define(SKIP_WIDTH, 12).

-type tree_type() :: tree | idxt.
-type leveled_tree_tree() :: {tree, gb_trees:tree()}.
-type leveled_tree_idxt() ::
    {idxt, non_neg_integer(), {tuple(), gb_trees:tree()}}.
-type leveled_tree() ::
    leveled_tree_tree() | leveled_tree_idxt().

-export_type([leveled_tree/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec from_orderedset(ets:tab(), tree_type()) -> leveled_tree().
%% @doc
%% Convert an ETS table of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.
from_orderedset(Table, Type) ->
    from_orderedlist(ets:tab2list(Table), Type, ?SKIP_WIDTH).

-spec from_orderedset(
    ets:tab(), tree_type(), integer() | auto
) -> leveled_tree().
%% @doc
%% Convert an ETS table of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.  The SkipWidth is an integer representing
%% the underlying list size joined in the tree (the trees are all trees of
%% lists of this size).
from_orderedset(Table, Type, SkipWidth) ->
    from_orderedlist(ets:tab2list(Table), Type, SkipWidth).

-spec from_orderedlist(list(tuple()), tree_type()) -> leveled_tree().
%% @doc
%% Convert a list of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.
from_orderedlist(OrderedList, Type) ->
    from_orderedlist(OrderedList, Type, ?SKIP_WIDTH).

-spec from_orderedlist(
    list(tuple()), tree_type(), integer() | auto
) -> leveled_tree().
%% @doc
%% Convert a list of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.  The SkipWidth is an integer representing
%% the underlying list size joined in the tree (the trees are all trees of
%% lists of this size).
from_orderedlist(OrderedList, tree, _SkipWidth) ->
    {tree, gb_trees:from_orddict(OrderedList)};
from_orderedlist(OrderedList, idxt, SkipWidth) ->
    L = length(OrderedList),
    {idxt, L, idxt_fromorderedlist(OrderedList, {[], [], 1}, L, SkipWidth)}.

-spec match(tuple() | integer(), leveled_tree()) -> none | {value, any()}.
%% @doc
%% Return the value from a tree associated with an exact match for the given
%% key.  This assumes the tree contains the actual keys and values to be
%% matched against, not a manifest representing ranges of keys and values.
match(Key, {tree, Tree}) ->
    gb_trees:lookup(Key, Tree);
match(Key, {idxt, _L, {TLI, IDX}}) when is_tuple(TLI) ->
    Iter = gb_trees:iterator_from(Key, IDX),
    case gb_trees:next(Iter) of
        none ->
            none;
        {_NK, ListID, _Iter} when is_integer(ListID) ->
            lookup_match(Key, element(ListID, TLI))
    end.

-spec search(
    tuple() | integer(),
    leveled_tree(),
    fun((term()) -> leveled_codec:object_key())
) ->
    none | tuple().
%% @doc
%% Find the first key >= to the SearchKey in the tree.  The StartKeyFun may
%% be used when the value contains information about whether that key
%% represents a genuine match (normally used only when the tree is a tree of
%% penciller manifest entries - where the Key is the LastKey in the manifest
%% entry, but it is also required for the Key to be >= than the FirstKey in
%% the manifest entry)
search(Key, {tree, Tree}, StartKeyFun) ->
    Iter = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(Iter) of
        none ->
            none;
        {NK, V, _Iter} ->
            case Key >= StartKeyFun(V) of
                true ->
                    {NK, V};
                false ->
                    none
            end
    end;
search(Key, {idxt, _L, {TLI, IDX}}, StartKeyFun) when is_tuple(TLI) ->
    Iter = gb_trees:iterator_from(Key, IDX),
    case gb_trees:next(Iter) of
        none ->
            none;
        {_NK, ListID, _Iter} when is_integer(ListID) ->
            {K, V} = lookup_best(Key, element(ListID, TLI)),
            case Key >= StartKeyFun(V) of
                true ->
                    {K, V};
                false ->
                    none
            end
    end.

-spec match_range(
    tuple() | integer() | all,
    tuple() | integer() | all,
    leveled_tree()
) -> list().
%% @doc
%% Return a range of value between trees from a tree associated with an
%% exact match for the given key.  This assumes the tree contains the actual
%% keys and values to be matched against, not a manifest representing ranges
%% of keys and values.
%%
%% The keyword all can be used as a substitute for the StartKey to remove a
%% constraint from the range.
match_range(StartRange, EndRange, Tree) ->
    EndRangeFun =
        fun(ER, FirstRHSKey, _FirstRHSValue) ->
            ER == FirstRHSKey
        end,
    match_range(StartRange, EndRange, Tree, EndRangeFun).

-spec match_range(
    tuple() | integer() | all,
    tuple() | integer() | all,
    leveled_tree(),
    fun((term(), term(), term()) -> boolean())
) -> list().
%% @doc
%% As match_range/3 but a function can be passed to be used when comparing the
%5 EndKey with a key in the tree (such as leveled_codec:endkey_passed), where
%% Erlang term comparison will not give the desired result.
match_range(StartRange, EndRange, {tree, Tree}, EndRangeFun) ->
    treelookup_range_start(StartRange, EndRange, Tree, EndRangeFun);
match_range(StartRange, EndRange, {idxt, _L, Tree}, EndRangeFun) ->
    idxtlookup_range_start(StartRange, EndRange, Tree, EndRangeFun).

-spec search_range(
    tuple() | integer() | all,
    tuple() | integer() | all,
    leveled_tree(),
    fun((leveled_pmanifest:manifest_entry()) -> leveled_codec:object_key())
) ->
    list().
%% @doc
%% Extract a range from a tree, with search used when the tree is a manifest
%% of key ranges and it is necessary to find a range which may encapsulate the
%% key range.
%%
%% The StartKeyFun is used if the values contain extra information that can be
%% used to determine if the key is or is not present.
search_range(StartRange, EndRange, Tree, StartKeyFun) ->
    EndRangeFun =
        fun(ER, _FirstRHSKey, FirstRHSValue) ->
            StartRHSKey = StartKeyFun(FirstRHSValue),
            not leveled_codec:endkey_passed(ER, StartRHSKey)
        end,
    case Tree of
        {tree, T} ->
            treelookup_range_start(StartRange, EndRange, T, EndRangeFun);
        {idxt, _L, T} ->
            idxtlookup_range_start(StartRange, EndRange, T, EndRangeFun)
    end.

-spec to_list(leveled_tree()) -> list().
%% @doc
%% Collapse the tree back to a list
to_list({tree, Tree}) ->
    gb_trees:to_list(Tree);
to_list({idxt, _L, {TLI, _IDX}}) when is_tuple(TLI) ->
    lists:append(tuple_to_list(TLI)).

-spec tsize(leveled_tree()) -> integer().
%% @doc
%% Return the count of items in a tree
tsize({tree, Tree}) ->
    gb_trees:size(Tree);
tsize({_Type, L, _Tree}) ->
    L.

-spec empty(tree_type()) -> leveled_tree().
%% @doc
%% Return an empty tree of the given type
empty(tree) ->
    {tree, gb_trees:empty()};
empty(idxt) ->
    {idxt, 0, {{}, gb_trees:empty()}}.

%%%============================================================================
%%% Internal Functions
%%%============================================================================

idxt_fromorderedlist([], {TmpListElements, TmpListIdx, _C}, _L, _SkipWidth) ->
    {
        list_to_tuple(lists:reverse(TmpListElements)),
        gb_trees:from_orddict(lists:reverse(TmpListIdx))
    };
idxt_fromorderedlist(OrdList, {TmpListElements, TmpListIdx, C}, L, SkipWidth) ->
    SubLL = min(SkipWidth, L),
    {Head, Tail} = lists:split(SubLL, OrdList),
    {LastK, _LastV} = lists:last(Head),
    idxt_fromorderedlist(
        Tail,
        {[Head | TmpListElements], [{LastK, C} | TmpListIdx], C + 1},
        L - SubLL,
        SkipWidth
    ).

lookup_match(Key, KVList) ->
    case lists:keyfind(Key, 1, KVList) of
        false ->
            none;
        {Key, Value} ->
            {value, Value}
    end.

lookup_best(Key, [{EK, EV} | _Tail]) when EK >= Key ->
    {EK, EV};
lookup_best(Key, [_Top | Tail]) ->
    lookup_best(Key, Tail).

treelookup_range_start(StartRange, EndRange, Tree, EndRangeFun) ->
    Iter0 = gb_trees:iterator_from(StartRange, Tree),
    lists:reverse(tree_range_fold(Iter0, EndRange, EndRangeFun, [])).

tree_range_fold(Iter, EndRange, EndRangeFun, Acc) ->
    case gb_trees:next(Iter) of
        none ->
            Acc;
        {NK, NV, Iter1} ->
            case leveled_codec:endkey_passed(EndRange, NK) of
                true ->
                    case EndRangeFun(EndRange, NK, NV) of
                        true ->
                            [{NK, NV} | Acc];
                        false ->
                            Acc
                    end;
                false ->
                    tree_range_fold(Iter1, EndRange, EndRangeFun, [
                        {NK, NV} | Acc
                    ])
            end
    end.

idxtlookup_range_start(StartRange, EndRange, {TLI, IDX}, EndRangeFun) ->
    % TLI tuple of lists, IDS is a gb_tree of End Keys mapping to tuple
    % indexes
    Iter0 = gb_trees:iterator_from(StartRange, IDX),
    case gb_trees:next(Iter0) of
        none ->
            [];
        {NK, ListID, Iter1} ->
            BeforeFun =
                fun({K, _V}) ->
                    K < StartRange
                end,
            {_LHS, RHS} = lists:splitwith(BeforeFun, element(ListID, TLI)),
            % The RHS is the list of {EK, SK} elements where the EK >=  the
            % StartRange, otherwise the LHS falls before the range
            case idxtlookup_range_end(EndRange, NK, Iter1, []) of
                {[], true} ->
                    right_trim(RHS, EndRangeFun, EndRange);
                {[], false} ->
                    RHS;
                {[HdIdx | RestIdx], RTrim} ->
                    RHS ++
                        lists:foldl(
                            fun(I, Acc) -> element(I, TLI) ++ Acc end,
                            case RTrim of
                                true ->
                                    right_trim(
                                        element(HdIdx, TLI),
                                        EndRangeFun,
                                        EndRange
                                    );
                                false ->
                                    []
                            end,
                            case RTrim of
                                true ->
                                    RestIdx;
                                false ->
                                    [HdIdx | RestIdx]
                            end
                        )
            end
    end.

right_trim(SubList, EndRangeFun, EndRange) ->
    PredFun =
        fun({K, _V}) ->
            not leveled_codec:endkey_passed(EndRange, K)
        % true if EndRange is after K
        end,
    {LHS, [{FirstRHSKey, FirstRHSValue} | _Rest]} =
        lists:splitwith(PredFun, SubList),
    case EndRangeFun(EndRange, FirstRHSKey, FirstRHSValue) of
        true ->
            % The start key is not after the end of the range
            % and so this should be included in the range
            LHS ++ [{FirstRHSKey, FirstRHSValue}];
        false ->
            % the start key of the next key is after the end
            % of the range and so should not be included
            LHS
    end.

idxtlookup_range_end(EndRange, NK0, Iter0, Acc) ->
    case leveled_codec:endkey_passed(EndRange, NK0) of
        true ->
            {Acc, true};
        false ->
            case gb_trees:next(Iter0) of
                none ->
                    {Acc, false};
                {NK1, ListID, Iter1} ->
                    idxtlookup_range_end(
                        EndRange,
                        NK1,
                        Iter1,
                        [ListID | Acc]
                    )
            end
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(
        Seqn,
        Count,
        [],
        BucketRangeLow,
        BucketRangeHigh
    ).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BRand = rand:uniform(BRange),
    BNumber =
        lists:flatten(
            io_lib:format("K~4..0B", [BucketLow + BRand])
        ),
    KNumber =
        lists:flatten(
            io_lib:format("K~8..0B", [rand:uniform(1000)])
        ),
    {K, V} =
        {
            {o_kv, {<<"btype">>, list_to_binary("Bucket" ++ BNumber)},
                list_to_binary("Key" ++ KNumber), null},
            Seqn
        },
    generate_randomkeys(
        Seqn + 1,
        Count - 1,
        [{K, V} | Acc],
        BucketLow,
        BRange
    ).

generate_simplekeys(Seqn, Count) ->
    generate_simplekeys(Seqn, Count, []).

generate_simplekeys(_Seqn, 0, Acc) ->
    Acc;
generate_simplekeys(Seqn, Count, Acc) ->
    KNumber =
        list_to_binary(
            lists:flatten(
                io_lib:format("K~8..0B", [rand:uniform(100000)])
            )
        ),
    generate_simplekeys(Seqn + 1, Count - 1, [{KNumber, Seqn} | Acc]).

tree_search_test() ->
    search_test_by_type(tree),
    extra_searchrange_test_by_type(tree).

idxt_search_test() ->
    search_test_by_type(idxt),
    extra_searchrange_test_by_type(idxt).

search_test_by_type(Type) ->
    MapFun =
        fun(N) ->
            {N * 4, N * 4 - 2}
        end,
    KL = lists:map(MapFun, lists:seq(1, 50)),
    T = from_orderedlist(KL, Type),

    StartKeyFun = fun(V) -> V end,
    statistics(runtime),
    ?assertMatch([], search_range(0, 1, T, StartKeyFun)),
    ?assertMatch([], search_range(201, 202, T, StartKeyFun)),
    ?assertMatch([{4, 2}], search_range(2, 4, T, StartKeyFun)),
    ?assertMatch([{4, 2}], search_range(2, 5, T, StartKeyFun)),
    ?assertMatch([{4, 2}, {8, 6}], search_range(2, 6, T, StartKeyFun)),
    ?assertMatch(50, length(search_range(2, 200, T, StartKeyFun))),
    ?assertMatch(50, length(search_range(2, 198, T, StartKeyFun))),
    ?assertMatch(49, length(search_range(2, 197, T, StartKeyFun))),
    ?assertMatch(49, length(search_range(4, 197, T, StartKeyFun))),
    ?assertMatch(48, length(search_range(5, 197, T, StartKeyFun))),
    {_, T1} = statistics(runtime),
    io:format(
        user,
        "10 range tests with type ~w in ~w microseconds~n",
        [Type, T1]
    ).

tree_oor_test() ->
    outofrange_test_by_type(tree).

idxt_oor_test() ->
    outofrange_test_by_type(idxt).

outofrange_test_by_type(Type) ->
    MapFun =
        fun(N) ->
            {N * 4, N * 4 - 2}
        end,
    KL = lists:map(MapFun, lists:seq(1, 50)),
    T = from_orderedlist(KL, Type),

    io:format("Out of range searches~n"),
    ?assertMatch(none, match(0, T)),
    ?assertMatch(none, match(5, T)),
    ?assertMatch(none, match(97, T)),
    ?assertMatch(none, match(197, T)),
    ?assertMatch(none, match(201, T)),

    StartKeyFun = fun(V) -> V end,

    ?assertMatch(none, search(0, T, StartKeyFun)),
    ?assertMatch(none, search(5, T, StartKeyFun)),
    ?assertMatch(none, search(97, T, StartKeyFun)),
    ?assertMatch(none, search(197, T, StartKeyFun)),
    ?assertMatch(none, search(201, T, StartKeyFun)).

tree_tolist_test() ->
    tolist_test_by_type(tree).

idxt_tolist_test() ->
    tolist_test_by_type(idxt).

tolist_test_by_type(Type) ->
    MapFun =
        fun(N) ->
            {N * 4, N * 4 - 2}
        end,
    KL = lists:map(MapFun, lists:seq(1, 50)),
    T = from_orderedlist(KL, Type),
    T_Reverse = to_list(T),
    ?assertMatch(KL, T_Reverse).

timing_tests_tree_test_() ->
    {timeout, 60, fun tree_timing/0}.

timing_tests_idxt_test_() ->
    {timeout, 60, fun idxt_timing/0}.

tree_timing() ->
    log_tree_test_by_(1, tree, 8000),
    log_tree_test_by_(1, tree, 4000),
    log_tree_test_by_(1, tree, 2000),
    log_tree_test_by_(1, tree, 256),
    log_tree_test_by_simplekey_(1, tree, 256).

idxt_timing() ->
    log_tree_test_by_(12, idxt, 8000),
    log_tree_test_by_(12, idxt, 4000),
    log_tree_test_by_(32, idxt, 2000),
    log_tree_test_by_(12, idxt, 2000),
    log_tree_test_by_(6, idxt, 2000),
    log_tree_test_by_(12, idxt, 256),
    log_tree_test_by_simplekey_(12, idxt, 256).

log_tree_test_by_(Width, Type, N) ->
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    SW = os:timestamp(),
    tree_test_by_(Width, Type, KL),
    io:format(
        user,
        "Test took ~w ms",
        [timer:now_diff(os:timestamp(), SW) div 1000]
    ).

log_tree_test_by_simplekey_(Width, Type, N) ->
    KL = lists:ukeysort(1, generate_simplekeys(1, N)),
    SW = os:timestamp(),
    tree_test_by_(Width, Type, KL, false),
    io:format(
        user,
        "Test with simple key took ~w ms",
        [timer:now_diff(os:timestamp(), SW) div 1000]
    ).

tree_test_by_(Width, Type, KL) ->
    tree_test_by_(Width, Type, KL, true).

tree_test_by_(Width, Type, KL, ComplexKey) ->
    io:format(
        user,
        "~n~nTree test with complexkey=~w for type and width: ~w ~w~n",
        [ComplexKey, Type, Width]
    ),

    OS = ets:new(test, [ordered_set, private]),
    ets:insert(OS, KL),
    SWaETS = os:timestamp(),
    Tree0 = from_orderedset(OS, Type, Width),
    io:format(
        user,
        "Generating tree from ETS in ~w microseconds" ++
            " of size ~w~n",
        [
            timer:now_diff(os:timestamp(), SWaETS),
            tsize(Tree0)
        ]
    ),
    io:format(
        user,
        "Tree has footprint size ~w bytes flat_size ~w bytes~n",
        [erts_debug:size(Tree0) * 8, erts_debug:flat_size(Tree0) * 8]
    ),

    SWaGSL = os:timestamp(),
    Tree1 = from_orderedlist(KL, Type, Width),
    io:format(
        user,
        "Generating tree from orddict in ~w microseconds" ++
            " of size ~w~n",
        [
            timer:now_diff(os:timestamp(), SWaGSL),
            tsize(Tree1)
        ]
    ),
    io:format(
        user,
        "Tree has footprint size ~w bytes flat_size ~w bytes~n",
        [erts_debug:size(Tree1) * 8, erts_debug:flat_size(Tree1) * 8]
    ),

    SWaLUP = os:timestamp(),
    lists:foreach(match_fun(Tree0), KL),
    lists:foreach(match_fun(Tree1), KL),
    io:format(
        user,
        "Looked up all keys twice in ~w microseconds~n",
        [timer:now_diff(os:timestamp(), SWaLUP)]
    ),

    ?assertMatch(Tree0, Tree1),

    SWaSRCH1 = os:timestamp(),
    lists:foreach(search_exactmatch_fun(Tree0), KL),
    lists:foreach(search_exactmatch_fun(Tree1), KL),
    io:format(
        user,
        "Search all keys twice for exact match in ~w microseconds~n",
        [timer:now_diff(os:timestamp(), SWaSRCH1)]
    ),

    BitBiggerKeyFun =
        case ComplexKey of
            true ->
                fun(Idx) ->
                    {K, _V} = lists:nth(Idx, KL),
                    {o_kv, B, FullKey, null} = K,
                    {
                        {o_kv, B,
                            list_to_binary(binary_to_list(FullKey) ++ "0"),
                            null},
                        lists:nth(Idx + 1, KL)
                    }
                end;
            false ->
                fun(Idx) ->
                    {K, _V} = lists:nth(Idx, KL),
                    {
                        list_to_binary(binary_to_list(K) ++ "0"),
                        lists:nth(Idx + 1, KL)
                    }
                end
        end,

    SrchKL = lists:map(BitBiggerKeyFun, lists:seq(1, length(KL) - 1)),

    SWaSRCH2 = os:timestamp(),
    lists:foreach(search_nearmatch_fun(Tree0), SrchKL),
    lists:foreach(search_nearmatch_fun(Tree1), SrchKL),
    io:format(
        user,
        "Search all keys twice for near match in ~w microseconds~n",
        [timer:now_diff(os:timestamp(), SWaSRCH2)]
    ),

    BigRanges =
        lists:map(
            fun(I) ->
                get_random_range(
                    KL,
                    case I rem 2 of
                        0 -> exact;
                        1 -> over
                    end,
                    400
                )
            end,
            lists:seq(1, 1000)
        ),
    ok = test_ranges(BigRanges, Tree0, Tree1, big),
    MidRanges =
        lists:map(
            fun(I) ->
                get_random_range(
                    KL,
                    case I rem 2 of
                        0 -> exact;
                        1 -> over
                    end,
                    40
                )
            end,
            lists:seq(1, 1000)
        ),
    ok = test_ranges(MidRanges, Tree0, Tree1, mid),
    SmallRanges =
        lists:map(
            fun(I) ->
                get_random_range(
                    KL,
                    case I rem 2 of
                        0 -> exact;
                        1 -> over
                    end,
                    10
                )
            end,
            lists:seq(1, 1000)
        ),
    ok = test_ranges(SmallRanges, Tree0, Tree1, small),

    {TC0, OL} = timer:tc(fun() -> to_list(Tree0) end),
    {TC1, OL} = timer:tc(fun() -> to_list(Tree1) end),

    io:format(user, "Reverted both to_list in ~w microseconds~n", [TC0 + TC1]).

test_ranges(TestRanges, Tree0, Tree1, Size) ->
    {TCRange0, RL0} =
        timer:tc(
            fun() ->
                lists:map(
                    fun({SK, EK, SL}) ->
                        {match_range(SK, EK, Tree0), SL}
                    end,
                    TestRanges
                )
            end
        ),
    {TCRange1, RL1} =
        timer:tc(
            fun() ->
                lists:map(
                    fun({SK, EK, SL}) ->
                        {match_range(SK, EK, Tree1), SL}
                    end,
                    TestRanges
                )
            end
        ),
    lists:foreach(
        fun({R, Exp}) ->
            ?assertMatch(Exp, R)
        end,
        RL0
    ),
    lists:foreach(
        fun({R, Exp}) ->
            ?assertMatch(Exp, R)
        end,
        RL1
    ),
    io:format(
        user,
        "Matched 1000 ~w ranges in both trees in ~w microseconds~n",
        [Size, TCRange0 + TCRange1]
    ).

get_random_range(KL, RangeType, MaxSize) ->
    L = length(KL),
    R = rand:uniform(L - 5),
    RangeSize = min(max(4, rand:uniform(L - R)), MaxSize),
    SL = lists:sublist(KL, R, RangeSize),
    case {RangeType, lists:last(SL)} of
        {exact, LastKV} ->
            {element(1, hd(SL)), element(1, LastKV), SL};
        {over, {{o_kv, B, FullKey, null}, _LastV}} ->
            LastKey =
                {
                    o_kv,
                    B,
                    list_to_binary(binary_to_list(FullKey) ++ "0"),
                    null
                },
            {element(1, hd(SL)), LastKey, SL};
        {over, {K, _V}} ->
            LastKey = list_to_binary(binary_to_list(K) ++ "0"),
            {element(1, hd(SL)), LastKey, SL}
    end.

tree_matchrange_test() ->
    matchrange_test_by_type(tree),
    extra_matchrange_test_by_type(tree).

idxt_matchrange_test() ->
    matchrange_test_by_type(idxt),
    extra_matchrange_test_by_type(idxt).

matchrange_test_by_type(Type) ->
    N = 4000,
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    Tree0 = from_orderedlist(KL, Type),

    FirstKey = element(1, lists:nth(1, KL)),
    FinalKey = element(1, lists:last(KL)),
    PenultimateKey = element(1, lists:nth(length(KL) - 1, KL)),
    AfterFirstKey =
        setelement(
            3,
            FirstKey,
            list_to_binary(binary_to_list(element(3, FirstKey)) ++ "0")
        ),
    AfterPenultimateKey =
        setelement(
            3,
            PenultimateKey,
            list_to_binary(binary_to_list(element(3, PenultimateKey)) ++ "0")
        ),

    LengthR =
        fun(SK, EK, T) ->
            length(match_range(SK, EK, T))
        end,

    KL_Length = length(KL),
    io:format("KL_Length ~w~n", [KL_Length]),
    ?assertMatch(KL_Length, LengthR(FirstKey, FinalKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(FirstKey, PenultimateKey, Tree0) + 1),
    ?assertMatch(1, LengthR(all, FirstKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(all, PenultimateKey, Tree0) + 1),
    ?assertMatch(KL_Length, LengthR(all, all, Tree0)),
    ?assertMatch(2, LengthR(PenultimateKey, FinalKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(AfterFirstKey, PenultimateKey, Tree0) + 2),
    ?assertMatch(1, LengthR(AfterPenultimateKey, FinalKey, Tree0)).

extra_matchrange_test_by_type(Type) ->
    N = 4000,
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    Tree0 = from_orderedlist(KL, Type),

    SubL = lists:sublist(KL, 2000, 3100),
    RangeLists =
        lists:map(
            fun(P) -> lists:sublist(SubL, P, P + 50) end,
            lists:seq(1, 50)
        ),
    TestRangeLFun =
        fun(RangeL) ->
            SKeyV = lists:nth(1, RangeL),
            EKeyV = lists:nth(50, RangeL),
            {{o_kv, SB, SK, null}, _SV} = SKeyV,
            {{o_kv, EB, EK, null}, _EV} = EKeyV,
            SRangeK =
                {o_kv, SB, list_to_binary(binary_to_list(SK) ++ "0"), null},
            ERangeK =
                {o_kv, EB, list_to_binary(binary_to_list(EK) ++ "0"), null},
            ?assertMatch(49, length(match_range(SRangeK, ERangeK, Tree0)))
        end,
    lists:foreach(TestRangeLFun, RangeLists).

extra_searchrange_test_by_type(Type) ->
    N = 4000,
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    SearchKL = convertkeylist(KL, []),
    % Each {K, V} in the convert list is now an {EK, SK} or a range
    Tree0 = from_orderedlist(SearchKL, Type),

    SubL = lists:sublist(KL, 2000, 3100),

    SKFun = fun(V) -> V end,

    TestRangeLFun =
        fun(P) ->
            RangeL = lists:sublist(SubL, P, P + 50),
            % If P is odd, the range keys will be between a start key and an
            % end key.
            % If P is even, the range keys will be between an end key and a
            % start key
            SKeyV = lists:nth(1, RangeL),
            EKeyV = lists:nth(50, RangeL),
            {{o_kv, SB, SK, null}, _SV} = SKeyV,
            {{o_kv, EB, EK, null}, _EV} = EKeyV,
            FRangeK =
                {o_kv, SB, list_to_binary(binary_to_list(SK) ++ "0"), null},
            BRangeK =
                {o_kv, EB, list_to_binary(binary_to_list(EK) ++ "0"), null},
            ?assertMatch(
                25, length(search_range(FRangeK, BRangeK, Tree0, SKFun))
            )
        end,
    lists:foreach(TestRangeLFun, lists:seq(1, 50)).

convertkeylist(KeyList, Acc) when length(KeyList) < 2 ->
    lists:reverse(Acc);
convertkeylist(KeyList, Acc) ->
    [{SK, _SV} | OddTail] = KeyList,
    [{EK, _EV} | EvenTail] = OddTail,
    convertkeylist(EvenTail, [{EK, SK} | Acc]).

match_fun(Tree) ->
    fun({K, V}) ->
        ?assertMatch({value, V}, match(K, Tree))
    end.

search_exactmatch_fun(Tree) ->
    StartKeyFun = fun(_V) -> all end,
    fun({K, V}) ->
        ?assertMatch({K, V}, search(K, Tree, StartKeyFun))
    end.

search_nearmatch_fun(Tree) ->
    StartKeyFun = fun(_V) -> all end,
    fun({K, {NK, NV}}) ->
        ?assertMatch({NK, NV}, search(K, Tree, StartKeyFun))
    end.

empty_test() ->
    T0 = empty(tree),
    ?assertMatch(0, tsize(T0)),
    T2 = empty(idxt),
    ?assertMatch(0, tsize(T2)).

search_range_idx_test() ->
    Tree =
        {idxt, 1, {
            {[
                {
                    {o_rkv, <<"Bucket1">>, <<"Key1">>, null},
                    leveled_pmanifest:new_entry(
                        {o_rkv, <<"Bucket">>, <<"Key9083">>, null},
                        {o_rkv, <<"Bucket1">>, <<"Key1">>, null},
                        list_to_pid("<0.320.0>"),
                        "./16_1_6.sst",
                        none
                    )
                }
            ]},
            gb_trees:from_orddict(
                [{{o_rkv, <<"Bucket1">>, <<"Key1">>, null}, 1}]
            )
        }},
    R =
        search_range(
            {o_rkv, <<"Bucket">>, null, null},
            {o_rkv, <<"Bucket">>, null, null},
            Tree,
            fun leveled_pmanifest:entry_startkey/1
        ),
    ?assertMatch(1, length(R)).

-endif.
