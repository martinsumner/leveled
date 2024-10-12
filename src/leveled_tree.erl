%% -------- TREE ---------
%%
%% This module is intended to address two issues
%% - the lack of iterator_from support in OTP16 gb_trees
%% - the time to convert from/to list in gb_trees
%%
%% Leveled had had a skiplist implementation previously, and this is a 
%% variation on that.  The Treein this case is a bunch of sublists of length
%% SKIP_WIDTH with the start_keys in a gb_tree. 

-module(leveled_tree).

-include("leveled.hrl").

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

-define(SKIP_WIDTH, 16).

-type tree_type() :: tree|idxt|skpl. 
-type leveled_tree() :: {tree_type(), integer(), any()}.

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

-spec from_orderedset(ets:tab(), tree_type(), integer()|auto)
                                                            -> leveled_tree().
%% @doc
%% Convert an ETS table of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.  The SkipWidth is an integer representing
%% the underlying list size joined in the tree (the trees are all trees of
%% lists of this size).  For the skpl type the width can be auto-sized based
%% on the length
from_orderedset(Table, Type, SkipWidth) ->
    from_orderedlist(ets:tab2list(Table), Type, SkipWidth).

-spec from_orderedlist(list(tuple()), tree_type()) -> leveled_tree().
%% @doc
%% Convert a list of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.
from_orderedlist(OrderedList, Type) ->
    from_orderedlist(OrderedList, Type, ?SKIP_WIDTH).

-spec from_orderedlist(list(tuple()), tree_type(), integer()|auto)
                                                            -> leveled_tree().
%% @doc
%% Convert a list of Keys and Values (of table type ordered_set) into a
%% leveled_tree of the given type.  The SkipWidth is an integer representing
%% the underlying list size joined in the tree (the trees are all trees of
%% lists of this size).  For the skpl type the width can be auto-sized based
%% on the length
from_orderedlist(OrderedList, tree, SkipWidth) ->
    L = length(OrderedList),
    {tree, L, tree_fromorderedlist(OrderedList, [], L, SkipWidth)};
from_orderedlist(OrderedList, idxt, SkipWidth) ->
    L = length(OrderedList),
    {idxt, L, idxt_fromorderedlist(OrderedList, {[], [], 1}, L, SkipWidth)};
from_orderedlist(OrderedList, skpl, _SkipWidth) ->
    L = length(OrderedList),
    SkipWidth =
        % Autosize the skip width
        case L of
            L when L > 4096 -> 32;
            L when L > 512 -> 16;
            L when L > 64 -> 8;
            _ -> 4
        end,
    {skpl, L, skpl_fromorderedlist(OrderedList, L, SkipWidth, 2)}.
    
-spec match(tuple()|integer(), leveled_tree()) -> none|{value, any()}.
%% @doc
%% Return the value from a tree associated with an exact match for the given
%% key.  This assumes the tree contains the actual keys and values to be
%% macthed against, not a manifest representing ranges of keys and values.
match(Key, {tree, _L, Tree}) ->
    Iter = tree_iterator_from(Key, Tree),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            lookup_match(Key, SL)
    end;
match(Key, {idxt, _L, {TLI, IDX}}) when is_tuple(TLI) ->
    Iter = tree_iterator_from(Key, IDX),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, ListID, _Iter} ->
            lookup_match(Key, element(ListID, TLI))
    end;
match(Key, {skpl, _L, SkipList}) ->
    SL0 = skpl_getsublist(Key, SkipList),
    lookup_match(Key, SL0).

-spec search(tuple()|integer(), leveled_tree(), fun()) -> none|tuple().
%% @doc
%% Search is used when the tree is a manifest of key ranges and it is necessary
%% to find a rnage which may contain the key.  The StartKeyFun is used if the
%% values contain extra information that can be used to determine if the key is
%% or is not present.
search(Key, {tree, _L, Tree}, StartKeyFun) ->
    Iter = tree_iterator_from(Key, Tree),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            {K, V} = lookup_best(Key, SL),
            case Key < StartKeyFun(V) of
                true ->
                    none;
                false ->
                    {K, V}
            end
    end;
search(Key, {idxt, _L, {TLI, IDX}}, StartKeyFun) when is_tuple(TLI) ->
    Iter = tree_iterator_from(Key, IDX),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, ListID, _Iter} ->
            {K, V} = lookup_best(Key, element(ListID, TLI)),
            case Key < StartKeyFun(V) of
                true ->
                    none;
                false ->
                    {K, V}
            end
    end;
search(Key, {skpl, _L, SkipList}, StartKeyFun) ->
    SL0 = skpl_getsublist(Key, SkipList),
    case lookup_best(Key, SL0) of
        {K, V} ->
            case Key < StartKeyFun(V) of
                true ->
                    none;
                false ->
                    {K, V}
            end;
        none ->
            none
    end.

-spec match_range(tuple()|integer()|all,
                    tuple()|integer()|all,
                    leveled_tree())
                                 -> list().
%% @doc
%% Return a range of value between trees from a tree associated with an
%% exact match for the given key.  This assumes the tree contains the actual
%% keys and values to be macthed against, not a manifest representing ranges
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

-spec match_range(tuple()|integer()|all,
                    tuple()|integer()|all,
                    leveled_tree(),
                    fun())
                                 -> list().
%% @doc
%% As match_range/3 but a function can be passed to be used when comparing the
%5 EndKey with a key in the tree (such as leveled_codec:endkey_passed), where
%% Erlang term comparison will not give the desired result.
match_range(StartRange, EndRange, {tree, _L, Tree}, EndRangeFun) ->
    treelookup_range_start(StartRange, EndRange, Tree, EndRangeFun);
match_range(StartRange, EndRange, {idxt, _L, Tree}, EndRangeFun) ->
    idxtlookup_range_start(StartRange, EndRange, Tree, EndRangeFun);
match_range(StartRange, EndRange, {skpl, _L, SkipList}, EndRangeFun) ->
    skpllookup_to_range(StartRange, EndRange, SkipList, EndRangeFun).

-spec search_range(tuple()|integer()|all,
                    tuple()|integer()|all,
                    leveled_tree(),
                    fun())
                                -> list().
%% @doc
%% Extract a range from a tree, with search used when the tree is a manifest
%% of key ranges and it is necessary to find a rnage which may encapsulate the
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
        {tree, _L, T} ->
            treelookup_range_start(StartRange, EndRange, T, EndRangeFun);
        {idxt, _L, T} ->
            idxtlookup_range_start(StartRange, EndRange, T, EndRangeFun);
        {skpl, _L, SL} ->
            skpllookup_to_range(StartRange, EndRange, SL, EndRangeFun)
    end.

-spec to_list(leveled_tree()) -> list().
%% @doc
%% Collapse the tree back to a list
to_list({tree, _L, Tree}) ->
    FoldFun =
        fun({_MK, SL}, Acc) ->
            Acc ++ SL
        end,
    lists:foldl(FoldFun, [], tree_to_list(Tree));
to_list({idxt, _L, {TLI, _IDX}}) when is_tuple(TLI) ->
    lists:append(tuple_to_list(TLI));
to_list({skpl, _L, SkipList}) when is_list(SkipList) ->
    FoldFun = 
        fun({_M, SL}, Acc) ->
            [SL|Acc]
        end,
    Lv1List = lists:reverse(lists:foldl(FoldFun, [], SkipList)),
    Lv0List = lists:reverse(lists:foldl(FoldFun, [], lists:append(Lv1List))),
    lists:append(Lv0List).


-spec tsize(leveled_tree()) -> integer().
%% @doc
%% Return the count of items in a tree
tsize({_Type, L, _Tree}) ->
    L.

-spec empty(tree_type()) -> leveled_tree().
%% @doc
%% Return an empty tree of the given type
empty(tree) ->
    {tree, 0, empty_tree()};
empty(idxt) ->
    {idxt, 0, {{}, empty_tree()}};
empty(skpl) ->
    {skpl, 0, []}.

%%%============================================================================
%%% Internal Functions
%%%============================================================================


tree_fromorderedlist([], TmpList, _L, _SkipWidth) ->
    gb_trees:from_orddict(lists:reverse(TmpList));
tree_fromorderedlist(OrdList, TmpList, L, SkipWidth) ->
    SubLL = min(SkipWidth, L),
    {Head, Tail} = lists:split(SubLL, OrdList),
    {LastK, _LastV} = lists:last(Head),
    tree_fromorderedlist(Tail, [{LastK, Head}|TmpList], L - SubLL, SkipWidth).
    
idxt_fromorderedlist([], {TmpListElements, TmpListIdx, _C}, _L, _SkipWidth) ->
    {list_to_tuple(lists:reverse(TmpListElements)),
        gb_trees:from_orddict(lists:reverse(TmpListIdx))};
idxt_fromorderedlist(OrdList, {TmpListElements, TmpListIdx, C}, L, SkipWidth) ->
    SubLL = min(SkipWidth, L),
    {Head, Tail} = lists:split(SubLL, OrdList),
    {LastK, _LastV} = lists:last(Head),
    idxt_fromorderedlist(Tail,
                            {[Head|TmpListElements],
                                [{LastK, C}|TmpListIdx],
                                C + 1},
                            L - SubLL,
                            SkipWidth).

skpl_fromorderedlist(SkipList, _L, _SkipWidth, 0) ->
    SkipList;
skpl_fromorderedlist(SkipList, L, SkipWidth, Height) ->
    SkipList0 = roll_list(SkipList, L, [], SkipWidth),
    skpl_fromorderedlist(SkipList0, length(SkipList0), SkipWidth, Height - 1).

roll_list([], 0, SkipList, _SkipWidth) ->
    lists:reverse(SkipList);
roll_list(KVList, L, SkipList, SkipWidth) ->
    SubLL = min(SkipWidth, L),
    {Head, Tail} = lists:split(SubLL, KVList),
    {LastK, _LastV} = lists:last(Head),
    roll_list(Tail, L - SubLL, [{LastK, Head}|SkipList], SkipWidth).



% lookup_match(_Key, []) ->
%     none;
% lookup_match(Key, [{EK, _EV}|_Tail]) when EK > Key ->
%     none;
% lookup_match(Key, [{Key, EV}|_Tail]) ->
%     {value, EV};
% lookup_match(Key, [_Top|Tail]) ->
%     lookup_match(Key, Tail).

lookup_match(Key, KVList) ->
    case lists:keyfind(Key, 1, KVList) of 
        false ->
            none;
        {Key, Value} ->
            {value, Value}
    end.

lookup_best(_Key, []) ->
    none;
lookup_best(Key, [{EK, EV}|_Tail]) when EK >= Key ->
    {EK, EV};
lookup_best(Key, [_Top|Tail]) ->
    lookup_best(Key, Tail).

treelookup_range_start(StartRange, EndRange, Tree, EndRangeFun) ->
    Iter0 = tree_iterator_from(StartRange, Tree),
    case tree_next(Iter0) of
        none ->
            [];
        {NK, SL, Iter1} ->
            PredFun =
                fun({K, _V}) ->
                    K < StartRange
                end,
            {_LHS, RHS} = lists:splitwith(PredFun, SL),
            treelookup_range_end(EndRange, {NK, RHS}, Iter1, [], EndRangeFun)
    end.

treelookup_range_end(EndRange, {NK0, SL0}, Iter0, Output, EndRangeFun) ->
    PredFun =
        fun({K, _V}) ->
            not leveled_codec:endkey_passed(EndRange, K)
        end,
    case leveled_codec:endkey_passed(EndRange, NK0) of
        true ->
            {LHS, RHS} = lists:splitwith(PredFun, SL0),
            [{FirstRHSKey, FirstRHSValue}|_Rest] = RHS,
            case EndRangeFun(EndRange, FirstRHSKey, FirstRHSValue) of
                true ->
                    Output ++ LHS ++ [{FirstRHSKey, FirstRHSValue}];
                false ->
                    Output ++ LHS
            end;
        false ->
            UpdOutput = Output ++ SL0,
            case tree_next(Iter0) of
                none ->
                    UpdOutput;
                {NK1, SL1, Iter1} ->
                    treelookup_range_end(EndRange,
                                            {NK1, SL1},
                                            Iter1,
                                            UpdOutput,
                                            EndRangeFun)
            end 
    end.

idxtlookup_range_start(StartRange, EndRange, {TLI, IDX}, EndRangeFun) ->
    % TLI tuple of lists, IDS is a gb_tree of End Keys mapping to tuple 
    % indexes
    Iter0 = tree_iterator_from(StartRange, IDX),
    case tree_next(Iter0) of
        none ->
            [];
        {NK, ListID, Iter1} ->
            PredFun =
                fun({K, _V}) ->
                    K < StartRange
                end,
            {_LHS, RHS} = lists:splitwith(PredFun, element(ListID, TLI)),
            % The RHS is the list of {EK, SK} elements where the EK >=  the 
            % StartRange, otherwise the LHS falls before the range
            idxtlookup_range_end(EndRange, {TLI, NK, RHS}, Iter1, [], EndRangeFun)
    end.

idxtlookup_range_end(EndRange, {TLI, NK0, SL0}, Iter0, Output, EndRangeFun) ->
    PredFun =
        fun({K, _V}) ->
            not leveled_codec:endkey_passed(EndRange, K)
            % true if EndRange is after K
        end,
    case leveled_codec:endkey_passed(EndRange, NK0) of
        true ->
            % The end key of this list is after the end of the range, so no
            % longer interested in any of the rest of the tree - just this 
            % sublist
            {LHS, RHS} = lists:splitwith(PredFun, SL0),
            % Split the {EK, SK} pairs based on the EndRange.  Note that the 
            % last key is passed the end range - so the RHS cannot be empty, it
            % must at least include the last key (as NK0 is at the end of SL0).
            [{FirstRHSKey, FirstRHSValue}|_Rest] = RHS,
            case EndRangeFun(EndRange, FirstRHSKey, FirstRHSValue) of
                true ->
                    % The start key is not after the end of the range
                    % and so this should be included in the range
                    Output ++ LHS ++ [{FirstRHSKey, FirstRHSValue}];
                false ->
                    % the start key of the next key is after the end
                    % of the range and so should not be included
                    Output ++ LHS
            end;
        false ->
            UpdOutput = Output ++ SL0,
            case tree_next(Iter0) of
                none ->
                    UpdOutput;
                {NK1, ListID, Iter1} ->
                    idxtlookup_range_end(EndRange,
                                            {TLI, NK1, element(ListID, TLI)},
                                            Iter1,
                                            UpdOutput,
                                            EndRangeFun)
            end 
    end.


skpllookup_to_range(StartRange, EndRange, SkipList, EndRangeFun) ->
    FoldFun =
        fun({K, SL}, {PassedStart, PassedEnd, Acc}) ->
            case {PassedStart, PassedEnd} of
                {false, false} ->
                    case StartRange > K of
                        true ->
                            {PassedStart, PassedEnd, Acc};
                        false ->
                            case leveled_codec:endkey_passed(EndRange, K) of
                                true ->
                                    {true, true, [SL|Acc]};
                                false ->
                                    {true, false, [SL|Acc]}
                            end
                    end;
                {true, false} ->
                    case leveled_codec:endkey_passed(EndRange, K) of
                        true ->
                            {true, true, [SL|Acc]};
                        false ->
                            {true, false, [SL|Acc]}
                    end;
                {true, true} ->
                    {PassedStart, PassedEnd, Acc}
            end
        end,
    Lv1List = lists:reverse(element(3,
                                    lists:foldl(FoldFun,
                                                {false, false, []},
                                                SkipList))),
    Lv0List = lists:reverse(element(3,
                                    lists:foldl(FoldFun,
                                                {false, false, []},
                                                lists:append(Lv1List)))),
    BeforeFun =
        fun({K, _V}) ->
            K < StartRange
        end,
    AfterFun =
        fun({K, V}) ->
            case leveled_codec:endkey_passed(EndRange, K) of
                false ->
                    true;
                true ->
                    EndRangeFun(EndRange, K, V)
            end
        end,
    
    case length(Lv0List) of
        0 ->
            [];
        1 ->
            RHS = lists:dropwhile(BeforeFun, lists:nth(1, Lv0List)),
            lists:takewhile(AfterFun, RHS);
        2 ->
            RHSofLHL = lists:dropwhile(BeforeFun, lists:nth(1, Lv0List)),
            LHSofRHL = lists:takewhile(AfterFun, lists:last(Lv0List)),
            RHSofLHL ++ LHSofRHL;
        L ->
            RHSofLHL = lists:dropwhile(BeforeFun, lists:nth(1, Lv0List)),
            LHSofRHL = lists:takewhile(AfterFun, lists:last(Lv0List)),
            MidLists = lists:sublist(Lv0List, 2, L - 2),
            lists:append([RHSofLHL] ++ MidLists ++ [LHSofRHL])
    end.


skpl_getsublist(Key, SkipList) ->
    FoldFun =
        fun({Mark, SL}, Acc) ->
            case {Acc, Mark} of
                {[], Mark} when Mark >= Key ->
                    SL;
                _ ->
                    Acc
            end
        end,
    SL1 = lists:foldl(FoldFun, [], SkipList),
    lists:foldl(FoldFun, [], SL1).

%%%============================================================================
%%% Balance tree implementation
%%%============================================================================

empty_tree() ->
    gb_trees:empty().

tree_to_list(T) ->
    gb_trees:to_list(T).

tree_iterator_from(K, T) ->
    % For OTP 16 compatibility with gb_trees
    iterator_from(K, T).

tree_next(I) ->
    % For OTP 16 compatibility with gb_trees
    next(I).


iterator_from(S, {_, T}) ->
    iterator_1_from(S, T).

iterator_1_from(S, T) ->
    iterator_from(S, T, []).

iterator_from(S, {K, _, _, T}, As) when K < S ->
    iterator_from(S, T, As);
iterator_from(_, {_, _, nil, _} = T, As) ->
    [T | As];
iterator_from(S, {_, _, L, _} = T, As) ->
    iterator_from(S, L, [T | As]);
iterator_from(_, nil, As) ->
    As.

next([{X, V, _, T} | As]) ->
    {X, V, iterator(T, As)};
next([]) ->
    none.

%% The iterator structure is really just a list corresponding to
%% the call stack of an in-order traversal. This is quite fast.

iterator({_, _, nil, _} = T, As) ->
    [T | As];
iterator({_, _, L, _} = T, As) ->
    iterator(L, [T | As]);
iterator(nil, As) ->
    As.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        [],
                        BucketRangeLow,
                        BucketRangeHigh).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BRand = rand:uniform(BRange),
    BNumber =
        lists:flatten(
            io_lib:format("K~4..0B", [BucketLow + BRand])),
    KNumber =
        lists:flatten(
            io_lib:format("K~8..0B", [rand:uniform(1000)])),
    {K, V} =
        {{o_kv,
            {<<"btype">>, list_to_binary("Bucket" ++ BNumber)},
            list_to_binary("Key" ++ KNumber),
            null},
            Seqn},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{K, V}|Acc],
                        BucketLow,
                        BRange).

generate_simplekeys(Seqn, Count) ->
    generate_simplekeys(Seqn, Count, []).

generate_simplekeys(_Seqn, 0, Acc) ->
    Acc;
generate_simplekeys(Seqn, Count, Acc) ->
    KNumber =
        list_to_binary(
            lists:flatten(
                io_lib:format("K~8..0B", [rand:uniform(100000)]))),
    generate_simplekeys(Seqn + 1, Count - 1, [{KNumber, Seqn}|Acc]).


tree_search_test() ->
    search_test_by_type(tree),
    extra_searchrange_test_by_type(tree).

idxt_search_test() ->
    search_test_by_type(idxt),
    extra_searchrange_test_by_type(idxt).

skpl_search_test() ->
    search_test_by_type(skpl),
    extra_searchrange_test_by_type(skpl).

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
    io:format(user, "10 range tests with type ~w in ~w microseconds~n",
                [Type, T1]).


tree_oor_test() ->
    outofrange_test_by_type(tree).

idxt_oor_test() ->
    outofrange_test_by_type(idxt).

skpl_oor_test() ->
    outofrange_test_by_type(skpl).

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

skpl_tolist_test() ->
    tolist_test_by_type(skpl).

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

timing_tests_skpl_test_() ->
    {timeout, 60, fun skpl_timing/0}.

tree_timing() ->
    log_tree_test_by_(16, tree, 8000),
    log_tree_test_by_(16, tree, 4000),
    log_tree_test_by_(4, tree, 256).

idxt_timing() ->
    log_tree_test_by_(16, idxt, 8000),
    log_tree_test_by_(16, idxt, 4000),
    log_tree_test_by_(4, idxt, 256),
    log_tree_test_by_(16, idxt, 256),
    log_tree_test_by_simplekey_(16, idxt, 256).

skpl_timing() ->
    log_tree_test_by_(auto, skpl, 8000),
    log_tree_test_by_(auto, skpl, 4000),
    log_tree_test_by_simplekey_(auto, skpl, 4000),
    log_tree_test_by_(auto, skpl, 512),
    log_tree_test_by_simplekey_(auto, skpl, 512),
    log_tree_test_by_(auto, skpl, 256),
    log_tree_test_by_simplekey_(auto, skpl, 256).

log_tree_test_by_(Width, Type, N) ->
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    SW = os:timestamp(),
    tree_test_by_(Width, Type, KL),
    io:format(user, "Test took ~w ms",
                [timer:now_diff(os:timestamp(), SW) div 1000]).

log_tree_test_by_simplekey_(Width, Type, N) ->
    KL = lists:ukeysort(1, generate_simplekeys(1, N)),
    SW = os:timestamp(),
    tree_test_by_(Width, Type, KL, false),
    io:format(user, "Test with simple key took ~w ms",
                [timer:now_diff(os:timestamp(), SW) div 1000]).

tree_test_by_(Width, Type, KL) ->
    tree_test_by_(Width, Type, KL, true).

tree_test_by_(Width, Type, KL, ComplexKey) ->
    io:format(
        user,
        "~n~nTree test with complexkey=~w for type and width: ~w ~w~n",
        [ComplexKey, Type, Width]),

    OS = ets:new(test, [ordered_set, private]),
    ets:insert(OS, KL),
    SWaETS = os:timestamp(),
    Tree0 = from_orderedset(OS, Type, Width),
    io:format(user, "Generating tree from ETS in ~w microseconds" ++
                        " of size ~w~n",
                [timer:now_diff(os:timestamp(), SWaETS),
                    tsize(Tree0)]),
    io:format(user,
        "Tree has footprint size ~w bytes flat_size ~w bytes~n",
        [erts_debug:size(Tree0) * 8, erts_debug:flat_size(Tree0) * 8]),
    
    SWaGSL = os:timestamp(),
    Tree1 = from_orderedlist(KL, Type, Width),
    io:format(user, "Generating tree from orddict in ~w microseconds" ++
                        " of size ~w~n",
                [timer:now_diff(os:timestamp(), SWaGSL),
                    tsize(Tree1)]),
    io:format(user,
        "Tree has footprint size ~w bytes flat_size ~w bytes~n",
        [erts_debug:size(Tree1) * 8, erts_debug:flat_size(Tree1) * 8]),

    SWaLUP = os:timestamp(),
    lists:foreach(match_fun(Tree0), KL),
    lists:foreach(match_fun(Tree1), KL),
    io:format(user, "Looked up all keys twice in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaLUP)]),
    
    ?assertMatch(Tree0, Tree1),
    
    SWaSRCH1 = os:timestamp(),
    lists:foreach(search_exactmatch_fun(Tree0), KL),
    lists:foreach(search_exactmatch_fun(Tree1), KL),
    io:format(user, "Search all keys twice for exact match in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaSRCH1)]),
    
    BitBiggerKeyFun =
        case ComplexKey of
            true ->
                fun(Idx) ->
                    {K, _V} = lists:nth(Idx, KL),
                    {o_kv, B, FullKey, null} = K,
                    {{o_kv,
                        B,
                        list_to_binary(binary_to_list(FullKey) ++ "0"),
                        null},
                        lists:nth(Idx + 1, KL)}
                end;
            false ->
                fun(Idx) ->
                    {K, _V} = lists:nth(Idx, KL),
                    {list_to_binary(binary_to_list(K) ++ "0"),
                        lists:nth(Idx + 1, KL)}
                end
        end,            
    
    SrchKL = lists:map(BitBiggerKeyFun, lists:seq(1, length(KL) - 1)),
    
    SWaSRCH2 = os:timestamp(),
    lists:foreach(search_nearmatch_fun(Tree0), SrchKL),
    lists:foreach(search_nearmatch_fun(Tree1), SrchKL),
    io:format(user, "Search all keys twice for near match in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaSRCH2)]).


tree_matchrange_test() ->
    matchrange_test_by_type(tree),
    extra_matchrange_test_by_type(tree).

idxt_matchrange_test() ->
    matchrange_test_by_type(idxt),
    extra_matchrange_test_by_type(idxt).

skpl_matchrange_test() ->
    matchrange_test_by_type(skpl),
    extra_matchrange_test_by_type(skpl).


matchrange_test_by_type(Type) ->
    N = 4000,
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    Tree0 = from_orderedlist(KL, Type),
    
    FirstKey = element(1, lists:nth(1, KL)),
    FinalKey = element(1, lists:last(KL)),
    PenultimateKey = element(1, lists:nth(length(KL) - 1, KL)),
    AfterFirstKey =
        setelement(3,
        FirstKey,
        list_to_binary(binary_to_list(element(3, FirstKey)) ++ "0")),
    AfterPenultimateKey =
        setelement(3,
        PenultimateKey,
        list_to_binary(binary_to_list(element(3, PenultimateKey)) ++ "0")),
    
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
        lists:map(fun(P) -> lists:sublist(SubL, P, P + 50) end, 
                    lists:seq(1, 50)),
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
            ?assertMatch(25, length(search_range(FRangeK, BRangeK, Tree0, SKFun)))
        end,
    lists:foreach(TestRangeLFun, lists:seq(1, 50)).

convertkeylist(KeyList, Acc) when length(KeyList) < 2 ->
    lists:reverse(Acc);
convertkeylist(KeyList, Acc) -> 
    [{SK, _SV}|OddTail] = KeyList,
    [{EK, _EV}|EvenTail] = OddTail,
    convertkeylist(EvenTail, [{EK, SK}|Acc]).

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
    T1 = empty(skpl),
    ?assertMatch(0, tsize(T1)),
    T2 = empty(idxt),
    ?assertMatch(0, tsize(T2)).

search_range_idx_test() ->
    Tree = 
        {idxt,1,
            {{[{{o_rkv,"Bucket1","Key1",null},
                {manifest_entry,{o_rkv,"Bucket","Key9083",null},
                                {o_rkv,"Bucket1","Key1",null},
                                "<0.320.0>","./16_1_6.sst", none}}]},
                {1,{{o_rkv,"Bucket1","Key1",null},1,nil,nil}}}},
    StartKeyFun =
        fun(ME) ->
            ME#manifest_entry.start_key
        end,
    R = search_range({o_rkv, "Bucket", null, null}, 
                        {o_rkv, "Bucket", null, null}, 
                        Tree, 
                        StartKeyFun),
    ?assertMatch(1, length(R)).

-endif.
