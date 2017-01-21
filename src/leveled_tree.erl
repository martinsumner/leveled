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

-include("include/leveled.hrl").

-export([
        from_orderedlist/1,
        from_orderedset/1,
        to_list/1,
        match_range/3,
        search_range/4,
        match/2,
        search/3,
        tsize/1,
        empty/0
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SKIP_WIDTH, 16).


%%%============================================================================
%%% API
%%%============================================================================

from_orderedlist(OrderedList) ->
    L = length(OrderedList),
    {tree, L, from_orderedlist(OrderedList, [], L)}.

from_orderedset(Table) ->
    from_orderedlist(ets:tab2list(Table)).

match(Key, {tree, _L, Tree}) ->
    Iter = tree_iterator_from(Key, Tree),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            lookup_match(Key, SL)
    end.

search(Key, {tree, _L, Tree}, StartKeyFun) ->
    Iter = tree_iterator_from(Key, Tree),
    case tree_next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            {K, V} = lookup_best(Key, SL),
            case K < StartKeyFun(V) of
                true ->
                    none;
                false ->
                    {K, V}
            end
    end.

match_range(StartRange, EndRange, {tree, _L, Tree}) ->
    EndRangeFun =
        fun(ER, FirstRHSKey, _FirstRHSValue) ->
            ER == FirstRHSKey
        end,
    lookup_range_start(StartRange, EndRange, Tree, EndRangeFun).


search_range(StartRange, EndRange, {tree, _L, Tree}, StartKeyFun) ->
    EndRangeFun =
        fun(ER, _FirstRHSKey, FirstRHSValue) ->
            StartRHSKey = StartKeyFun(FirstRHSValue),
            ER >= StartRHSKey 
        end,
    lookup_range_start(StartRange, EndRange, Tree, EndRangeFun).

to_list({tree, _L, Tree}) ->
    FoldFun =
        fun({_MK, SL}, Acc) ->
            Acc ++ SL
        end,
    lists:foldl(FoldFun, [], tree_to_list(Tree)).

tsize({tree, L, _Tree}) ->
    L.

empty() ->
    {tree, 0, empty_tree()}.

%%%============================================================================
%%% Internal Functions
%%%============================================================================


from_orderedlist([], TmpList, _L) ->
    gb_trees:from_orddict(lists:reverse(TmpList));
from_orderedlist(OrdList, TmpList, L) ->
    SubLL = min(?SKIP_WIDTH, L),
    {Head, Tail} = lists:split(SubLL, OrdList),
    {LastK, _LastV} = lists:last(Head),
    from_orderedlist(Tail, [{LastK, Head}|TmpList], L - SubLL).
    
lookup_match(_Key, []) ->
    none;
lookup_match(Key, [{EK, _EV}|_Tail]) when EK > Key ->
    none;
lookup_match(Key, [{Key, EV}|_Tail]) ->
    {value, EV};
lookup_match(Key, [_Top|Tail]) ->
    lookup_match(Key, Tail).

lookup_best(Key, [{EK, EV}|_Tail]) when EK >= Key ->
    {EK, EV};
lookup_best(Key, [_Top|Tail]) ->
    lookup_best(Key, Tail).

lookup_range_start(StartRange, EndRange, Tree, EndRangeFun) ->
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
            lookup_range_end(EndRange, {NK, RHS}, Iter1, [], EndRangeFun)
    end.

lookup_range_end(EndRange, {NK0, SL0}, Iter0, Output, EndRangeFun) ->
    PredFun =
        fun({K, _V}) ->
            not leveled_codec:endkey_passed(EndRange, K)
        end,
    case leveled_codec:endkey_passed(EndRange, NK0) of
        true ->
            {LHS, RHS} = lists:splitwith(PredFun, SL0),
            case RHS of
                [] ->
                    Output ++ LHS;
                [{FirstRHSKey, FirstRHSValue}|_Rest] ->
                    case EndRangeFun(EndRange, FirstRHSKey, FirstRHSValue) of
                        true ->
                            Output ++ LHS ++ [{FirstRHSKey, FirstRHSValue}];
                        false ->
                            Output ++ LHS
                    end
            end;
        false ->
            UpdOutput = Output ++ SL0,
            case tree_next(Iter0) of
                none ->
                    UpdOutput;
                {NK1, SL1, Iter1} ->
                    lookup_range_end(EndRange,
                                        {NK1, SL1},
                                        Iter1,
                                        UpdOutput,
                                        EndRangeFun)
            end 
    end.

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

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        [],
                        BucketRangeLow,
                        BucketRangeHigh).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BNumber =
        case BRange of
            0 ->
                string:right(integer_to_list(BucketLow), 4, $0);
            _ ->
                BRand = random:uniform(BRange),
                string:right(integer_to_list(BucketLow + BRand), 4, $0)
        end,
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{K, V}|Acc],
                        BucketLow,
                        BRange).


tree_search_test() ->
    MapFun =
        fun(N) ->
            {N * 4, N * 4 - 2}
        end,
    KL = lists:map(MapFun, lists:seq(1, 50)),
    T = from_orderedlist(KL),
    
    StartKeyFun = fun(V) -> V end,
    
    SW = os:timestamp(),
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
    io:format(user, "10 range tests in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)]).
    
    
tree_test() ->
    N = 4000,
    KL = lists:ukeysort(1, generate_randomkeys(1, N, 1, N div 5)),
    
    OS = ets:new(test, [ordered_set, private]),
    ets:insert(OS, KL),
    SWaETS = os:timestamp(),
    Tree0 = from_orderedset(OS),
    io:format(user, "Generating tree from ETS in ~w microseconds" ++
                        " of size ~w~n",
                [timer:now_diff(os:timestamp(), SWaETS),
                    tsize(Tree0)]),
    
    SWaGSL = os:timestamp(),
    Tree1 = from_orderedlist(KL),
    io:format(user, "Generating tree from orddict in ~w microseconds" ++
                        " of size ~w~n",
                [timer:now_diff(os:timestamp(), SWaGSL),
                    tsize(Tree1)]),
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
        fun(Idx) ->
            {K, _V} = lists:nth(Idx, KL),
            {o, B, FullKey, null} = K,
            {{o, B, FullKey ++ "0", null}, lists:nth(Idx + 1, KL)}
        end,
    SrchKL = lists:map(BitBiggerKeyFun, lists:seq(1, length(KL) - 1)),
    
    SWaSRCH2 = os:timestamp(),
    lists:foreach(search_nearmatch_fun(Tree0), SrchKL),
    lists:foreach(search_nearmatch_fun(Tree1), SrchKL),
    io:format(user, "Search all keys twice for near match in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SWaSRCH2)]),

    FirstKey = element(1, lists:nth(1, KL)),
    FinalKey = element(1, lists:last(KL)),
    PenultimateKey = element(1, lists:nth(length(KL) - 1, KL)),
    AfterFirstKey = setelement(3, FirstKey, element(3, FirstKey) ++ "0"),
    AfterPenultimateKey = setelement(3,
                                    PenultimateKey,
                                    element(3, PenultimateKey) ++ "0"),
    
    LengthR =
        fun(SK, EK, T) ->
            length(match_range(SK, EK, T))
        end,
    
    KL_Length = length(KL),
    ?assertMatch(KL_Length, LengthR(FirstKey, FinalKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(FirstKey, PenultimateKey, Tree0) + 1),
    ?assertMatch(1, LengthR(all, FirstKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(all, PenultimateKey, Tree0) + 1),
    ?assertMatch(KL_Length, LengthR(all, all, Tree0)),
    ?assertMatch(2, LengthR(PenultimateKey, FinalKey, Tree0)),
    ?assertMatch(KL_Length, LengthR(AfterFirstKey, PenultimateKey, Tree0) + 2),
    ?assertMatch(1, LengthR(AfterPenultimateKey, FinalKey, Tree0)).

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

-endif.