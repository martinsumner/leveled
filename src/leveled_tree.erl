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
        % search_range/3,
        match/2,
        search/2,
        tsize/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SKIP_WIDTH, 32).


%%%============================================================================
%%% API
%%%============================================================================

from_orderedlist(OrderedList) ->
    L = length(OrderedList),
    {tree, L, from_orderedlist(OrderedList, gb_trees:empty(), L)}.

from_orderedset(Table) ->
    from_orderedlist(ets:tab2list(Table)).

match(Key, {tree, _L, Tree}) ->
    Iter = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            lookup_match(Key, SL)
    end.

match_range(StartKey, EndKey, {tree, _L, Tree}) ->
    Iter0 = gb_trees:iterator_from(StartKey, Tree),
    case gb_trees:next(Iter0) of
        none ->
            [];
        {NK, SL, Iter1} ->
            PredFun =
                fun({K, _V}) ->
                    K < StartKey
                end,
            {_LHS, RHS} = lists:splitwith(PredFun, SL),
            lookup_match_range(EndKey, {NK, RHS}, Iter1, [])
    end.
    
search(Key, {tree, _L, Tree}) ->
    Iter = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(Iter) of
        none ->
            none;
        {_NK, SL, _Iter} ->
            lookup_best(Key, SL)
    end.

to_list({tree, _L, Tree}) ->
    FoldFun =
        fun({_MK, SL}, Acc) ->
            Acc ++ SL
        end,
    lists:foldl(FoldFun, [], gb_trees:to_list(Tree)).

tsize({tree, L, _Tree}) ->
    L.

%%%============================================================================
%%% Internal Functions
%%%============================================================================


from_orderedlist([], Tree, _L) ->
    Tree;
from_orderedlist(OrdList, Tree, L) ->
    SubLL = min(?SKIP_WIDTH, L),
    {Head, Tail} = lists:split(SubLL, OrdList),
    {LastK, _LastV} = lists:last(Head),
    from_orderedlist(Tail, gb_trees:insert(LastK, Head, Tree), L - SubLL).
    
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

lookup_match_range(EndKey, {NK0, SL0}, Iter0, Output) ->
    PredFun =
        fun({K, _V}) ->
            not leveled_codec:endkey_passed(EndKey, K)
        end,
    case leveled_codec:endkey_passed(EndKey, NK0) of
        true ->
            {LHS, RHS} = lists:splitwith(PredFun, SL0),
            case RHS of
                [{EndKey, FirstValue}|_Tail] ->
                    Output ++ LHS ++ [{EndKey, FirstValue}];
                _ ->
                    Output ++ LHS
            end;
        false ->
            UpdOutput = Output ++ SL0,
            case gb_trees:next(Iter0) of
                none ->
                    UpdOutput;
                {NK1, SL1, Iter1} ->
                    lookup_match_range(EndKey, {NK1, SL1}, Iter1, UpdOutput)
            end 
    end.


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
    fun({K, V}) ->
        ?assertMatch({K, V}, search(K, Tree))
    end.

search_nearmatch_fun(Tree) ->
    fun({K, {NK, NV}}) ->
        ?assertMatch({NK, NV}, search(K, Tree))
    end.

-endif.