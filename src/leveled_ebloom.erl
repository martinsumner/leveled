%% -------- TinyBloom ---------
%%
%% A fixed size bloom that supports 32K keys only, made to try and minimise
%% the cost of producing the bloom
%%


-module(leveled_ebloom).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
            create_bloom/1,
            check_hash/2
            ]).

-define(BLOOM_SIZE_BYTES, 2048). 
-define(INTEGER_SIZE, 16384). 
-define(BAND_MASK, ?INTEGER_SIZE - 1). 


%%%============================================================================
%%% API
%%%============================================================================

-spec create_bloom(list(integer())) -> binary().
%% @doc
%% Create a binary bloom filter from alist of hashes
create_bloom(HashList) ->
    case length(HashList) of
        0 ->
            <<>>;
        L when L > 16384 ->
            add_hashlist(HashList,
                            7,
                            0, 0, 0, 0, 0, 0, 0, 0);
        L when L > 8192 ->
            add_hashlist(HashList, 3, 0, 0, 0, 0);
        _ ->
            add_hashlist(HashList, 1, 0, 0)
    end.


-spec check_hash(integer(), binary()) -> boolean().
%% @doc
%% Check for the presence of a given hash within a bloom
check_hash(_Hash, <<>>) ->
    false;
check_hash({_SegHash, Hash}, BloomBin) ->
    SlotSplit = (byte_size(BloomBin) div ?BLOOM_SIZE_BYTES) - 1,
    {Slot, Hashes} = split_hash(Hash, SlotSplit),
    Mask = get_mask(Hashes),
    Pos = Slot * ?BLOOM_SIZE_BYTES,
    IntSize = ?INTEGER_SIZE,
    <<_H:Pos/binary, CheckInt:IntSize/integer, _T/binary>> = BloomBin,
    case CheckInt band Mask of
        Mask ->
            true;
        _ ->
            false
    end.
    
%%%============================================================================
%%% Internal Functions
%%%============================================================================

split_hash(Hash, SlotSplit) ->
    Slot = Hash band SlotSplit,
    H0 = (Hash bsr 4) band (?BAND_MASK),
    H1 = (Hash bsr 18) band (?BAND_MASK),
    % H2 = (Hash bsr 34) band (?BAND_MASK),
    % H3 = (Hash bsr 49) band (?BAND_MASK),
    {Slot, [H0, H1 
                %, H2, H3
                ]}.

get_mask([H0, H1
            %, H2, H3
            ]) ->
    (1 bsl H0) bor (1 bsl H1) 
        % bor (1 bsl H2) bor (1 bsl H3)
        .


%% This looks ugly and clunky, but in tests it was quicker than modifying an
%% Erlang term like an array as it is passed around the loop

add_hashlist([], _S, S0, S1) ->
    IntSize = ?INTEGER_SIZE,
    <<S0:IntSize/integer, S1:IntSize/integer>>;
add_hashlist([{_SegHash, TopHash}|T], SlotSplit, S0, S1) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T, SlotSplit, S0 bor Mask, S1);
        1 ->
            add_hashlist(T, SlotSplit, S0, S1 bor Mask)
    end.

add_hashlist([], _S, S0, S1, S2, S3) ->
     IntSize = ?INTEGER_SIZE,
     <<S0:IntSize/integer, S1:IntSize/integer,
        S2:IntSize/integer, S3:IntSize/integer>>;
add_hashlist([{_SegHash, TopHash}|T], SlotSplit, S0, S1, S2, S3) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T, SlotSplit, S0 bor Mask, S1, S2, S3);
        1 ->
            add_hashlist(T, SlotSplit, S0, S1 bor Mask, S2, S3);
        2 ->
            add_hashlist(T, SlotSplit, S0, S1, S2 bor Mask, S3);
        3 ->
            add_hashlist(T, SlotSplit, S0, S1, S2, S3 bor Mask)
    end.

add_hashlist([], _S, S0, S1, S2, S3, S4, S5, S6, S7) ->
    IntSize = ?INTEGER_SIZE,
    <<S0:IntSize/integer, S1:IntSize/integer,
        S2:IntSize/integer, S3:IntSize/integer,
        S4:IntSize/integer, S5:IntSize/integer,
        S6:IntSize/integer, S7:IntSize/integer>>;
add_hashlist([{_SegHash, TopHash}|T],
                SlotSplit,
                S0, S1, S2, S3, S4, S5, S6, S7) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T,
                            SlotSplit,
                            S0 bor Mask, S1, S2, S3, S4, S5, S6, S7);
        1 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1 bor Mask, S2, S3, S4, S5, S6, S7);
        2 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2 bor Mask, S3, S4, S5, S6, S7);
        3 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3 bor Mask, S4, S5, S6, S7);
        4 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4 bor Mask, S5, S6, S7);
        5 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5 bor Mask, S6, S7);
        6 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6 bor Mask, S7);
        7 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7 bor Mask)
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_orderedkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_orderedkeys(Seqn,
                            Count,
                            [],
                            BucketRangeLow,
                            BucketRangeHigh).

generate_orderedkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_orderedkeys(Seqn, Count, Acc, BucketLow, BucketHigh) ->
    BNumber = Seqn div (BucketHigh - BucketLow),
    BucketExt = string:right(integer_to_list(BucketLow + BNumber), 4, $0),
    KNumber = Seqn * 100 + leveled_rand:uniform(100),
    KeyExt = 
        string:right(integer_to_list(KNumber), 8, $0),
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BucketExt, "Key" ++ KeyExt, o),
    Chunk = leveled_rand:rand_bytes(16),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    generate_orderedkeys(Seqn + 1,
                        Count - 1,
                        [{LK, MV}|Acc],
                        BucketLow,
                        BucketHigh).


get_hashlist(N) ->
    KVL = generate_orderedkeys(1, N, 1, 20),
    HashFun =
        fun({K, _V}) ->
            leveled_codec:segment_hash(K)
        end,
    lists:map(HashFun, KVL).

check_all_hashes(BloomBin, HashList) ->
    CheckFun =
        fun(Hash) ->
            ?assertMatch(true, check_hash(Hash, BloomBin))
        end,
    lists:foreach(CheckFun, HashList).
        
check_neg_hashes(BloomBin, HashList, Counters) ->
    CheckFun =
        fun(Hash, {AccT, AccF}) ->
            case check_hash(Hash, BloomBin) of
                true ->
                    {AccT + 1, AccF};
                false ->
                    {AccT, AccF + 1}
            end
        end,
    lists:foldl(CheckFun, Counters, HashList).


empty_bloom_test() ->
    BloomBin0 = create_bloom([]),
    ?assertMatch({0, 4},
                    check_neg_hashes(BloomBin0, [0, 10, 100, 100000], {0, 0})).

bloom_test_() ->
    {timeout, 60, fun bloom_test_ranges/0}.

bloom_test_ranges() ->
    test_bloom(40000, 2),
    test_bloom(128 * 256, 10),
    test_bloom(20000, 2),
    test_bloom(10000, 2),
    test_bloom(5000, 2).

test_bloom(N, Runs) ->
    ListOfHashLists = 
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),
    SpliListFun =
        fun(HashList) -> 
            HitOrMissFun = 
                fun (Entry, {HitL, MissL}) ->
                    case random:uniform() < 0.5 of
                        true -> 
                            {[Entry|HitL], MissL};
                        false ->
                            {HitL, [Entry|MissL]}
                    end 
                end,
            lists:foldl(HitOrMissFun, {[], []}, HashList)
        end,
    SplitListOfHashLists = lists:map(SpliListFun, ListOfHashLists),

    SWa = os:timestamp(),
    ListOfBlooms =
        lists:map(fun({HL, _ML}) -> create_bloom(HL) end, 
                    SplitListOfHashLists),
    TSa = timer:now_diff(os:timestamp(), SWa)/Runs,
    
    SWb = os:timestamp(),
    lists:foreach(fun(Nth) ->
                        {HL, _ML} = lists:nth(Nth, SplitListOfHashLists),
                        BB = lists:nth(Nth, ListOfBlooms),
                        check_all_hashes(BB, HL)
                     end,
                     lists:seq(1, Runs)),
    TSb = timer:now_diff(os:timestamp(), SWb)/Runs,
    
    SWc = os:timestamp(),
    {Pos, Neg} = 
        lists:foldl(fun(Nth, Acc) ->
                            {_HL, ML} = lists:nth(Nth, SplitListOfHashLists),
                            BB = lists:nth(Nth, ListOfBlooms),
                            check_neg_hashes(BB, ML, Acc)
                        end,
                        {0, 0},
                        lists:seq(1, Runs)),
    FPR = Pos / (Pos + Neg),
    TSc = timer:now_diff(os:timestamp(), SWc)/Runs,
    
    io:format(user,
                "Test with size ~w has microsecond timings: -"
                    ++ " build ~w check ~w neg_check ~w and fpr ~w~n",
                [N, TSa, TSb, TSc, FPR]).


-endif.
