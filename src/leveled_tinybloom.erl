%% -------- TinyBloom ---------
%%
%% A fixed size bloom that supports 128 keys only, made to try and minimise
%% the cost of producing the bloom
%%


-module(leveled_tinybloom).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
            create_bloom/1,
            check_hash/2
            ]).

-define(BITS_PER_KEY, 8). % Must be 8 or 4
-define(INTEGER_SIZE, ?BITS_PER_KEY * 8). 
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
        L when L > 32 ->
            add_hashlist(HashList,
                            15,
                            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 0, 0, 0);
        L when L > 16 ->
            add_hashlist(HashList, 3, 0, 0, 0, 0);
        _ ->
            add_hashlist(HashList, 1, 0, 0)
    end.

-spec check_hash(integer(), binary()) -> boolean().
%% @doc
%% Check for the presence of a given hash within a bloom
check_hash(_Hash, <<>>) ->
    false;
check_hash(Hash, BloomBin) ->
    SlotSplit = (byte_size(BloomBin) div ?BITS_PER_KEY) - 1,
    {Slot, H0, H1} = split_hash(Hash, SlotSplit),
    Mask = get_mask(H0, H1),
    Pos = Slot * ?BITS_PER_KEY,
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
    H1 = (Hash bsr 10) band (?BAND_MASK),
    H3 = (Hash bsr 16) band (?BAND_MASK),
    H4 = (Hash bsr 22) band (?BAND_MASK),
    Slot0 = (Hash bsr 28) band SlotSplit,
    {Slot bxor Slot0, H0 bxor H3, H1 bxor H4}.

get_mask(H0, H1) ->
    case H0 == H1 of
        true ->
            1 bsl H0;
        false ->
            (1 bsl H0) + (1 bsl H1)
    end.


%% This looks ugly and clunky, but in tests it was quicker than modifying an
%% Erlang term like an array as it is passed around the loop

add_hashlist([], _S, S0, S1) ->
    IntSize = ?INTEGER_SIZE,
    <<S0:IntSize/integer, S1:IntSize/integer>>;
add_hashlist([TopHash|T], SlotSplit, S0, S1) ->
    {Slot, H0, H1} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(H0, H1),
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
add_hashlist([TopHash|T], SlotSplit, S0, S1, S2, S3) ->
    {Slot, H0, H1} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(H0, H1),
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

add_hashlist([], _S, S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                                                    SA, SB, SC, SD, SE, SF) ->
    IntSize = ?INTEGER_SIZE,
    <<S0:IntSize/integer, S1:IntSize/integer,
        S2:IntSize/integer, S3:IntSize/integer,
        S4:IntSize/integer, S5:IntSize/integer,
        S6:IntSize/integer, S7:IntSize/integer,
        S8:IntSize/integer, S9:IntSize/integer,
        SA:IntSize/integer, SB:IntSize/integer,
        SC:IntSize/integer, SD:IntSize/integer,
        SE:IntSize/integer, SF:IntSize/integer>>;
add_hashlist([TopHash|T],
                SlotSplit,
                S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                SA, SB, SC, SD, SE, SF) ->
    {Slot, H0, H1} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(H0, H1),
    case Slot of
        0 ->
            add_hashlist(T,
                            SlotSplit,
                            S0 bor Mask, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        1 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1 bor Mask, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        2 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2 bor Mask, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        3 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3 bor Mask, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        4 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4 bor Mask, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        5 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5 bor Mask, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        6 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6 bor Mask, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        7 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7 bor Mask, S8, S9,
                            SA, SB, SC, SD, SE, SF);
        8 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8 bor Mask, S9,
                            SA, SB, SC, SD, SE, SF);
        9 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9 bor Mask,
                            SA, SB, SC, SD, SE, SF);
        10 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA bor Mask, SB, SC, SD, SE, SF);
        11 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB bor Mask, SC, SD, SE, SF);
        12 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC bor Mask, SD, SE, SF);
        13 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD bor Mask, SE, SF);
        14 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE bor Mask, SF);
        15 ->
            add_hashlist(T,
                            SlotSplit,
                            S0, S1, S2, S3, S4, S5, S6, S7, S8, S9,
                            SA, SB, SC, SD, SE, SF bor Mask)
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
    BRand = leveled_rand:uniform(BRange),
    BNumber = string:right(integer_to_list(BucketLow + BRand), 4, $0),
    KNumber = string:right(integer_to_list(leveled_rand:uniform(10000)), 6, $0),
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BNumber, "Key" ++ KNumber, o),
    Chunk = leveled_rand:rand_bytes(64),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{LK, MV}|Acc],
                        BucketLow,
                        BRange).


get_hashlist(N) ->
    KVL0 = lists:ukeysort(1, generate_randomkeys(1, N * 2, 1, 20)),
    KVL = lists:sublist(KVL0, N),
    HashFun =
        fun({K, _V}) ->
            leveled_codec:magic_hash(K)
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

bloom_test() ->
    test_bloom(128),
    test_bloom(64),
    test_bloom(32),
    test_bloom(16),
    test_bloom(8).

test_bloom(N) ->
    HashList1 = get_hashlist(N),
    HashList2 = get_hashlist(N),
    HashList3 = get_hashlist(N),
    HashList4 = get_hashlist(N),
    
    SWa = os:timestamp(),
    BloomBin1 = create_bloom(HashList1),
    BloomBin2 = create_bloom(HashList2),
    BloomBin3 = create_bloom(HashList3),
    BloomBin4 = create_bloom(HashList4),
    TSa = timer:now_diff(os:timestamp(), SWa),
    
    SWb = os:timestamp(),
    check_all_hashes(BloomBin1, HashList1),
    check_all_hashes(BloomBin2, HashList2),
    check_all_hashes(BloomBin3, HashList3),
    check_all_hashes(BloomBin4, HashList4),
    TSb = timer:now_diff(os:timestamp(), SWb),
     
    HashPool = get_hashlist(N * 2),
    HashListOut1 = lists:sublist(lists:subtract(HashPool, HashList1), N),
    HashListOut2 = lists:sublist(lists:subtract(HashPool, HashList2), N),
    HashListOut3 = lists:sublist(lists:subtract(HashPool, HashList3), N),
    HashListOut4 = lists:sublist(lists:subtract(HashPool, HashList4), N),
    
    SWc = os:timestamp(),
    C0 = {0, 0},
    C1 = check_neg_hashes(BloomBin1, HashListOut1, C0),
    C2 = check_neg_hashes(BloomBin2, HashListOut2, C1),
    C3 = check_neg_hashes(BloomBin3, HashListOut3, C2),
    C4 = check_neg_hashes(BloomBin4, HashListOut4, C3),
    {Pos, Neg} = C4,
    FPR = Pos / (Pos + Neg),
    TSc = timer:now_diff(os:timestamp(), SWc),
    
    io:format(user,
                "Test with size ~w has microsecond timings: -"
                    ++ " build ~w check ~w neg_check ~w and fpr ~w~n",
                [N, TSa, TSb, TSc, FPR]).



-endif.
