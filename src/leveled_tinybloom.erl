%% -------- TinyBloom ---------
%%
%% A fixed size bloom that supports 128 keys only, made to try and minimise
%% the cost of producing the bloom
%%


-module(leveled_tinybloom).

-include("include/leveled.hrl").

-define(TWO_POWER,
            list_to_tuple(
                lists:reverse(
                    element(2,
                        lists:foldl(
                                fun(_I, {AccLast, AccList}) ->
                                    {AccLast * 2,
                                        [(AccLast * 2)|AccList]}
                                end,
                            {1, [1]},
                            lists:seq(2, 32))
                        )
                    )
                )
            ).

-include_lib("eunit/include/eunit.hrl").

-export([
            create_bloom/1,
            check_hash/2
            ]).


%%%============================================================================
%%% API
%%%============================================================================


create_bloom(HashList) ->
    SlotSplit = 
        case length(HashList) of
            L when L > 64 ->
                15;
            L when L > 32 ->
                7;
            L when L > 16 ->
                3;
            _ ->
                1
        end,
    add_hashlist(HashList,
                    array:new([{size, SlotSplit + 1}, {default, 0}]),
                    SlotSplit).

check_hash(Hash, BloomBin) ->
    SlotSplit = (byte_size(BloomBin) div 4) - 1,
    {Slot, H0, H1} = split_hash(Hash, SlotSplit),
    Mask = get_mask(H0, H1),
    Pos = Slot * 4,
    <<_H:Pos/binary, CheckInt:32/integer, _T/binary>> = BloomBin,
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
    H0 = (Hash bsr 4) band 31,
    H1 = (Hash bsr 9) band 31,
    H3 = (Hash bsr 14) band 31,
    H4 = (Hash bsr 19) band 31,
    {Slot, H0 bxor H3, H1 bxor H4}.

get_mask(H0, H1) ->
    case H0 == H1 of
        true ->
            element(H0 + 1, ?TWO_POWER);
        false ->
            element(H0 + 1, ?TWO_POWER) + element(H1 + 1, ?TWO_POWER)
    end.

add_hashlist([], SlotArray, SlotSplit) ->
    BuildBinFun =
        fun(I, Acc) ->
            Bloom = array:get(I, SlotArray),
            <<Acc/binary, Bloom:32/integer>>
        end,
    lists:foldl(BuildBinFun, <<>>, lists:seq(0, SlotSplit));
add_hashlist([TopHash|T], SlotArray, SlotSplit) ->
    {Slot, H0, H1} = split_hash(TopHash, SlotSplit),
    Mask = get_mask(H0, H1),
    I = array:get(Slot, SlotArray),
    add_hashlist(T, array:set(Slot, I bor Mask, SlotArray), SlotSplit).


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
    BRand = random:uniform(BRange),
    BNumber = string:right(integer_to_list(BucketLow + BRand), 4, $0),
    KNumber = string:right(integer_to_list(random:uniform(10000)), 6, $0),
    LedgerKey = leveled_codec:to_ledgerkey("Bucket" ++ BNumber,
                                            "Key" ++ KNumber,
                                            o),
    {_B, _K, KV, _H} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                        Seqn,
                                                        crypto:rand_bytes(64),
                                                        64,
                                                        infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [KV|Acc],
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

bloom_test() ->
    test_bloom(128),
    test_bloom(64),
    test_bloom(32).


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
    
    case N of
        128 ->
            ?assertMatch(64, byte_size(BloomBin1)),
            ?assertMatch(64, byte_size(BloomBin2)),
            ?assertMatch(64, byte_size(BloomBin3)),
            ?assertMatch(64, byte_size(BloomBin4));
        _ ->
            ok
    end,
    
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

twopower_test() ->
    ?assertMatch(1, element(1, ?TWO_POWER)),
    ?assertMatch(128, element(8, ?TWO_POWER)),
    ?assertMatch(2147483648, element(32, ?TWO_POWER)).



-endif.