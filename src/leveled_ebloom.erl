%% -------- TinyBloom ---------
%%
%% A 1-byte per key bloom filter with a 5% fpr.  Pre-prepared segment hashes
%% (a leveled codec type) are, used for building and checking - the filter
%% splits a single hash into a 1 byte slot identifier, and 2 x 12 bit hashes
%% (so k=2, although only a single hash is used).
%% 
%% The filter is designed to support a maximum of 64K keys, larger numbers of
%% keys will see higher fprs - with a 40% fpr at 250K keys.
%% 
%% The filter uses the second "Extra Hash" part of the segment-hash to ensure
%% no overlap of fpr with the leveled_sst find_pos function.
%% 
%% The completed bloom is a binary - to minimise the cost of copying between
%% processes and holding in memory.

-module(leveled_ebloom).

-export([
    create_bloom/1,
    check_hash/2
    ]).

-define(BLOOM_SLOTSIZE_BYTES, 512).
-define(INTEGER_SLICE_SIZE, 64).
-define(INTEGER_SLICES, 64).
    % i.e. ?INTEGER_SLICES * ?INTEGER_SLICE_SIZE = ?BLOOM_SLOTSIZE_BYTES div 8
-define(MASK_BSR, 6).
    % i.e. 2 ^ (12 - 6) = ?INTEGER_SLICES
-define(MASK_BAND, 63).
    % i.e. integer slize size - 1
-define(SPLIT_BAND, 4095).
    % i.e. (?BLOOM_SLOTSIZE_BYTES * 8) - 1

-type bloom() :: binary().

-export_type([bloom/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec create_bloom(list(leveled_codec:segment_hash())) -> bloom().
%% @doc
%% Create a binary bloom filter from a list of hashes.  In the leveled
%% implementation the hashes are leveled_codec:segment_hash/0 type, but only
%% a single 32-bit hash (the second element of the tuple is actually used in
%% the building of the bloom filter
create_bloom(HashList) ->
    SlotCount =
        case length(HashList) of
            0 ->
                0;
            L ->
                min(128, max(2, (L - 1) div 512))
        end,
    SlotHashes =
        map_hashes(
            HashList,
            list_to_tuple(lists:duplicate(SlotCount, [])),
            SlotCount
        ),
    build_bloom(SlotHashes, SlotCount).

-spec check_hash(leveled_codec:segment_hash(), bloom()) -> boolean().
%% @doc
%% Check for the presence of a given hash within a bloom. Only the second
%% element of the leveled_codec:segment_hash/0 type is used - a 32-bit hash.
check_hash(_Hash, <<>>) ->
    false;
check_hash({_SegHash, Hash}, BloomBin) when is_binary(BloomBin)->
    SlotSplit = byte_size(BloomBin) div ?BLOOM_SLOTSIZE_BYTES,
    {Slot, [H0, H1]} = split_hash(Hash, SlotSplit),
    Pos = ((Slot + 1) * ?BLOOM_SLOTSIZE_BYTES) - 1,
    case match_hash(BloomBin, Pos - (H0 div 8), H0 rem 8) of
        true ->
            match_hash(BloomBin, Pos - (H1 div 8), H1 rem 8);
        _ ->
            false
    end.
    
%%%============================================================================
%%% Internal Functions
%%%============================================================================

-type slot_count() :: 0|2..128.
-type bloom_hash() :: 0..16#FFF.
-type external_hash() :: 0..16#FFFFFFFF.

-spec map_hashes(
        list(leveled_codec:segment_hash()), tuple(), slot_count()) -> tuple().
map_hashes([], HashListTuple,  _SlotCount) ->
    HashListTuple;
map_hashes([Hash|Rest], HashListTuple, SlotCount) ->
    {Slot, [H0, H1]} = split_hash(element(2, Hash), SlotCount),
    SlotHL = element(Slot + 1, HashListTuple),
    map_hashes(
        Rest,
        setelement(Slot + 1, HashListTuple, [H0, H1 | SlotHL]),
        SlotCount).

-spec split_hash(external_hash(), slot_count())
        -> {non_neg_integer(), [bloom_hash()]}.
split_hash(Hash, SlotSplit) ->
    Slot = (Hash band 255) rem SlotSplit,
    H0 = (Hash bsr 8) band ?SPLIT_BAND,
    H1 = (Hash bsr 20) band ?SPLIT_BAND,
    {Slot, [H0, H1]}.

-spec match_hash(bloom(), non_neg_integer(), 0..16#FF) -> boolean().
match_hash(BloomBin, Pos, Hash) ->
    <<_Pre:Pos/binary, CheckInt:8/integer, _Rest/binary>> = BloomBin,
    (CheckInt bsr Hash) band 1 == 1.

-spec build_bloom(tuple(), slot_count()) -> bloom().
build_bloom(_SlotHashes, 0) ->
    <<>>;
build_bloom(SlotHashes, SlotCount) when SlotCount > 0 ->
    lists:foldr(
        fun(I, AccBin) ->
            HashList = element(I, SlotHashes),
            SlotBin =
                add_hashlist(
                    lists:usort(HashList), 0, 1, ?INTEGER_SLICES, <<>>),
            <<SlotBin/binary, AccBin/binary>>
        end,
        <<>>,
        lists:seq(1, SlotCount)
    ).

-spec add_hashlist(
        list(bloom_hash()),
        non_neg_integer(),
        non_neg_integer(),
        0..?INTEGER_SLICES,
        binary()) -> bloom().
add_hashlist([], ThisSlice, SliceCount, SliceCount, AccBin) ->
    <<ThisSlice:?INTEGER_SLICE_SIZE/integer, AccBin/binary>>;
add_hashlist([], ThisSlice, SliceNumber, SliceCount, AccBin) ->
    add_hashlist(
        [],
        0,
        SliceNumber + 1,
        SliceCount,
        <<ThisSlice:?INTEGER_SLICE_SIZE/integer, AccBin/binary>>);
add_hashlist([H0|Rest], ThisSlice, SliceNumber, SliceCount, AccBin)
        when ((H0 bsr ?MASK_BSR) + 1) == SliceNumber ->
    Mask0 = 1 bsl (H0 band (?MASK_BAND)),
    add_hashlist(
        Rest, ThisSlice bor Mask0, SliceNumber, SliceCount, AccBin);
add_hashlist(Rest, ThisSlice, SliceNumber, SliceCount, AccBin) ->
    add_hashlist(
        Rest,
        0,
        SliceNumber + 1,
        SliceCount,
        <<ThisSlice:?INTEGER_SLICE_SIZE/integer, AccBin/binary>>).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

generate_orderedkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_orderedkeys(Seqn, Count, [], BucketRangeLow, BucketRangeHigh).

generate_orderedkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_orderedkeys(Seqn, Count, Acc, BucketLow, BucketHigh) ->
    BNumber = Seqn div (BucketHigh - BucketLow),
    BucketExt =
        io_lib:format("K~4..0B", [BucketLow + BNumber]),
    KeyExt =
        io_lib:format("K~8..0B", [Seqn * 100 + leveled_rand:uniform(100)]),
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BucketExt, "Key" ++ KeyExt, o),
    Chunk = leveled_rand:rand_bytes(16),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    generate_orderedkeys(
        Seqn + 1, Count - 1, [{LK, MV}|Acc], BucketLow, BucketHigh).

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
    ?assertMatch(
        {0, 4}, check_neg_hashes(BloomBin0, [0, 10, 100, 100000], {0, 0})).

bloom_test_() ->
    {timeout, 120, fun bloom_test_ranges/0}.

bloom_test_ranges() ->
    test_bloom(250000, 2),
    test_bloom(80000, 4),
    test_bloom(60000, 4),
    test_bloom(40000, 4),
    test_bloom(128 * 256, 4),
    test_bloom(20000, 4),
    test_bloom(10000, 4),
    test_bloom(5000, 4),
    test_bloom(2000, 4),
    test_bloom(1000, 4).

test_bloom(N, Runs) ->
    ListOfHashLists = 
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),
    SpliListFun =
        fun(HashList) -> 
            HitOrMissFun = 
                fun (Entry, {HitL, MissL}) ->
                    case leveled_rand:uniform() < 0.5 of
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
        lists:map(
            fun({HL, _ML}) -> create_bloom(HL) end, SplitListOfHashLists),
    TSa = timer:now_diff(os:timestamp(), SWa)/Runs,
    
    SWb = os:timestamp(),
    PosChecks =
        lists:foldl(
            fun(Nth, ChecksMade) ->
                {HL, _ML} = lists:nth(Nth, SplitListOfHashLists),
                BB = lists:nth(Nth, ListOfBlooms),
                check_all_hashes(BB, HL),
                ChecksMade + length(HL)
            end,
            0,
            lists:seq(1, Runs)),
    TSb = timer:now_diff(os:timestamp(), SWb),

    SWc = os:timestamp(),
    {Pos, Neg} = 
        lists:foldl(
            fun(Nth, Acc) ->
                {_HL, ML} = lists:nth(Nth, SplitListOfHashLists),
                BB = lists:nth(Nth, ListOfBlooms),
                check_neg_hashes(BB, ML, Acc)
            end,
            {0, 0},
            lists:seq(1, Runs)),
    FPR = Pos / (Pos + Neg),
    TSc = timer:now_diff(os:timestamp(), SWc),

    BytesPerKey =
        (lists:sum(lists:map(fun byte_size/1, ListOfBlooms)) div 4) / N,

    io:format(
        user,
        "Test with size ~w has microsecond timings: - "
            "build in ~w then ~.3f per pos-check, ~.3f per neg-check, "
            "fpr ~.3f with bytes-per-key ~.3f~n",
        [N, round(TSa), TSb / PosChecks, TSc / (Pos + Neg), FPR, BytesPerKey]).


split_builder_speed_test_() ->
    {timeout, 60, fun split_builder_speed_tester/0}.

split_builder_speed_tester() ->
    N = 40000,
    Runs = 50,
    ListOfHashLists = 
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),

    Timings =
        lists:map(
            fun(HashList) ->
                SlotCount = min(128, max(2, (length(HashList) - 1) div 512)),
                InitTuple = list_to_tuple(lists:duplicate(SlotCount, [])),
                {MTC, SlotHashes} =
                    timer:tc(
                        fun map_hashes/3, [HashList, InitTuple, SlotCount]
                    ),
                {BTC, _Bloom} =
                    timer:tc(
                        fun build_bloom/2, [SlotHashes, SlotCount]
                    ),
                {MTC, BTC}
            end,
            ListOfHashLists
        ),
    {MTs, BTs} = lists:unzip(Timings),
    io:format(
        user,
        "Total time in microseconds for map_hashlist ~w build_bloom ~w~n",
        [lists:sum(MTs), lists:sum(BTs)]
    ).
    

-endif.
