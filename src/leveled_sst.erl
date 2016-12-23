%% -------- SST (Variant) ---------
%%
%% A FSM module intended to wrap a persisted, ordered view of Keys and Values
%%
%% The persisted view is built from a list (which may be created by merging
%% multiple lists)

-module(leveled_sst).

-include("include/leveled.hrl").

-define(SLOT_SIZE, 128).
-define(COMPRESSION_LEVEL, 1).

-include_lib("eunit/include/eunit.hrl").

-record(slot_index_value, {slot_id :: integer(),
                            bloom :: dict:dict(),
                            cache :: tuple(),
                            start_position :: integer(),
                            length :: integer()}).

%%%============================================================================
%%% API
%%%============================================================================




%%%============================================================================
%%% Internal Functions
%%%============================================================================

build_all_slots(KVList, BasePosition) ->
    build_all_slots(KVList, BasePosition, [], 1, []).

build_all_slots([], _Start, AllHashes, _SlotID, SlotIndex) ->
    {SlotIndex, AllHashes};
build_all_slots(KVList, StartPosition, AllHashes, SlotID, SlotIndex) ->
    {SlotList, KVRem} = lists:split(?SLOT_SIZE, KVList),
    {LastKey, _V} = lists:tail(SlotList),
    ExtractHashFun =
        fun({K, V}, Acc) ->
            {_SQN, H} = leveled_codec:strip_to_seqnhashonly({K, V}),
            case H of
                no_lookup ->
                    Acc;
                H ->
                    [{hash, H}|Acc]
            end
            end,
    HashList = lists:foldr(ExtractHashFun, [], KVList),
    {SlotBin, Bloom} = build_slot(KVList, HashList),
    Length = byte_size(SlotBin),
    SlotIndexV = #slot_index_value{slot_id = SlotID,
                                    bloom = Bloom,
                                    start_position = StartPosition,
                                    length = Length},
    build_all_slots(KVRem,
                    StartPosition + Length,
                    HashList ++ AllHashes,
                    SlotID + 1,
                    [{LastKey, SlotIndexV}|SlotIndex]).



build_slot(KVList, HashList) when length(KVList) =< ?SLOT_SIZE ->
    SkipList = leveled_skiplist:to_sstlist(KVList),
    Bloom = lists:foldr(fun leveled_tinybloom:tiny_enter/2,
                        leveled_tinybloom:tiny_empty(),
                        HashList),
    SlotBin = term_to_binary(SkipList, [{compressed, ?COMPRESSION_LEVEL}]),
    {SlotBin, Bloom}.

is_check_slot_required(_Hash, none) ->
    true;
is_check_slot_required(Hash, Bloom) ->
    leveled_tinybloom:tiny_check(Hash, Bloom).

lookup_in_slot(Key, {pointer, Handle, Pos, Length}) ->
    lookup_in_slot(Key, read_slot(Handle, Pos, Length));
lookup_in_slot(Key, SlotBin) ->
    SkipList = binary_to_term(SlotBin),
    leveled_skiplist:lookup(Key, SkipList).

range_from_slot(StartKey, EndKey, {pointer, Handle, Pos, Length}) ->
    range_from_slot(StartKey, EndKey, read_slot(Handle, Pos, Length));
range_from_slot(StartKey, EndKey, SlotBin) ->
    SkipList = binary_to_term(SlotBin),
    leveled_skiplist:to_range(SkipList, StartKey, EndKey).

all_from_slot({pointer, Handle, Pos, Length}) ->
    all_from_slot(read_slot(Handle, Pos, Length));
all_from_slot(SlotBin) ->
    SkipList = binary_to_term(SlotBin),
    leveled_skiplist:to_list(SkipList).


read_slot(_Handle, _Pos, _Length) ->
    not_yet_implemented.


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
    LedgerKey = leveled_codec:to_ledgerkey("Bucket" ++ BNumber,
                                            "Key" ++ KNumber,
                                            o),
    {_B, _K, KV} = leveled_codec:generate_ledgerkv(LedgerKey,
                                                    Seqn,
                                                    crypto:rand_bytes(64),
                                                    64,
                                                    infinity),
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [KV|Acc],
                        BucketLow,
                        BRange).


simple_slotbin_test() ->
    KVList0 = generate_randomkeys(1, ?SLOT_SIZE * 2, 1, 4),
    KVList1 = lists:sublist(lists:ukeysort(1, KVList0), 1, ?SLOT_SIZE),
    ExtractHashFun =
        fun({K, V}) ->
            {_SQN, H} = leveled_codec:strip_to_seqnhashonly({K, V}),
            {hash, H} end,
    HashList = lists:map(ExtractHashFun, KVList1),
    io:format(user, "~nSkiplist with bloom timing test~n", []),
    SW0 = os:timestamp(),
    {SlotBin0, Bloom0} = build_slot(KVList1, HashList),
    io:format(user, "Slot built in ~w microseconds with size ~w~n",
                [timer:now_diff(os:timestamp(), SW0), byte_size(SlotBin0)]),
    
    SW1 = os:timestamp(),
    lists:foreach(fun(H) -> ?assertMatch(true,
                                            is_check_slot_required(H, Bloom0))
                                            end,
                    HashList),
    lists:foreach(fun({K, V}) ->
                            ?assertMatch({value, V},
                                            lookup_in_slot(K, SlotBin0))
                                            end,
                    KVList1),
    io:format(user, "Slot checked for all keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW1)]),
    SW5 = os:timestamp(),
    leveled_skiplist:to_list(binary_to_term(SlotBin0)),
    io:format(user, "Skiplist flattened in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW5)]),
    
    
    io:format(user, "~ngb_tree comparison~n", []),
    SW2 = os:timestamp(),
    Bloom = lists:foldr(fun leveled_tinybloom:tiny_enter/2,
                        leveled_tinybloom:tiny_empty(),
                        HashList),
    Tree0 = gb_trees:from_orddict(KVList1),
    TreeBin = term_to_binary(Tree0, [{compressed, ?COMPRESSION_LEVEL}]),
    io:format(user, "Bloom and Tree created for all keys in ~w microseconds " ++
                "with size ~w~n",
                [timer:now_diff(os:timestamp(), SW2), byte_size(TreeBin)]),
    SW3 = os:timestamp(),
    lists:foreach(fun({K, V}) ->
                            ?assertMatch({value, V},
                                            gb_trees:lookup(K, binary_to_term(TreeBin)))
                                            end,
                    KVList1),
    io:format(user, "Tree checked for all keys in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW3)]),
    SW4 = os:timestamp(),
    gb_trees:to_list(binary_to_term(TreeBin)),
    io:format(user, "Tree flattened in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW4)]).


-endif.