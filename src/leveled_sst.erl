%% -------- SST (Variant) ---------
%%
%% A FSM module intended to wrap a persisted, ordered view of Keys and Values
%%
%% The persisted view is built from a list (which may be created by merging
%% multiple lists).
%%
%%
%% -------- Slots ---------
%%
%% The view is built from sublists referred to as slot.  Each slot is up to 128
%% keys and values in size.  The slots are each themselves a gb_tree.  The
%% gb_tree is slightly slower than the skiplist at fetch time, and doesn't
%% support directly the useful to_range function.  However the from_orddict
%% capability is much faster than from_sortedlist in skiplist, saving on CPU
%% at sst build time:
%%
%% Skiplist:
%% build and serialise slot 3233 microseconds
%% de-serialise and check * 128 - 14669 microseconds
%% flatten back to list - 164 microseconds
%%
%% GBTree:
%% build and serialise tree 402 microseconds
%% de-serialise and check * 128 - 15263 microseconds
%% flatten back to list - 175 microseconds
%%
%% The performance advantage at lookup time is no negligible as the time to
%% de-deserialise for each check is dominant.  This time grows linearly with
%% the size of the slot, wherease the serialisation time is relatively constant
%% with growth.  So bigger slots would be quicker to build, but the penalty for
%% that speed is too high at lookup time.


-module(leveled_sst).

-include("include/leveled.hrl").

-define(SLOT_SIZE, 128).
-define(COMPRESSION_LEVEL, 1).

-include_lib("eunit/include/eunit.hrl").

-record(slot_index_value, {slot_id :: integer(),
                            bloom :: dict:dict(),
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
    Tree = gb_trees:from_orddict(KVList),
    Bloom = lists:foldr(fun leveled_tinybloom:tiny_enter/2,
                        leveled_tinybloom:tiny_empty(),
                        HashList),
    SlotBin = term_to_binary(Tree, [{compressed, ?COMPRESSION_LEVEL}]),
    {SlotBin, Bloom}.

is_check_slot_required(_Hash, none) ->
    true;
is_check_slot_required(Hash, Bloom) ->
    leveled_tinybloom:tiny_check(Hash, Bloom).

lookup_in_slot(Key, {pointer, Handle, Pos, Length}) ->
    lookup_in_slot(Key, read_slot(Handle, Pos, Length));
lookup_in_slot(Key, SlotBin) ->
    Tree = binary_to_term(SlotBin),
    gb_trees:lookup(Key, Tree).

all_from_slot({pointer, Handle, Pos, Length}) ->
    all_from_slot(read_slot(Handle, Pos, Length));
all_from_slot(SlotBin) ->
    SkipList = binary_to_term(SlotBin),
    gb_trees:to_list(SkipList).


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
    SW2 = os:timestamp(),
    ?assertMatch(KVList1, all_from_slot(SlotBin0)),
    io:format(user, "Slot flattened in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW2)]).
    
    
-endif.