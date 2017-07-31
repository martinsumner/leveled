%% -------- Key Codec ---------
%%
%% Functions for manipulating keys and values within leveled.
%%
%%
%% Within the LEDGER:
%% Keys are of the form -
%% {Tag, Bucket, Key, SubKey|null}
%% Values are of the form
%% {SQN, Status, MD}
%%
%% Within the JOURNAL:
%% Keys are of the form -
%% {SQN, LedgerKey}
%% Values are of the form
%% {Object, IndexSpecs} (as a binary)
%%
%% IndexSpecs are of the form of a Ledger Key/Value
%%
%% Tags need to be set during PUT operations and each Tag used must be
%% supported in an extract_metadata and a build_metadata_object function clause
%%
%% Currently the only tags supported are:
%% - o (standard objects)
%% - o_rkv (riak objects)
%% - i (index entries)


-module(leveled_codec).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
        inker_reload_strategy/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_keyseqonly/1,
        strip_to_seqnhashonly/1,
        striphead_to_details/1,
        is_active/3,
        endkey_passed/2,
        key_dominates/2,
        maybe_reap_expiredkey/2,
        to_ledgerkey/3,
        to_ledgerkey/5,
        from_ledgerkey/1,
        to_inkerkv/4,
        from_inkerkv/1,
        from_inkerkv/2,
        from_journalkey/1,
        compact_inkerkvc/2,
        split_inkvalue/1,
        check_forinkertype/2,
        maybe_compress/1,
        create_value_for_journal/2,
        build_metadata_object/2,
        generate_ledgerkv/5,
        get_size/2,
        get_keyandobjhash/2,
        idx_indexspecs/5,
        aae_indexspecs/6,
        generate_uuid/0,
        integer_now/0,
        riak_extract_metadata/2,
        magic_hash/1,
        to_lookup/1]).         

-define(V1_VERS, 1).
-define(MAGIC, 53). % riak_kv -> riak_object
-define(LMD_FORMAT, "~4..0w~2..0w~2..0w~2..0w~2..0w").
-define(NRT_IDX, "$aae.").
-define(ALL_BUCKETS, <<"$all">>).

-type recent_aae() :: #recent_aae{}.

-spec magic_hash(any()) -> integer().
%% @doc 
%% Use DJ Bernstein magic hash function. Note, this is more expensive than
%% phash2 but provides a much more balanced result.
%%
%% Hash function contains mysterious constants, some explanation here as to
%% what they are -
%% http://stackoverflow.com/questions/10696223/reason-for-5381-number-in-djb-hash-function
magic_hash({?RIAK_TAG, Bucket, Key, _SubKey}) ->
    magic_hash({Bucket, Key});
magic_hash({?STD_TAG, Bucket, Key, _SubKey}) ->
    magic_hash({Bucket, Key});
magic_hash(AnyKey) ->
    BK = term_to_binary(AnyKey),
    H = 5381,
    hash1(H, BK) band 16#FFFFFFFF.

hash1(H, <<>>) -> 
    H;
hash1(H, <<B:8/integer, Rest/bytes>>) ->
    H1 = H * 33,
    H2 = H1 bxor B,
    hash1(H2, Rest).

%% @doc
%% Should it be possible to lookup a key in the merge tree.  This is not true
%% For keys that should only be read through range queries.  Direct lookup
%% keys will have presence in bloom filters and other lookup accelerators. 
to_lookup(Key) ->
    case element(1, Key) of
        ?IDX_TAG ->
            no_lookup;
        _ ->
            lookup
    end.

-spec generate_uuid() -> list().
%% @doc
%% Generate a new globally unique ID as a string.
%% Credit to
%% https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = leveled_rand:rand_bytes(16),
    L = io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b", 
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]),
    binary_to_list(list_to_binary(L)).

inker_reload_strategy(AltList) ->
    ReloadStrategy0 = [{?RIAK_TAG, retain}, {?STD_TAG, retain}],
    lists:foldl(fun({X, Y}, SList) ->
                        lists:keyreplace(X, 1, SList, {X, Y})
                        end,
                    ReloadStrategy0,
                    AltList).

strip_to_statusonly({_, {_, St, _, _}}) -> St.

strip_to_seqonly({_, {SeqN, _, _, _}}) -> SeqN.

strip_to_keyseqonly({LK, {SeqN, _, _, _}}) -> {LK, SeqN}.

strip_to_seqnhashonly({_, {SeqN, _, MH, _}}) -> {SeqN, MH}.

striphead_to_details({SeqN, St, MH, MD}) -> {SeqN, St, MH, MD}.


key_dominates(LeftKey, RightKey) ->
    case {LeftKey, RightKey} of
        {{LK, _LVAL}, {RK, _RVAL}} when LK < RK ->
            left_hand_first;
        {{LK, _LVAL}, {RK, _RVAL}} when RK < LK ->
            right_hand_first;
        {{LK, {LSN, _LST, _LMH, _LMD}}, {RK, {RSN, _RST, _RMH, _RMD}}}
                                                when LK == RK, LSN >= RSN ->
            left_hand_dominant;
        {{LK, {LSN, _LST, _LMH, _LMD}}, {RK, {RSN, _RST, _RMH, _RMD}}}
                                                when LK == RK, LSN < RSN ->
            right_hand_dominant
    end.


maybe_reap_expiredkey(KV, LevelD) ->
    Status = strip_to_statusonly(KV),
    maybe_reap(Status, LevelD).

maybe_reap({_, infinity}, _) ->
    false; % key is not set to expire
maybe_reap({_, TS}, {true, CurrTS}) when CurrTS > TS ->
    true; % basement and ready to expire
maybe_reap(tomb, {true, _CurrTS}) ->
    true; % always expire in basement
maybe_reap(_, _) ->
    false.

is_active(Key, Value, Now) ->
    case strip_to_statusonly({Key, Value}) of
        {active, infinity} ->
            true;
        tomb ->
            false;
        {active, TS} when TS >= Now ->
            true;
        {active, _TS} ->
            false
    end.

from_ledgerkey({?IDX_TAG, ?ALL_BUCKETS, {_IdxFld, IdxVal}, {Bucket, Key}}) ->
    {Bucket, Key, IdxVal};
from_ledgerkey({?IDX_TAG, Bucket, {_IdxFld, IdxVal}, Key}) ->
    {Bucket, Key, IdxVal};
from_ledgerkey({_Tag, Bucket, Key, null}) ->
    {Bucket, Key}.

to_ledgerkey(Bucket, Key, Tag, Field, Value) when Tag == ?IDX_TAG ->
    {?IDX_TAG, Bucket, {Field, Value}, Key}.

to_ledgerkey(Bucket, Key, Tag) ->
    {Tag, Bucket, Key, null}.

%% Return the Key, Value and Hash Option for this object.  The hash option
%% indicates whether the key would ever be looked up directly, and so if it
%% requires an entry in the hash table
to_inkerkv(LedgerKey, SQN, to_fetch, null) ->
    {{SQN, ?INKT_STND, LedgerKey}, null, true};
to_inkerkv(LedgerKey, SQN, Object, KeyChanges) ->
    InkerType = check_forinkertype(LedgerKey, Object),
    Value = create_value_for_journal({Object, KeyChanges}, false),
    {{SQN, InkerType, LedgerKey}, Value}.

%% Used when fetching objects, so only handles standard, hashable entries
from_inkerkv(Object) ->
    from_inkerkv(Object, false).

from_inkerkv(Object, ToIgnoreKeyChanges) ->
    case Object of
        {{SQN, ?INKT_STND, PK}, Bin} when is_binary(Bin) ->
            {{SQN, PK}, revert_value_from_journal(Bin, ToIgnoreKeyChanges)};
        {{SQN, ?INKT_STND, PK}, Term} ->
            {{SQN, PK}, Term};
        _ ->
            Object
    end.

create_value_for_journal({Object, KeyChanges}, Compress)
                                            when not is_binary(KeyChanges) ->
    KeyChangeBin = term_to_binary(KeyChanges, [compressed]),
    create_value_for_journal({Object, KeyChangeBin}, Compress);
create_value_for_journal({Object, KeyChangeBin}, Compress) ->
    KeyChangeBinLen = byte_size(KeyChangeBin),
    ObjectBin = serialise_object(Object, Compress),
    TypeCode = encode_valuetype(is_binary(Object), Compress),
    <<ObjectBin/binary,
        KeyChangeBin/binary,
        KeyChangeBinLen:32/integer,
        TypeCode:8/integer>>.

maybe_compress({null, KeyChanges}) ->
    create_value_for_journal({null, KeyChanges}, false);
maybe_compress(JournalBin) ->
    Length0 = byte_size(JournalBin) - 5,
    <<JBin0:Length0/binary,
        KeyChangeLength:32/integer,
        Type:8/integer>> = JournalBin,
    {IsBinary, IsCompressed} = decode_valuetype(Type),
    case IsCompressed of
        true ->
            JournalBin;
        false ->
            Length1 = Length0 - KeyChangeLength,
            <<OBin2:Length1/binary, KCBin2:KeyChangeLength/binary>> = JBin0,
            V0 = {deserialise_object(OBin2, IsBinary, IsCompressed),
                    binary_to_term(KCBin2)},
            create_value_for_journal(V0, true)
    end.

serialise_object(Object, false) when is_binary(Object) ->
    Object;
serialise_object(Object, true) when is_binary(Object) ->
    zlib:compress(Object);
serialise_object(Object, false) ->
    term_to_binary(Object);
serialise_object(Object, true) ->
    term_to_binary(Object, [compressed]).

revert_value_from_journal(JournalBin) ->
    revert_value_from_journal(JournalBin, false).

revert_value_from_journal(JournalBin, ToIgnoreKeyChanges) ->
    Length0 = byte_size(JournalBin) - 5,
    <<JBin0:Length0/binary,
        KeyChangeLength:32/integer,
        Type:8/integer>> = JournalBin,
    {IsBinary, IsCompressed} = decode_valuetype(Type),
    Length1 = Length0 - KeyChangeLength,
    case ToIgnoreKeyChanges of
        true ->
            <<OBin2:Length1/binary, _KCBin2:KeyChangeLength/binary>> = JBin0,
            {deserialise_object(OBin2, IsBinary, IsCompressed), []};
        false ->
            <<OBin2:Length1/binary, KCBin2:KeyChangeLength/binary>> = JBin0,
            {deserialise_object(OBin2, IsBinary, IsCompressed),
                binary_to_term(KCBin2)}
    end.

deserialise_object(Binary, true, true) ->
    zlib:uncompress(Binary);
deserialise_object(Binary, true, false) ->
    Binary;
deserialise_object(Binary, false, _) ->
    binary_to_term(Binary).

encode_valuetype(IsBinary, IsCompressed) ->
    Bit2 =
        case IsBinary of            
            true -> 2;
            false -> 0
        end,
    Bit1 =
        case IsCompressed of
            true -> 1;
            false -> 0
        end,
    Bit1 + Bit2.

decode_valuetype(TypeInt) ->
    IsCompressed = TypeInt band 1 == 1,
    IsBinary = TypeInt band 2 == 2,
    {IsBinary, IsCompressed}.

from_journalkey({SQN, _Type, LedgerKey}) ->
    {SQN, LedgerKey}.

compact_inkerkvc({_InkerKey, crc_wonky, false}, _Strategy) ->
    skip;
compact_inkerkvc({{_SQN, ?INKT_TOMB, _LK}, _V, _CrcCheck}, _Strategy) ->
    skip;
compact_inkerkvc({{SQN, ?INKT_KEYD, LK}, V, CrcCheck}, Strategy) ->
    case get_tagstrategy(LK, Strategy) of
        skip ->
            skip;
        retain ->
            {retain, {{SQN, ?INKT_KEYD, LK}, V, CrcCheck}};
        TagStrat ->
            {TagStrat, null}
    end;
compact_inkerkvc({{SQN, ?INKT_STND, LK}, V, CrcCheck}, Strategy) ->
    case get_tagstrategy(LK, Strategy) of
        skip ->
            skip;
        retain ->
            {_V, KeyDeltas} = revert_value_from_journal(V),    
            {retain, {{SQN, ?INKT_KEYD, LK}, {null, KeyDeltas}, CrcCheck}};
        TagStrat ->
            {TagStrat, null}
    end;
compact_inkerkvc(_KVC, _Strategy) ->
    skip.

get_tagstrategy(LK, Strategy) ->
    case LK of
        {Tag, _, _, _} ->
            case lists:keyfind(Tag, 1, Strategy) of
                {Tag, TagStrat} ->
                    TagStrat;
                false ->
                    leveled_log:log("IC012", [Tag, Strategy]),
                    skip
            end;
        _ ->
            skip
    end.

split_inkvalue(VBin) ->
    case is_binary(VBin) of
            true ->
                revert_value_from_journal(VBin);
            false ->
                VBin
        end.

check_forinkertype(_LedgerKey, delete) ->
    ?INKT_TOMB;
check_forinkertype(_LedgerKey, _Object) ->
    ?INKT_STND.

hash(Obj) ->
    erlang:phash2(term_to_binary(Obj)).


% Compare a key against a query key, only comparing elements that are non-null
% in the Query key.  This is used for comparing against end keys in queries.
endkey_passed(all, _) ->
    false;
endkey_passed({EK1, null, null, null}, {CK1, _, _, _}) ->
    EK1 < CK1;
endkey_passed({EK1, EK2, null, null}, {CK1, CK2, _, _}) ->
    {EK1, EK2} < {CK1, CK2};
endkey_passed({EK1, EK2, EK3, null}, {CK1, CK2, CK3, _}) ->
    {EK1, EK2, EK3} < {CK1, CK2, CK3};
endkey_passed(EndKey, CheckingKey) ->
    EndKey < CheckingKey.

idx_indexspecs(IndexSpecs, Bucket, Key, SQN, TTL) ->
    lists:map(
            fun({IdxOp, IdxFld, IdxTrm}) ->
                gen_indexspec(Bucket, Key, IdxOp, IdxFld, IdxTrm, SQN, TTL)
            end,
            IndexSpecs
                ).

gen_indexspec(Bucket, Key, IdxOp, IdxField, IdxTerm, SQN, TTL) ->
    Status =
        case IdxOp of
            add ->
                {active, TTL};
            remove ->
                %% TODO: timestamps for delayed reaping 
                tomb
        end,
    case Bucket of
        {all, RealBucket} ->    
            {to_ledgerkey(?ALL_BUCKETS,
                            {RealBucket, Key},
                            ?IDX_TAG,
                            IdxField,
                            IdxTerm),
                {SQN, Status, no_lookup, null}};
        _ ->
            {to_ledgerkey(Bucket,
                            Key,
                            ?IDX_TAG,
                            IdxField,
                            IdxTerm),
                {SQN, Status, no_lookup, null}}
    end.

-spec aae_indexspecs(false|recent_aae(),
                                any(), any(),
                                integer(), integer(),
                                list())
                                    -> list().
%% @doc
%% Generate an additional index term representing the change, if the last
%% modified date for the change is within the definition of recency.
%%
%% The objetc may have multiple last modified dates (siblings), and in this
%% case index entries for all dates within the range are added.
%%
%% The index should entry auto-expire in the future (when it is no longer
%% relevant to assessing recent changes)
aae_indexspecs(false, _Bucket, _Key, _SQN, _H, _LastMods) ->
    [];
aae_indexspecs(_AAE, _Bucket, _Key, _SQN, _H, []) ->
    [];
aae_indexspecs(AAE, Bucket, Key, SQN, H, LastMods) ->
    InList = lists:member(Bucket, AAE#recent_aae.buckets),
    Bucket0 =
        case AAE#recent_aae.filter of
            blacklist ->
                case InList of
                    true ->
                        false;
                    false ->
                        {all, Bucket}
                end;
            whitelist ->
                case InList of
                    true ->
                        Bucket;
                    false ->
                        false
                end
        end,
    case Bucket0 of
        false ->
            [];
        Bucket0 ->
            GenIdxFun =
                fun(LMD0, Acc) ->
                    Dates = parse_date(LMD0,
                                        AAE#recent_aae.unit_minutes,
                                        AAE#recent_aae.limit_minutes,
                                        integer_now()),
                    case Dates of
                        no_index ->
                            Acc;
                        {LMD1, TTL} ->
                            TreeSize = AAE#recent_aae.tree_size,
                            SegID =
                                leveled_tictac:get_segment(Key, TreeSize),
                            IdxFldStr = ?NRT_IDX ++ LMD1 ++ "_bin",
                            IdxTrmStr =
                                string:right(integer_to_list(SegID), 8, $0) ++
                                "." ++
                                string:right(integer_to_list(H), 8, $0),
                            {IdxK, IdxV} =
                                gen_indexspec(Bucket0, Key,
                                                add,
                                                list_to_binary(IdxFldStr),
                                                list_to_binary(IdxTrmStr),
                                                SQN, TTL),
                            [{IdxK, IdxV}|Acc]
                    end
                end,
            lists:foldl(GenIdxFun, [], LastMods)
    end.

-spec parse_date(tuple(), integer(), integer(), integer()) ->
                    no_index|{binary(), integer()}.
%% @doc
%% Parse the lat modified date and the AAE date configuration to return a
%% binary to be used as the last modified date part of the index, and an
%% integer to be used as the TTL of the index entry.
%% Return no_index if the change is not recent.
parse_date(LMD, UnitMins, LimitMins, Now) ->
    LMDsecs = integer_time(LMD),
    Recent = (LMDsecs + LimitMins * 60) > Now,
    case Recent of
        false ->
            no_index;
        true ->
            {{Y, M, D}, {Hour, Minute, _Second}} =
                calendar:now_to_datetime(LMD),
            RoundMins =
                UnitMins * (Minute div UnitMins),
            StrTime =
                lists:flatten(io_lib:format(?LMD_FORMAT,
                                                [Y, M, D, Hour, RoundMins])),
            TTL = min(Now, LMDsecs) + (LimitMins + UnitMins) * 60,
            {StrTime, TTL}
    end.

-spec generate_ledgerkv(
            tuple(), integer(), any(), integer(), tuple()|infinity) ->
            {any(), any(), any(), {integer()|no_lookup, integer()}, list()}.
%% @doc
%% Function to extract from an object the information necessary to populate
%% the Penciller's ledger.
%% Outputs -
%% Bucket - original Bucket extracted from the PrimaryKey 
%% Key - original Key extracted from the PrimaryKey
%% Value - the value to be used in the Ledger (essentially the extracted
%% metadata)
%% {Hash, ObjHash} - A magic hash of the key to accelerate lookups, and a hash
%% of the value to be used for equality checking between objects
%% LastMods - the last modified dates for the object (may be multiple due to
%% siblings)
generate_ledgerkv(PrimaryKey, SQN, Obj, Size, TS) ->
    {Tag, Bucket, Key, _} = PrimaryKey,
    Status = case Obj of
                    delete ->
                        tomb;
                    _ ->
                        {active, TS}
                end,
    Hash = magic_hash(PrimaryKey),
    {MD, LastMods} = extract_metadata(Obj, Size, Tag),
    ObjHash = get_objhash(Tag, MD),
    Value = {SQN,
                Status,
                Hash,
                MD},
    {Bucket, Key, Value, {Hash, ObjHash}, LastMods}.


integer_now() ->
    integer_time(os:timestamp()).

integer_time(TS) ->
    DT = calendar:now_to_universal_time(TS),
    calendar:datetime_to_gregorian_seconds(DT).

extract_metadata(Obj, Size, ?RIAK_TAG) ->
    riak_extract_metadata(Obj, Size);
extract_metadata(Obj, Size, ?STD_TAG) ->
    {{hash(Obj), Size}, []}.

get_size(PK, Value) ->
    {Tag, _Bucket, _Key, _} = PK,
    {_, _, _, MD} = Value,
    case Tag of
        ?RIAK_TAG ->
            {_RMD, _VC, _Hash, Size} = MD,
            Size;
        ?STD_TAG ->
            {_Hash, Size} = MD,
            Size
    end.

-spec get_keyandobjhash(tuple(), tuple()) -> tuple().
%% @doc
%% Return a tucple of {Bucket, Key, Hash} where hash is a hash of the object
%% not the key (for example with Riak tagged objects this will be a hash of
%% the sorted vclock)
get_keyandobjhash(LK, Value) ->
    {Tag, Bucket, Key, _} = LK,
    {_, _, _, MD} = Value,
    case Tag of
        ?IDX_TAG ->
            from_ledgerkey(LK); % returns {Bucket, Key, IdxValue}
        _ ->
            {Bucket, Key, get_objhash(Tag, MD)}
    end.

get_objhash(Tag, ObjMetaData) ->
    case Tag of
        ?RIAK_TAG ->
            {_RMD, _VC, Hash, _Size} = ObjMetaData,
            Hash;
        ?STD_TAG ->
            {Hash, _Size} = ObjMetaData,
            Hash
    end.
        
        
build_metadata_object(PrimaryKey, MD) ->
    {Tag, _Bucket, _Key, null} = PrimaryKey,
    case Tag of
        ?RIAK_TAG ->
            {SibData, Vclock, _Hash, _Size} = MD,
            riak_metadata_to_binary(Vclock, SibData);
        ?STD_TAG ->
            MD
    end.


riak_extract_metadata(delete, Size) ->
    {{delete, null, null, Size}, []};
riak_extract_metadata(ObjBin, Size) ->
    {VclockBin, SibBin, LastMods} = riak_metadata_from_binary(ObjBin),
    {{SibBin, 
            VclockBin, 
            erlang:phash2(lists:sort(binary_to_term(VclockBin))), 
            Size},
        LastMods}.

%% <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
%%%     VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

riak_metadata_to_binary(VclockBin, SibMetaBin) ->
    VclockLen = byte_size(VclockBin),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer,
        VclockLen:32/integer, VclockBin/binary,
        SibMetaBin/binary>>.
    
riak_metadata_from_binary(V1Binary) ->
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
            Rest/binary>> = V1Binary,
    <<VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> = Rest,
    {SibMetaBin, LastMods} =
        case SibCount of
            SC when is_integer(SC) ->
                get_metadata_from_siblings(SibsBin,
                                            SibCount,
                                            <<SibCount:32/integer>>,
                                            [])
        end,
    {VclockBin, SibMetaBin, LastMods}.

get_metadata_from_siblings(<<>>, 0, SibMetaBin, LastMods) ->
    {SibMetaBin, LastMods};
get_metadata_from_siblings(<<ValLen:32/integer, Rest0/binary>>,
                            SibCount,
                            SibMetaBin,
                            LastMods) ->
    <<_ValBin:ValLen/binary, MetaLen:32/integer, Rest1/binary>> = Rest0,
    <<MetaBin:MetaLen/binary, Rest2/binary>> = Rest1,
    LastMod =
        case MetaBin of 
            <<MegaSec:32/integer,
                Sec:32/integer,
                MicroSec:32/integer,
                _Rest/binary>> ->
                    {MegaSec, Sec, MicroSec};
            _ ->
                {0, 0, 0}
        end,
    get_metadata_from_siblings(Rest2,
                                SibCount - 1,
                                <<SibMetaBin/binary,
                                    0:32/integer,
                                    MetaLen:32/integer,
                                    MetaBin:MetaLen/binary>>,
                                    [LastMod|LastMods]).




%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


indexspecs_test() ->
    IndexSpecs = [{add, "t1_int", 456},
                    {add, "t1_bin", "adbc123"},
                    {remove, "t1_bin", "abdc456"}],
    Changes = idx_indexspecs(IndexSpecs, "Bucket", "Key2", 1, infinity),
    ?assertMatch({{i, "Bucket", {"t1_int", 456}, "Key2"},
                        {1, {active, infinity}, no_lookup, null}},
                    lists:nth(1, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "adbc123"}, "Key2"},
                        {1, {active, infinity}, no_lookup, null}},
                    lists:nth(2, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "abdc456"}, "Key2"},
                        {1, tomb, no_lookup, null}},
                    lists:nth(3, Changes)).

endkey_passed_test() ->
    TestKey = {i, null, null, null},
    K1 = {i, 123, {"a", "b"}, <<>>},
    K2 = {o, 123, {"a", "b"}, <<>>},
    ?assertMatch(false, endkey_passed(TestKey, K1)),
    ?assertMatch(true, endkey_passed(TestKey, K2)).


corrupted_ledgerkey_test() ->
    % When testing for compacted journal which has been corrupted, there may
    % be a corruptes ledger key.  Always skip these keys
    % Key has become a 3-tuple not a 4-tuple
    TagStrat1 = compact_inkerkvc({{1,
                                        ?INKT_STND,
                                        {?STD_TAG, "B1", "K1andSK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, retain}]),
    ?assertMatch(skip, TagStrat1),
    TagStrat2 = compact_inkerkvc({{1,
                                        ?INKT_KEYD,
                                        {?STD_TAG, "B1", "K1andSK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, retain}]),
    ?assertMatch(skip, TagStrat2).

general_skip_strategy_test() ->
    % Confirm that we will skip if the strategy says so
    TagStrat1 = compact_inkerkvc({{1,
                                        ?INKT_STND,
                                        {?STD_TAG, "B1", "K1andSK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}]),
    ?assertMatch(skip, TagStrat1),
    TagStrat2 = compact_inkerkvc({{1,
                                        ?INKT_KEYD,
                                        {?STD_TAG, "B1", "K1andSK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}]),
    ?assertMatch(skip, TagStrat2),
    TagStrat3 = compact_inkerkvc({{1,
                                        ?INKT_KEYD,
                                        {?IDX_TAG, "B1", "K1", "SK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}]),
    ?assertMatch(skip, TagStrat3),
    TagStrat4 = compact_inkerkvc({{1,
                                        ?INKT_KEYD,
                                        {?IDX_TAG, "B1", "K1", "SK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}, {?IDX_TAG, recalc}]),
    ?assertMatch({recalc, null}, TagStrat4),
    TagStrat5 = compact_inkerkvc({{1,
                                        ?INKT_TOMB,
                                        {?IDX_TAG, "B1", "K1", "SK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}, {?IDX_TAG, recalc}]),
    ?assertMatch(skip, TagStrat5).
    
corrupted_inker_tag_test() ->
    % Confirm that we will skip on unknown inker tag
    TagStrat1 = compact_inkerkvc({{1,
                                        foo,
                                        {?STD_TAG, "B1", "K1andSK"}},
                                    {},
                                    true},
                                    [{?STD_TAG, retain}]),
    ?assertMatch(skip, TagStrat1).

%% Test below proved that the overhead of performing hashes was trivial
%% Maybe 5 microseconds per hash

hashperf_test() ->
    OL = lists:map(fun(_X) -> leveled_rand:rand_bytes(8192) end, lists:seq(1, 1000)),
    SW = os:timestamp(),
    _HL = lists:map(fun(Obj) -> erlang:phash2(Obj) end, OL),
    io:format(user, "1000 object hashes in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)]).

magichashperf_test() ->
    KeyFun =
        fun(X) ->
            K = {o, "Bucket", "Key" ++ integer_to_list(X), null},
            {K, X}
        end,
    KL = lists:map(KeyFun, lists:seq(1, 1000)),
    {TimeMH, _HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(user, "1000 keys magic hashed in ~w microseconds~n", [TimeMH]),
    {TimePH, _Hl2} = timer:tc(lists, map, [fun(K) -> erlang:phash2(K) end, KL]),
    io:format(user, "1000 keys phash2 hashed in ~w microseconds~n", [TimePH]),
    {TimeMH2, _HL1} = timer:tc(lists, map, [fun(K) -> magic_hash(K) end, KL]),
    io:format(user, "1000 keys magic hashed in ~w microseconds~n", [TimeMH2]).

parsedate_test() ->
    {MeS, S, MiS} = os:timestamp(),
    timer:sleep(100),
    Now = integer_now(),
    UnitMins = 5,
    LimitMins = 60,
    PD = parse_date({MeS, S, MiS}, UnitMins, LimitMins, Now),
    io:format("Parsed Date ~w~n", [PD]),
    ?assertMatch(true, is_tuple(PD)),
    check_pd(PD, UnitMins),
    CheckFun =
        fun(Offset) ->
            ModDate = {MeS, S + Offset * 60, MiS},
            check_pd(parse_date(ModDate, UnitMins, LimitMins, Now), UnitMins)
        end,  
    lists:foreach(CheckFun, lists:seq(1, 60)).
    
check_pd(PD, UnitMins) ->
    {LMDstr, _TTL} = PD,
    Minutes = list_to_integer(lists:nthtail(10, LMDstr)),
    ?assertMatch(0, Minutes rem UnitMins).

parseolddate_test() ->
    LMD = os:timestamp(),
    timer:sleep(100),
    Now = integer_now() + 60 * 60,
    UnitMins = 5,
    LimitMins = 60,
    PD = parse_date(LMD, UnitMins, LimitMins, Now),
    io:format("Parsed Date ~w~n", [PD]),
    ?assertMatch(no_index, PD).

genaaeidx_test() ->
    AAE = #recent_aae{filter=blacklist,
                        buckets=[],
                        limit_minutes=60,
                        unit_minutes=5},
    Bucket = <<"Bucket1">>,
    Key = <<"Key1">>,
    SQN = 1,
    H = erlang:phash2(null),
    LastMods = [os:timestamp(), os:timestamp()],

    AAESpecs = aae_indexspecs(AAE, Bucket, Key, SQN, H, LastMods),
    ?assertMatch(2, length(AAESpecs)),
    
    LastMods1 = [os:timestamp()],
    AAESpecs1 = aae_indexspecs(AAE, Bucket, Key, SQN, H, LastMods1),
    ?assertMatch(1, length(AAESpecs1)),
    IdxB = element(2, element(1, lists:nth(1, AAESpecs1))),
    io:format(user, "AAE IDXSpecs1 ~w~n", [AAESpecs1]),
    ?assertMatch(<<"$all">>, IdxB),
    
    LastMods0 = [],
    AAESpecs0 = aae_indexspecs(AAE, Bucket, Key, SQN, H, LastMods0),
    ?assertMatch(0, length(AAESpecs0)),
    
    AAE0 = AAE#recent_aae{filter=whitelist,
                            buckets=[<<"Bucket0">>]},
    AAESpecsB0 = aae_indexspecs(AAE0, Bucket, Key, SQN, H, LastMods1),
    ?assertMatch(0, length(AAESpecsB0)),
    
    AAESpecsB1 = aae_indexspecs(AAE0, <<"Bucket0">>, Key, SQN, H, LastMods1),
    ?assertMatch(1, length(AAESpecsB1)),
    [{{?IDX_TAG, <<"Bucket0">>, {Fld, Term}, <<"Key1">>},
        {SQN, {active, TS}, no_lookup, null}}] = AAESpecsB1,
    ?assertMatch(true, is_integer(TS)),
    ?assertMatch(17, length(binary_to_list(Term))),
    ?assertMatch("$aae.", lists:sublist(binary_to_list(Fld), 5)),
    
    AAE1 = AAE#recent_aae{filter=blacklist,
                            buckets=[<<"Bucket0">>]},
    AAESpecsB2 = aae_indexspecs(AAE1, <<"Bucket0">>, Key, SQN, H, LastMods1),
    ?assertMatch(0, length(AAESpecsB2)).

-endif.
