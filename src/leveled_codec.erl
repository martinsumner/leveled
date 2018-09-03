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
        from_ledgerkey/2,
        to_inkerkey/2,
        to_inkerkv/6,
        from_inkerkv/1,
        from_inkerkv/2,
        from_journalkey/1,
        compact_inkerkvc/2,
        split_inkvalue/1,
        check_forinkertype/2,
        maybe_compress/2,
        create_value_for_journal/3,
        build_metadata_object/2,
        generate_ledgerkv/5,
        get_size/2,
        get_keyandobjhash/2,
        idx_indexspecs/5,
        obj_objectspecs/3,
        aae_indexspecs/6,
        riak_extract_metadata/2,
        segment_hash/1,
        to_lookup/1,
        riak_metadata_to_binary/2,
        next_key/1]).         

-define(V1_VERS, 1).
-define(MAGIC, 53). % riak_kv -> riak_object
-define(LMD_FORMAT, "~4..0w~2..0w~2..0w~2..0w~2..0w").
-define(NRT_IDX, "$aae.").
-define(ALL_BUCKETS, <<"$all">>).

-type recent_aae() :: #recent_aae{}.
-type riak_metadata() :: {binary()|delete, % Sibling Metadata
                            binary()|null, % Vclock Metadata
                            integer()|null, % Hash of vclock - non-exportable 
                            integer()}. % Size in bytes of real object

-type tag() :: 
        ?STD_TAG|?RIAK_TAG|?IDX_TAG|?HEAD_TAG.
-type segment_hash() :: 
        {integer(), integer()}|no_lookup.
-type ledger_status() ::
        tomb|{active, non_neg_integer()|infinity}.
-type ledger_key() :: 
        {tag(), any(), any(), any()}|all.
-type ledger_value() :: 
        {integer(), ledger_status(), segment_hash(), tuple()|null}.
-type ledger_kv() ::
        {ledger_key(), ledger_value()}.
-type compaction_strategy() ::
        list({tag(), retain|skip|recalc}).
-type journal_key_tag() ::
        ?INKT_STND|?INKT_TOMB|?INKT_MPUT|?INKT_KEYD.
-type journal_key() ::
        {integer(), journal_key_tag(), ledger_key()}.
-type compression_method() ::
        lz4|native.
-type index_specs() ::
        list({add|remove, any(), any()}).
-type journal_keychanges() :: 
        {index_specs(), infinity|integer()}. % {KeyChanges, TTL}


-type segment_list() 
        :: list(integer())|false.

-export_type([tag/0,
                segment_hash/0,
                ledger_status/0,
                ledger_key/0,
                ledger_value/0,
                ledger_kv/0,
                compaction_strategy/0,
                journal_key_tag/0,
                journal_key/0,
                compression_method/0,
                journal_keychanges/0,
                index_specs/0,
                segment_list/0]).


%%%============================================================================
%%% Ledger Key Manipulation
%%%============================================================================

-spec segment_hash(ledger_key()|binary()) -> {integer(), integer()}.
%% @doc
%% Return two 16 bit integers - the segment ID and a second integer for spare
%% entropy.  The hashed should be used in blooms or indexes such that some 
%% speed can be gained if just the segment ID is known - but more can be 
%% gained should the extended hash (with the second element) is known
segment_hash(Key) when is_binary(Key) ->
    {segment_hash, SegmentID, ExtraHash} = leveled_tictac:keyto_segment48(Key),
    {SegmentID, ExtraHash};
segment_hash({?RIAK_TAG, Bucket, Key, null}) 
                                    when is_binary(Bucket), is_binary(Key) ->
    segment_hash(<<Bucket/binary, Key/binary>>);
segment_hash({?HEAD_TAG, Bucket, Key, SubK})
                    when is_binary(Bucket), is_binary(Key), is_binary(SubK) ->
    segment_hash(<<Bucket/binary, Key/binary, SubK/binary>>);
segment_hash({?HEAD_TAG, Bucket, Key, _SubK})
                                    when is_binary(Bucket), is_binary(Key) ->
    segment_hash(<<Bucket/binary, Key/binary>>);
segment_hash(Key) ->
    segment_hash(term_to_binary(Key)).


-spec to_lookup(ledger_key()) -> lookup|no_lookup.
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


%% @doc
%% Some helper functions to get a sub_components of the key/value

-spec strip_to_statusonly(ledger_kv()) -> ledger_status().
strip_to_statusonly({_, {_, St, _, _}}) -> St.

-spec strip_to_seqonly(ledger_kv()) -> non_neg_integer().
strip_to_seqonly({_, {SeqN, _, _, _}}) -> SeqN.

-spec strip_to_keyseqonly(ledger_kv()) -> {ledger_key(), integer()}.
strip_to_keyseqonly({LK, {SeqN, _, _, _}}) -> {LK, SeqN}.

-spec strip_to_seqnhashonly(ledger_kv()) -> {integer(), segment_hash()}.
strip_to_seqnhashonly({_, {SeqN, _, MH, _}}) -> {SeqN, MH}.

-spec striphead_to_details(ledger_value()) -> ledger_value().
striphead_to_details({SeqN, St, MH, MD}) -> {SeqN, St, MH, MD}.

-spec key_dominates(ledger_kv(), ledger_kv()) -> 
    left_hand_first|right_hand_first|left_hand_dominant|right_hand_dominant.
%% @doc
%% When comparing two keys in the ledger need to find if one key comes before 
%% the other, or if the match, which key is "better" and should be the winner
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

-spec maybe_reap_expiredkey(ledger_kv(), {boolean(), integer()}) -> boolean().
%% @doc
%% Make a reap decision based on the level in the ledger (needs to be expired
%% and in the basement).  the level is a tuple of the is_basement boolean, and 
%% a timestamp passed into the calling function
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

-spec is_active(ledger_key(), ledger_value(), non_neg_integer()) -> boolean().
%% @doc
%% Is this an active KV pair or has the timestamp expired
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

-spec from_ledgerkey(atom(), tuple()) -> false|tuple().
%% @doc
%% Return the "significant information" from the Ledger Key (normally the 
%% {Bucket, Key} pair) if and only if the ExpectedTag matched the tag - 
%% otherwise return false
from_ledgerkey(ExpectedTag, {ExpectedTag, Bucket, Key, SubKey}) ->
    from_ledgerkey({ExpectedTag, Bucket, Key, SubKey});
from_ledgerkey(_ExpectedTag, _OtherKey) ->
    false.

-spec from_ledgerkey(tuple()) -> tuple().
%% @doc
%% Return identifying information from the LedgerKey
from_ledgerkey({?IDX_TAG, ?ALL_BUCKETS, {_IdxFld, IdxVal}, {Bucket, Key}}) ->
    {Bucket, Key, IdxVal};
from_ledgerkey({?IDX_TAG, Bucket, {_IdxFld, IdxVal}, Key}) ->
    {Bucket, Key, IdxVal};
from_ledgerkey({?HEAD_TAG, Bucket, Key, SubKey}) ->
    {Bucket, {Key, SubKey}};
from_ledgerkey({_Tag, Bucket, Key, _SubKey}) ->
    {Bucket, Key}.

-spec to_ledgerkey(any(), any(), tag(), any(), any()) -> ledger_key().
%% @doc
%% Convert something into a ledger key
to_ledgerkey(Bucket, Key, Tag, Field, Value) when Tag == ?IDX_TAG ->
    {?IDX_TAG, Bucket, {Field, Value}, Key}.

-spec to_ledgerkey(any(), any(), tag()) -> ledger_key().
%% @doc
%% Convert something into a ledger key
to_ledgerkey(Bucket, {Key, SubKey}, ?HEAD_TAG) ->
    {?HEAD_TAG, Bucket, Key, SubKey};
to_ledgerkey(Bucket, Key, Tag) ->
    {Tag, Bucket, Key, null}.

-spec endkey_passed(ledger_key(), ledger_key()) -> boolean().
%% @oc
%% Compare a key against a query key, only comparing elements that are non-null
%% in the Query key.  This is used for comparing against end keys in queries.
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


%%%============================================================================
%%% Journal Compaction functions
%%%============================================================================

-spec inker_reload_strategy(compaction_strategy()) -> compaction_strategy().
%% @doc
%% Take the default startegy for compaction, and override the approach for any 
%% tags passed in
inker_reload_strategy(AltList) ->
    ReloadStrategy0 = [{?RIAK_TAG, retain}, {?STD_TAG, retain}],
    lists:foldl(fun({X, Y}, SList) ->
                        lists:keyreplace(X, 1, SList, {X, Y})
                        end,
                    ReloadStrategy0,
                    AltList).


-spec compact_inkerkvc({journal_key(), any(), boolean()}, 
                            compaction_strategy()) -> 
                            skip|{retain, any()}|{recalc, null}.
%% @doc
%% Decide whether a superceded object should be replicated in the compacted
%% file and in what format. 
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
    end.

-spec get_tagstrategy(ledger_key(), compaction_strategy()) 
                                                    -> skip|retain|recalc.
%% @doc
%% Work out the compaction startegy for the key
get_tagstrategy({Tag, _, _, _}, Strategy) ->
    case lists:keyfind(Tag, 1, Strategy) of
        {Tag, TagStrat} ->
            TagStrat;
        false ->
            leveled_log:log("IC012", [Tag, Strategy]),
            skip
    end.

%%%============================================================================
%%% Manipulate Journal Key and Value
%%%============================================================================

-spec to_inkerkey(ledger_key(), non_neg_integer()) -> journal_key().
%% @doc
%% convertion from ledger_key to journal_key to allow for the key to be fetched
to_inkerkey(LedgerKey, SQN) ->
    {SQN, ?INKT_STND, LedgerKey}.


-spec to_inkerkv(ledger_key(), non_neg_integer(), any(), journal_keychanges(), 
                    compression_method(), boolean()) -> {journal_key(), any()}.
%% @doc
%% Convert to the correct format of a Journal key and value
to_inkerkv(LedgerKey, SQN, Object, KeyChanges, PressMethod, Compress) ->
    InkerType = check_forinkertype(LedgerKey, Object),
    Value = 
        create_value_for_journal({Object, KeyChanges}, Compress, PressMethod),
    {{SQN, InkerType, LedgerKey}, Value}.

%% Used when fetching objects, so only handles standard, hashable entries
from_inkerkv(Object) ->
    from_inkerkv(Object, false).

from_inkerkv(Object, ToIgnoreKeyChanges) ->
    case Object of
        {{SQN, ?INKT_STND, PK}, Bin} when is_binary(Bin) ->
            {{SQN, PK}, revert_value_from_journal(Bin, ToIgnoreKeyChanges)};
        _ ->
            Object
    end.


-spec create_value_for_journal({any(), journal_keychanges()|binary()}, 
                                boolean(), compression_method()) -> binary().
%% @doc
%% Serialise the value to be stored in the Journal
create_value_for_journal({Object, KeyChanges}, Compress, Method)
                                            when not is_binary(KeyChanges) ->
    KeyChangeBin = term_to_binary(KeyChanges, [compressed]),
    create_value_for_journal({Object, KeyChangeBin}, Compress, Method);
create_value_for_journal({Object, KeyChangeBin}, Compress, Method) ->
    KeyChangeBinLen = byte_size(KeyChangeBin),
    ObjectBin = serialise_object(Object, Compress, Method),
    TypeCode = encode_valuetype(is_binary(Object), Compress, Method),
    <<ObjectBin/binary,
        KeyChangeBin/binary,
        KeyChangeBinLen:32/integer,
        TypeCode:8/integer>>.

maybe_compress({null, KeyChanges}, _PressMethod) ->
    create_value_for_journal({null, KeyChanges}, false, native);
maybe_compress(JournalBin, PressMethod) ->
    Length0 = byte_size(JournalBin) - 5,
    <<JBin0:Length0/binary,
        KeyChangeLength:32/integer,
        Type:8/integer>> = JournalBin,
    {IsBinary, IsCompressed, IsLz4} = decode_valuetype(Type),
    case IsCompressed of
        true ->
            JournalBin;
        false ->
            Length1 = Length0 - KeyChangeLength,
            <<OBin2:Length1/binary, KCBin2:KeyChangeLength/binary>> = JBin0,
            V0 = {deserialise_object(OBin2, IsBinary, IsCompressed, IsLz4),
                    binary_to_term(KCBin2)},
            create_value_for_journal(V0, true, PressMethod)
    end.

serialise_object(Object, false, _Method) when is_binary(Object) ->
    Object;
serialise_object(Object, true, Method) when is_binary(Object) ->
    case Method of 
        lz4 ->
            {ok, Bin} = lz4:pack(Object),
            Bin;
        native ->
            zlib:compress(Object)
    end;
serialise_object(Object, false, _Method) ->
    term_to_binary(Object);
serialise_object(Object, true, _Method) ->
    term_to_binary(Object, [compressed]).

-spec revert_value_from_journal(binary()) -> {any(), journal_keychanges()}.
%% @doc
%% Revert the object back to its deserialised state, along with the list of
%% key changes associated with the change
revert_value_from_journal(JournalBin) ->
    revert_value_from_journal(JournalBin, false).

revert_value_from_journal(JournalBin, ToIgnoreKeyChanges) ->
    Length0 = byte_size(JournalBin) - 5,
    <<JBin0:Length0/binary,
        KeyChangeLength:32/integer,
        Type:8/integer>> = JournalBin,
    {IsBinary, IsCompressed, IsLz4} = decode_valuetype(Type),
    Length1 = Length0 - KeyChangeLength,
    case ToIgnoreKeyChanges of
        true ->
            <<OBin2:Length1/binary, _KCBin2:KeyChangeLength/binary>> = JBin0,
            {deserialise_object(OBin2, IsBinary, IsCompressed, IsLz4), 
                {[], infinity}};
        false ->
            <<OBin2:Length1/binary, KCBin2:KeyChangeLength/binary>> = JBin0,
            {deserialise_object(OBin2, IsBinary, IsCompressed, IsLz4),
                binary_to_term(KCBin2)}
    end.

deserialise_object(Binary, true, true, true) ->
    {ok, Deflated} = lz4:unpack(Binary),
    Deflated;
deserialise_object(Binary, true, true, false) ->
    zlib:uncompress(Binary);
deserialise_object(Binary, true, false, _IsLz4) ->
    Binary;
deserialise_object(Binary, false, _, _IsLz4) ->
    binary_to_term(Binary).

encode_valuetype(IsBinary, IsCompressed, Method) ->
    Bit3 = 
        case Method of 
            lz4 -> 4;
            native -> 0
        end,
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
    Bit1 + Bit2 + Bit3.


-spec decode_valuetype(integer()) -> {boolean(), boolean(), boolean()}.
%% @doc
%% Check bit flags to confirm how the object has been serialised
decode_valuetype(TypeInt) ->
    IsCompressed = TypeInt band 1 == 1,
    IsBinary = TypeInt band 2 == 2,
    IsLz4 = TypeInt band 4 == 4,
    {IsBinary, IsCompressed, IsLz4}.

-spec from_journalkey(journal_key()) -> {integer(), ledger_key()}.
%% @doc
%% Return just SQN and Ledger Key
from_journalkey({SQN, _Type, LedgerKey}) ->
    {SQN, LedgerKey}.


split_inkvalue(VBin) when is_binary(VBin) ->
    revert_value_from_journal(VBin).

check_forinkertype(_LedgerKey, delete) ->
    ?INKT_TOMB;
check_forinkertype(_LedgerKey, head_only) ->
    ?INKT_MPUT;
check_forinkertype(_LedgerKey, _Object) ->
    ?INKT_STND.

hash(Obj) ->
    erlang:phash2(term_to_binary(Obj)).



%%%============================================================================
%%% Other Ledger Functions
%%%============================================================================


-spec obj_objectspecs(list(tuple()), integer(), integer()|infinity) 
                                                        -> list(ledger_kv()).
%% @doc
%% Convert object specs to KV entries ready for the ledger
obj_objectspecs(ObjectSpecs, SQN, TTL) ->
    lists:map(fun({IdxOp, Bucket, Key, SubKey, Value}) ->
                    gen_headspec(Bucket, Key, IdxOp, SubKey, Value, SQN, TTL)
                end,
                ObjectSpecs).

-spec idx_indexspecs(index_specs(), 
                        any(), any(), integer(), integer()|infinity) 
                                                        -> list(ledger_kv()).
%% @doc
%% Convert index specs to KV entries ready for the ledger
idx_indexspecs(IndexSpecs, Bucket, Key, SQN, TTL) ->
    lists:map(
            fun({IdxOp, IdxFld, IdxTrm}) ->
                gen_indexspec(Bucket, Key, IdxOp, IdxFld, IdxTrm, SQN, TTL)
            end,
            IndexSpecs
                ).

gen_indexspec(Bucket, Key, IdxOp, IdxField, IdxTerm, SQN, TTL) ->
    Status = set_status(IdxOp, TTL),
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

gen_headspec(Bucket, Key, IdxOp, SubKey, Value, SQN, TTL) ->
    Status = set_status(IdxOp, TTL),
    K = to_ledgerkey(Bucket, {Key, SubKey}, ?HEAD_TAG),
    {K, {SQN, Status, segment_hash(K), Value}}.


set_status(add, TTL) ->
    {active, TTL};
set_status(remove, _TTL) ->
    %% TODO: timestamps for delayed reaping 
    tomb.

-spec aae_indexspecs(false|recent_aae(),
                                any(), any(),
                                integer(), integer(),
                                list())
                                    -> list().
%% @doc
%% Generate an additional index term representing the change, if the last
%% modified date for the change is within the definition of recency.
%%
%% The object may have multiple last modified dates (siblings), and in this
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
                                        leveled_util:integer_now()),
                    case Dates of
                        no_index ->
                            Acc;
                        {LMD1, TTL} ->
                            TreeSize = AAE#recent_aae.tree_size,
                            SegID32 = leveled_tictac:keyto_segment32(Key),
                            SegID =
                                leveled_tictac:get_segment(SegID32, TreeSize),
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
                    no_index|{list(), integer()}.
%% @doc
%% Parse the last modified date and the AAE date configuration to return a
%% binary to be used as the last modified date part of the index, and an
%% integer to be used as the TTL of the index entry.
%% Return no_index if the change is not recent.
parse_date(LMD, UnitMins, LimitMins, Now) ->
    LMDsecs = leveled_util:integer_time(LMD),
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
            {any(), any(), any(), 
                {{integer(), integer()}|no_lookup, integer()}, 
                list()}.
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
    Hash = segment_hash(PrimaryKey),
    {MD, LastMods} = extract_metadata(Obj, Size, Tag),
    ObjHash = get_objhash(Tag, MD),
    Value = {SQN,
                Status,
                Hash,
                MD},
    {Bucket, Key, Value, {Hash, ObjHash}, LastMods}.


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
    {Tag, _Bucket, _Key, _SubKey} = PrimaryKey,
    case Tag of
        ?RIAK_TAG ->
            {SibData, Vclock, _Hash, _Size} = MD,
            riak_metadata_to_binary(Vclock, SibData);
        ?STD_TAG ->
            MD;
        ?HEAD_TAG ->
            MD
    end.


-spec riak_extract_metadata(binary()|delete, non_neg_integer()) -> 
                            {riak_metadata(), list()}.
%% @doc
%% Riak extract metadata should extract a metadata object which is a 
%% five-tuple of:
%% - Binary of sibling Metadata
%% - Binary of vector clock metadata
%% - Non-exportable hash of the vector clock metadata
%% - The largest last modified date of the object
%% - Size of the object
%%
%% The metadata object should be returned with the full list of last 
%% modified dates (which will be used for recent anti-entropy index creation)
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

-spec next_key(leveled_bookie:key()) -> leveled_bookie:key().
%% @doc
%% Get the next key to iterate from a given point
next_key(Key) when is_binary(Key) ->
    <<Key/binary, 0>>;
next_key(Key) when is_list(Key) ->
    Key ++ [0].


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


general_skip_strategy_test() ->
    % Confirm that we will skip if the strategy says so
    TagStrat1 = compact_inkerkvc({{1,
                                        ?INKT_STND,
                                        {?STD_TAG, "B1", "K1andSK", null}},
                                    {},
                                    true},
                                    [{?STD_TAG, skip}]),
    ?assertMatch(skip, TagStrat1),
    TagStrat2 = compact_inkerkvc({{1,
                                        ?INKT_KEYD,
                                        {?STD_TAG, "B1", "K1andSK", null}},
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
    

%% Test below proved that the overhead of performing hashes was trivial
%% Maybe 5 microseconds per hash

hashperf_test() ->
    OL = lists:map(fun(_X) -> leveled_rand:rand_bytes(8192) end, lists:seq(1, 1000)),
    SW = os:timestamp(),
    _HL = lists:map(fun(Obj) -> erlang:phash2(Obj) end, OL),
    io:format(user, "1000 object hashes in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)]).

parsedate_test() ->
    {MeS, S, MiS} = os:timestamp(),
    timer:sleep(100),
    Now = leveled_util:integer_now(),
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
    Now = leveled_util:integer_now() + 60 * 60,
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

delayedupdate_aaeidx_test() ->
    AAE = #recent_aae{filter=blacklist,
                        buckets=[],
                        limit_minutes=60,
                        unit_minutes=5},
    Bucket = <<"Bucket1">>,
    Key = <<"Key1">>,
    SQN = 1,
    H = erlang:phash2(null),
    {Mega, Sec, MSec} = os:timestamp(),
    LastMods = [{Mega -1, Sec, MSec}],
    AAESpecs = aae_indexspecs(AAE, Bucket, Key, SQN, H, LastMods),
    ?assertMatch(0, length(AAESpecs)).

head_segment_compare_test() ->
    % Reminder to align native and parallel(leveled_ko) key stores for 
    % kv_index_tictactree
    H1 = segment_hash({?HEAD_TAG, <<"B1">>, <<"K1">>, null}),
    H2 = segment_hash({?RIAK_TAG, <<"B1">>, <<"K1">>, null}),
    H3 = segment_hash({?HEAD_TAG, <<"B1">>, <<"K1">>, <<>>}),
    ?assertMatch(H1, H2),
    ?assertMatch(H1, H3).

-endif.
