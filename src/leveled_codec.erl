%% -------- Key Codec ---------
%%
%% Functions for manipulating keys and values within leveled.
%%
%% Any thing specific to handling of a given tag should be encapsulated 
%% within the leveled_head module


-module(leveled_codec).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
        inker_reload_strategy/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_segmentonly/1,
        strip_to_keyseqonly/1,
        strip_to_indexdetails/1,
        striphead_to_v1details/1,
        is_active/3,
        endkey_passed/2,
        key_dominates/2,
        maybe_reap_expiredkey/2,
        to_ledgerkey/3,
        to_ledgerkey/5,
        from_ledgerkey/1,
        from_ledgerkey/2,
        isvalid_ledgerkey/1,
        to_inkerkey/2,
        to_inkerkv/6,
        from_inkerkv/1,
        from_inkerkv/2,
        from_journalkey/1,
        revert_to_keydeltas/2,
        is_compaction_candidate/1,
        split_inkvalue/1,
        check_forinkertype/2,
        get_tagstrategy/2,
        maybe_compress/2,
        create_value_for_journal/3,
        generate_ledgerkv/5,
        get_size/2,
        get_keyandobjhash/2,
        idx_indexspecs/5,
        obj_objectspecs/3,
        segment_hash/1,
        to_lookup/1,
        next_key/1,
        return_proxy/4]).         

-define(LMD_FORMAT, "~4..0w~2..0w~2..0w~2..0w~2..0w").
-define(NRT_IDX, "$aae.").

-type tag() :: 
        leveled_head:object_tag()|?IDX_TAG|?HEAD_TAG|atom().
-type key() :: 
        binary()|string()|{binary(), binary()}.
        % Keys SHOULD be binary()
        % string() support is a legacy of old tests
-type sqn() ::
        % SQN of the object in the Journal
        pos_integer().
-type segment_hash() :: 
        % hash of the key to an aae segment - to be used in ledger filters
        {integer(), integer()}|no_lookup.
-type metadata() ::
        tuple()|null. % null for empty metadata
-type last_moddate() ::
        % modified date as determined by the object (not this store)
        % if the object has siblings in the store will be the maximum of those
        % dates
        integer()|undefined.
-type lastmod_range() :: {integer(), pos_integer()|infinity}.

-type ledger_status() ::
        tomb|{active, non_neg_integer()|infinity}.
-type ledger_key() :: 
        {tag(), any(), any(), any()}|all.
-type ledger_value() ::
        ledger_value_v1()|ledger_value_v2().
-type ledger_value_v1() ::
        {sqn(), ledger_status(), segment_hash(), metadata()}.
-type ledger_value_v2() ::
        {sqn(), ledger_status(), segment_hash(), metadata(), last_moddate()}.
-type ledger_kv() ::
        {ledger_key(), ledger_value()}.
-type compaction_method() ::
        retain|skip|recalc.
-type compaction_strategy() ::
        list({tag(), compaction_method()}).
-type journal_key_tag() ::
        ?INKT_STND|?INKT_TOMB|?INKT_MPUT|?INKT_KEYD.
-type journal_key() ::
        {sqn(), journal_key_tag(), ledger_key()}.
-type journal_ref() ::
        {ledger_key(), sqn()}.
-type object_spec_v0() ::
        {add|remove, key(), key(), key()|null, any()}.
-type object_spec_v1() ::
        {add|remove, v1, key(), key(), key()|null, 
            list(erlang:timestamp())|undefined, any()}.
-type object_spec() ::
        object_spec_v0()|object_spec_v1().
-type compression_method() ::
        lz4|native.
-type index_specs() ::
        list({add|remove, any(), any()}).
-type journal_keychanges() :: 
        {index_specs(), infinity|integer()}. % {KeyChanges, TTL}
-type maybe_lookup() ::
        lookup|no_lookup.
-type regular_expression() ::
        {re_pattern, term(), term(), term(), term()}|undefined. 
    % first element must be re_pattern, but tuple may change legnth with
    % versions

-type value_fetcher() ::
    {fun((pid(), leveled_codec:journal_key()) -> any()),
        pid(), leveled_codec:journal_key()}.
    % A 2-arity function, which when passed the other two elements of the tuple
    % will return the value
-type proxy_object() ::
    {proxy_object, leveled_head:head(), non_neg_integer(), value_fetcher()}.
    % Returns the head, size and a tuple for accessing the value
-type proxy_objectbin() ::
    binary().
    % using term_to_binary(proxy_object())


-type segment_list() 
        :: list(integer())|false.

-export_type([tag/0,
                key/0,
                object_spec/0,
                segment_hash/0,
                ledger_status/0,
                ledger_key/0,
                ledger_value/0,
                ledger_kv/0,
                compaction_strategy/0,
                compaction_method/0,
                journal_key_tag/0,
                journal_key/0,
                journal_ref/0,
                compression_method/0,
                journal_keychanges/0,
                index_specs/0,
                segment_list/0,
                maybe_lookup/0,
                last_moddate/0,
                lastmod_range/0,
                regular_expression/0,
                value_fetcher/0,
                proxy_object/0]).


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
    {segment_hash, SegmentID, ExtraHash, _AltHash}
        = leveled_tictac:keyto_segment48(Key),
    {SegmentID, ExtraHash};
segment_hash(KeyTuple) when is_tuple(KeyTuple) ->
    BinKey = 
        case element(1, KeyTuple) of
            ?HEAD_TAG ->
                headkey_to_canonicalbinary(KeyTuple);
            _ ->
                leveled_head:key_to_canonicalbinary(KeyTuple)
        end,
    segment_hash(BinKey).


headkey_to_canonicalbinary({?HEAD_TAG, Bucket, Key, SubK})
                    when is_binary(Bucket), is_binary(Key), is_binary(SubK) ->
    <<Bucket/binary, Key/binary, SubK/binary>>;
headkey_to_canonicalbinary({?HEAD_TAG, Bucket, Key, null})
                                    when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>;
headkey_to_canonicalbinary({?HEAD_TAG, {BucketType, Bucket}, Key, SubKey})
                            when is_binary(BucketType), is_binary(Bucket) ->
    headkey_to_canonicalbinary({?HEAD_TAG,
                                <<BucketType/binary, Bucket/binary>>, 
                                Key,
                                SubKey});
headkey_to_canonicalbinary(Key) when element(1, Key) == ?HEAD_TAG ->
    % In unit tests head specs can have non-binary keys, so handle
    % this through hashing the whole key
    term_to_binary(Key).


-spec to_lookup(ledger_key()) -> maybe_lookup().
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
strip_to_statusonly({_, V}) -> element(2, V).

-spec strip_to_seqonly(ledger_kv()) -> non_neg_integer().
strip_to_seqonly({_, V}) -> element(1, V).

-spec strip_to_segmentonly(ledger_kv()) -> segment_hash().
strip_to_segmentonly({_LK, LV}) -> element(3, LV).

-spec strip_to_keyseqonly(ledger_kv()) -> {ledger_key(), integer()}.
strip_to_keyseqonly({LK, V}) -> {LK, element(1, V)}.

-spec strip_to_indexdetails(ledger_kv()) ->
                                {integer(), segment_hash(), last_moddate()}.
strip_to_indexdetails({_, V}) when tuple_size(V) == 4 -> 
    % A v1 value
    {element(1, V), element(3, V), undefined};
strip_to_indexdetails({_, V}) when tuple_size(V) > 4 ->
    % A v2 value should have a fith element - Last Modified Date
    {element(1, V), element(3, V), element(5, V)}.

-spec striphead_to_v1details(ledger_value()) -> ledger_value().
striphead_to_v1details(V) -> 
    {element(1, V), element(2, V), element(3, V), element(4, V)}.

-spec key_dominates(ledger_kv(), ledger_kv()) -> 
    left_hand_first|right_hand_first|left_hand_dominant|right_hand_dominant.
%% @doc
%% When comparing two keys in the ledger need to find if one key comes before 
%% the other, or if the match, which key is "better" and should be the winner
key_dominates({LK, _LVAL}, {RK, _RVAL}) when LK < RK ->
    left_hand_first;
key_dominates({LK, _LVAL}, {RK, _RVAL}) when RK < LK ->
    right_hand_first;
key_dominates(LObj, RObj) ->
    case strip_to_seqonly(LObj) >= strip_to_seqonly(RObj) of
        true ->
            left_hand_dominant;
        false ->
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

%% No spec - due to tests
%% @doc
%% Check that the ledgerkey is a valid format, to handle un-checksummed keys
%% that may be returned corrupted (such as from the Journal)
isvalid_ledgerkey({Tag, _B, _K, _SK}) ->
    is_atom(Tag);
isvalid_ledgerkey(_LK) ->
    false.

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
    ReloadStrategy0 = 
        lists:map(fun leveled_head:default_reload_strategy/1,
                    leveled_head:defined_objecttags()),
    lists:foldl(fun({X, Y}, SList) ->
                        lists:keyreplace(X, 1, SList, {X, Y})
                        end,
                    ReloadStrategy0,
                    AltList).


-spec get_tagstrategy(ledger_key(), compaction_strategy()) 
                                                    -> skip|retain|recalc.
%% @doc
%% Work out the compaction strategy for the key
get_tagstrategy({Tag, _, _, _}, Strategy) ->
    case lists:keyfind(Tag, 1, Strategy) of
        {Tag, TagStrat} ->
            TagStrat;
        false ->
            leveled_log:log("IC012", [Tag, Strategy]),
            retain
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

-spec revert_to_keydeltas(journal_key(), any()) -> {journal_key(), any()}.
%% @doc
%% If we wish to retain key deltas when an object in the Journal has been
%% replaced - then this converts a Journal Key and Value into one which has no
%% object body just the key deltas.
revert_to_keydeltas({SQN, ?INKT_STND, LedgerKey}, InkerV) ->
    {_V, KeyDeltas} = revert_value_from_journal(InkerV),
    {{SQN, ?INKT_KEYD, LedgerKey}, {null, KeyDeltas}};
revert_to_keydeltas(JournalKey, InkerV) ->
    {JournalKey, InkerV}.

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

-spec is_compaction_candidate(journal_key()) -> boolean().
%% @doc
%% Only journal keys with standard objects should be scored for compaction
is_compaction_candidate({_SQN, ?INKT_STND, _LK}) ->
    true;
is_compaction_candidate(_OtherJKType) ->
    false.


%%%============================================================================
%%% Other Ledger Functions
%%%============================================================================


-spec obj_objectspecs(list(tuple()), integer(), integer()|infinity) 
                                                        -> list(ledger_kv()).
%% @doc
%% Convert object specs to KV entries ready for the ledger
obj_objectspecs(ObjectSpecs, SQN, TTL) ->
    lists:map(fun(ObjectSpec) -> gen_headspec(ObjectSpec, SQN, TTL) end,
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
    {to_ledgerkey(Bucket, Key, ?IDX_TAG, IdxField, IdxTerm),
        {SQN, Status, no_lookup, null}}.

-spec gen_headspec(object_spec(), integer(), integer()|infinity) -> ledger_kv().
%% @doc
%% Take an object_spec as passed in a book_mput, and convert it into to a
%% valid ledger key and value.  Supports different shaped tuples for different
%% versions of the object_spec
gen_headspec({IdxOp, v1, Bucket, Key, SubKey, LMD, Value}, SQN, TTL) ->
     % v1 object spec
    Status = set_status(IdxOp, TTL),
    K = to_ledgerkey(Bucket, {Key, SubKey}, ?HEAD_TAG),
    {K, {SQN, Status, segment_hash(K), Value, get_last_lastmodification(LMD)}};
gen_headspec({IdxOp, Bucket, Key, SubKey, Value}, SQN, TTL) ->
    % v0 object spec
    Status = set_status(IdxOp, TTL),
    K = to_ledgerkey(Bucket, {Key, SubKey}, ?HEAD_TAG),
    {K, {SQN, Status, segment_hash(K), Value, undefined}}.


-spec return_proxy(leveled_head:object_tag()|leveled_head:headonly_tag(),
                    leveled_head:object_metadata(),
                    pid(), journal_ref())
                        -> proxy_objectbin()|leveled_head:object_metadata().
%% @doc
%% If the object has a value, return the metadata and a proxy through which
%% the applictaion or runner can access the value.  If it is a ?HEAD_TAG
%% then it has no value, so just return the metadata
return_proxy(?HEAD_TAG, ObjectMetadata, _InkerClone, _JR) ->
    % Object has no value - so proxy object makese no sense, just return the
    % metadata as is
    ObjectMetadata;
return_proxy(Tag, ObjMetadata, InkerClone, JournalRef) ->
    Size = leveled_head:get_size(Tag, ObjMetadata),
    HeadBin = leveled_head:build_head(Tag, ObjMetadata),
    term_to_binary({proxy_object,
                    HeadBin,
                    Size,
                    {fun leveled_bookie:fetch_value/2,
                        InkerClone,
                        JournalRef}}).

set_status(add, TTL) ->
    {active, TTL};
set_status(remove, _TTL) ->
    %% TODO: timestamps for delayed reaping 
    tomb.

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
    {MD, LastMods} = leveled_head:extract_metadata(Tag, Size, Obj),
    ObjHash = leveled_head:get_hash(Tag, MD),
    Value = {SQN,
                Status,
                Hash,
                MD,
                get_last_lastmodification(LastMods)},
    {Bucket, Key, Value, {Hash, ObjHash}, LastMods}.

-spec get_last_lastmodification(list(erlang:timestamp())|undefined) 
                                                -> pos_integer()|undefined.
%% @doc
%% Get the highest of the last modifications measured in seconds.  This will be
%% stored as 4 bytes (unsigned) so will last for another 80 + years
get_last_lastmodification(undefined) ->
    undefined;
get_last_lastmodification([]) ->
    undefined;
get_last_lastmodification(LastMods) ->
    {Mega, Sec, _Micro} = lists:max(LastMods),
    Mega * 1000000 + Sec.

get_size(PK, Value) ->
    {Tag, _Bucket, _Key, _} = PK,
    MD = element(4, Value),
    leveled_head:get_size(Tag, MD).

-spec get_keyandobjhash(tuple(), tuple()) -> tuple().
%% @doc
%% Return a tucple of {Bucket, Key, Hash} where hash is a hash of the object
%% not the key (for example with Riak tagged objects this will be a hash of
%% the sorted vclock)
get_keyandobjhash(LK, Value) ->
    {Tag, Bucket, Key, _} = LK,
    MD = element(4, Value),
    case Tag of
        ?IDX_TAG ->
            from_ledgerkey(LK); % returns {Bucket, Key, IdxValue}
        _ ->
            {Bucket, Key, leveled_head:get_hash(Tag, MD)}
    end.

-spec next_key(key()) -> key().
%% @doc
%% Get the next key to iterate from a given point
next_key(Key) when is_binary(Key) ->
    <<Key/binary, 0>>;
next_key(Key) when is_list(Key) ->
    Key ++ [0];
next_key({Type, Bucket}) when is_binary(Type), is_binary(Bucket) ->
    {Type, next_key(Bucket)}.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

valid_ledgerkey_test() ->
    UserDefTag = {user_defined, <<"B">>, <<"K">>, null},
    ?assertMatch(true, isvalid_ledgerkey(UserDefTag)),
    KeyNotTuple = [?STD_TAG, <<"B">>, <<"K">>, null],
    ?assertMatch(false, isvalid_ledgerkey(KeyNotTuple)),
    TagNotAtom = {"tag", <<"B">>, <<"K">>, null},
    ?assertMatch(false, isvalid_ledgerkey(TagNotAtom)),
    ?assertMatch(retain, get_tagstrategy(UserDefTag, inker_reload_strategy([]))).

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



%% Test below proved that the overhead of performing hashes was trivial
%% Maybe 5 microseconds per hash

hashperf_test() ->
    OL = lists:map(fun(_X) -> leveled_rand:rand_bytes(8192) end, lists:seq(1, 1000)),
    SW = os:timestamp(),
    _HL = lists:map(fun(Obj) -> erlang:phash2(Obj) end, OL),
    io:format(user, "1000 object hashes in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), SW)]).

head_segment_compare_test() ->
    % Reminder to align native and parallel(leveled_ko) key stores for 
    % kv_index_tictactree
    H1 = segment_hash({?HEAD_TAG, <<"B1">>, <<"K1">>, null}),
    H2 = segment_hash({?RIAK_TAG, <<"B1">>, <<"K1">>, null}),
    H3 = segment_hash({?HEAD_TAG, <<"B1">>, <<"K1">>, <<>>}),
    ?assertMatch(H1, H2),
    ?assertMatch(H1, H3).

headspec_v0v1_test() ->
    % A v0 object spec generates the same outcome as a v1 object spec with the
    % last modified date undefined
    V1 = {add, v1, <<"B">>, <<"K">>, <<"SK">>, undefined, <<"V">>},
    V0 = {add, <<"B">>, <<"K">>, <<"SK">>, <<"V">>},
    TTL = infinity,
    ?assertMatch(true, gen_headspec(V0, 1, TTL) == gen_headspec(V1, 1, TTL)).


-endif.
