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
        strip_to_keyonly/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_keyseqonly/1,
        strip_to_seqnhashonly/1,
        striphead_to_details/1,
        is_active/3,
        endkey_passed/2,
        key_dominates/2,
        maybe_reap_expiredkey/2,
        print_key/1,
        to_ledgerkey/3,
        to_ledgerkey/5,
        from_ledgerkey/1,
        to_inkerkv/4,
        from_inkerkv/1,
        from_journalkey/1,
        compact_inkerkvc/2,
        split_inkvalue/1,
        check_forinkertype/2,
        create_value_for_journal/1,
        build_metadata_object/2,
        generate_ledgerkv/5,
        get_size/2,
        get_keyandhash/2,
        convert_indexspecs/5,
        generate_uuid/0,
        integer_now/0,
        riak_extract_metadata/2,
        magic_hash/1]).         

-define(V1_VERS, 1).
-define(MAGIC, 53). % riak_kv -> riak_object

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


%% Credit to
%% https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:rand_bytes(16),
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

strip_to_keyonly({K, _V}) -> K.

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

from_ledgerkey({Tag, Bucket, {_IdxField, IdxValue}, Key})
                                                when Tag == ?IDX_TAG ->
    {Bucket, Key, IdxValue};
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
    Value = create_value_for_journal({Object, KeyChanges}),
    {{SQN, InkerType, LedgerKey}, Value}.

%% Used when fetching objects, so only handles standard, hashable entries
from_inkerkv(Object) ->
    case Object of
        {{SQN, ?INKT_STND, PK}, Bin} when is_binary(Bin) ->
            {{SQN, PK}, binary_to_term(Bin)};
        {{SQN, ?INKT_STND, PK}, Term} ->
            {{SQN, PK}, Term};
        _ ->
            Object
    end.

from_journalkey({SQN, _Type, LedgerKey}) ->
    {SQN, LedgerKey}.

compact_inkerkvc({_InkerKey, crc_wonky, false}, _Strategy) ->
    skip;
compact_inkerkvc({{_SQN, ?INKT_TOMB, _LK}, _V, _CrcCheck}, _Strategy) ->
    skip;
compact_inkerkvc({{SQN, ?INKT_KEYD, LK}, V, CrcCheck}, Strategy) ->
    {Tag, _, _, _} = LK,
    {Tag, TagStrat} = lists:keyfind(Tag, 1, Strategy),
    case TagStrat of
        retain ->
            {retain, {{SQN, ?INKT_KEYD, LK}, V, CrcCheck}};
        TagStrat ->
            {TagStrat, null}
    end;
compact_inkerkvc({{SQN, ?INKT_STND, LK}, V, CrcCheck}, Strategy) ->
    {Tag, _, _, _} = LK,
    case lists:keyfind(Tag, 1, Strategy) of
        {Tag, TagStrat} ->
            case TagStrat of
                retain ->
                    {_V, KeyDeltas} = split_inkvalue(V),    
                    {TagStrat, {{SQN, ?INKT_KEYD, LK}, {null, KeyDeltas}, CrcCheck}}; 
                TagStrat ->
                    {TagStrat, null}
            end;
        false ->
            leveled_log:log("IC012", [Tag, Strategy]),
            skip
    end;
compact_inkerkvc(_KVC, _Strategy) ->
    skip.

split_inkvalue(VBin) ->
    case is_binary(VBin) of
            true ->
                binary_to_term(VBin);
            false ->
                VBin
        end.

check_forinkertype(_LedgerKey, delete) ->
    ?INKT_TOMB;
check_forinkertype(_LedgerKey, _Object) ->
    ?INKT_STND.

create_value_for_journal(Value) ->
    case Value of
        {Object, KeyChanges} ->
            term_to_binary({Object, KeyChanges}, [compressed]);
        Value when is_binary(Value) ->
            Value
    end.

hash(Obj) ->
    erlang:phash2(term_to_binary(Obj)).

% Return a tuple of strings to ease the printing of keys to logs
print_key(Key) ->
    {A_STR, B_TERM, C_TERM} = case Key of
                                    {?STD_TAG, B, K, _SK} ->
                                        {"Object", B, K};
                                    {?RIAK_TAG, B, K, _SK} ->
                                        {"RiakObject", B, K};
                                    {?IDX_TAG, B, {F, _V}, _K} ->
                                        {"Index", B, F}
                                end,
    B_STR = turn_to_string(B_TERM),
    C_STR = turn_to_string(C_TERM),
    {A_STR, B_STR, C_STR}.

turn_to_string(Item) ->
    if
        is_binary(Item) == true ->
            binary_to_list(Item);
        is_integer(Item) == true ->
            integer_to_list(Item);
        is_list(Item) == true ->
            Item;
        true ->
            [Output] = io_lib:format("~w", [Item]),
            Output
    end.
                    

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

convert_indexspecs(IndexSpecs, Bucket, Key, SQN, TTL) ->
    lists:map(fun({IndexOp, IdxField, IdxValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        {active, TTL};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        tomb
                                end,
                        {to_ledgerkey(Bucket, Key, ?IDX_TAG,
                                IdxField, IdxValue),
                            {SQN, Status, no_lookup, null}}
                    end,
                IndexSpecs).

generate_ledgerkv(PrimaryKey, SQN, Obj, Size, TS) ->
    {Tag, Bucket, Key, _} = PrimaryKey,
    Status = case Obj of
                    delete ->
                        tomb;
                    _ ->
                        {active, TS}
                end,
    Hash = magic_hash(PrimaryKey),
    Value = {SQN,
                Status,
                Hash,
                extract_metadata(Obj, Size, Tag)},
    {Bucket, Key, {PrimaryKey, Value}, Hash}.


integer_now() ->
    integer_time(os:timestamp()).

integer_time(TS) ->
    DT = calendar:now_to_universal_time(TS),
    calendar:datetime_to_gregorian_seconds(DT).

extract_metadata(Obj, Size, ?RIAK_TAG) ->
    riak_extract_metadata(Obj, Size);
extract_metadata(Obj, Size, ?STD_TAG) ->
    {hash(Obj), Size}.

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
    
get_keyandhash(LK, Value) ->
    {Tag, Bucket, Key, _} = LK,
    {_, _, _, MD} = Value,
    case Tag of
        ?RIAK_TAG ->
            {_RMD, _VC, Hash, _Size} = MD,
            {Bucket, Key, Hash};
        ?STD_TAG ->
            {Hash, _Size} = MD,
            {Bucket, Key, Hash}
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
    {delete, null, null, Size};
riak_extract_metadata(ObjBin, Size) ->
    {Vclock, SibData} = riak_metadata_from_binary(ObjBin),
    {SibData, Vclock, erlang:phash2(ObjBin), Size}.

%% <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
%%%     VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

riak_metadata_to_binary(Vclock, SibData) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    % <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
    %         VclockBin:VclockLen/binary, SibData:32/integer>>.
    SibCount = length(SibData),
    SibsBin = slimbin_contents(SibData),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
            VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>>.
    
riak_metadata_from_binary(V1Binary) ->
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
            Rest/binary>> = V1Binary,
    % Just grab the Sibling count and not the full metadata
    % <<VclockBin:VclockLen/binary, SibCount:32/integer, _Rest/binary>> = Rest,
    % {binary_to_term(VclockBin), SibCount}.
    <<VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> = Rest,
    SibMetaBinList =
        case SibCount of
            0 ->
                [];
            SC when is_integer(SC) ->
                get_metadata_from_siblings(SibsBin, SibCount, [])
        end,
    {binary_to_term(VclockBin), SibMetaBinList}.

% Fixes the value length for each sibling to be zero, and so includes no value
slimbin_content(MetaBin) ->
    MetaLen = byte_size(MetaBin),
    <<0:32/integer,  MetaLen:32/integer, MetaBin:MetaLen/binary>>.

slimbin_contents(SibMetaBinList) ->
    F = fun(MetaBin, Acc) ->
                <<Acc/binary, (slimbin_content(MetaBin))/binary>>
        end,
    lists:foldl(F, <<>>, SibMetaBinList).
    
get_metadata_from_siblings(<<>>, 0, SibMetaBinList) ->
    SibMetaBinList;
get_metadata_from_siblings(<<ValLen:32/integer, Rest0/binary>>,
                            SibCount,
                            SibMetaBinList) ->
    <<_ValBin:ValLen/binary, MetaLen:32/integer, Rest1/binary>> = Rest0,
    <<MetaBin:MetaLen/binary, Rest2/binary>> = Rest1,
    get_metadata_from_siblings(Rest2,
                                SibCount - 1,
                                [MetaBin|SibMetaBinList]).




%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


indexspecs_test() ->
    IndexSpecs = [{add, "t1_int", 456},
                    {add, "t1_bin", "adbc123"},
                    {remove, "t1_bin", "abdc456"}],
    Changes = convert_indexspecs(IndexSpecs, "Bucket", "Key2", 1, infinity),
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

stringcheck_test() ->
    ?assertMatch("Bucket", turn_to_string("Bucket")),
    ?assertMatch("Bucket", turn_to_string(<<"Bucket">>)),
    ?assertMatch("bucket", turn_to_string(bucket)).

%% Test below proved that the overhead of performing hashes was trivial
%% Maybe 5 microseconds per hash

%hashperf_test() ->
%    OL = lists:map(fun(_X) -> crypto:rand_bytes(8192) end, lists:seq(1, 10000)),
%    SW = os:timestamp(),
%    _HL = lists:map(fun(Obj) -> erlang:phash2(Obj) end, OL),
%    io:format(user, "10000 object hashes in ~w microseconds~n",
%                [timer:now_diff(os:timestamp(), SW)]).

-endif.