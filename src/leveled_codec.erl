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

-export([strip_to_keyonly/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_keyseqstatusonly/1,
        striphead_to_details/1,
        is_active/2,
        endkey_passed/2,
        key_dominates/2,
        maybe_reap_expiredkey/2,
        print_key/1,
        to_ledgerkey/3,
        to_ledgerkey/5,
        from_ledgerkey/1,
        build_metadata_object/2,
        generate_ledgerkv/4,
        generate_ledgerkv/5,
        get_size/2,
        convert_indexspecs/4,
        riakto_keydetails/1,
        generate_uuid/0]).         


%% Credit to
%% https://github.com/afiskon/erlang-uuid-v4/blob/master/src/uuid.erl
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:rand_bytes(16),
    io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b", 
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]).


strip_to_keyonly({keyonly, K}) -> K;
strip_to_keyonly({K, _V}) -> K.

strip_to_keyseqstatusonly({K, {SeqN, St, _MD}}) -> {K, SeqN, St}.

strip_to_statusonly({_, {_, St, _}}) -> St.

strip_to_seqonly({_, {SeqN, _, _}}) -> SeqN.

striphead_to_details({SeqN, St, MD}) -> {SeqN, St, MD}.

key_dominates(LeftKey, RightKey) ->
    case {LeftKey, RightKey} of
        {{LK, _LVAL}, {RK, _RVAL}} when LK < RK ->
            left_hand_first;
        {{LK, _LVAL}, {RK, _RVAL}} when RK < LK ->
            right_hand_first;
        {{LK, {LSN, _LST, _LMD}}, {RK, {RSN, _RST, _RMD}}}
                                                when LK == RK, LSN >= RSN ->
            left_hand_dominant;
        {{LK, {LSN, _LST, _LMD}}, {RK, {RSN, _RST, _RMD}}}
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

is_active(Key, Value) ->
    case strip_to_statusonly({Key, Value}) of
        {active, infinity} ->
            true;
        tomb ->
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
    {B_STR, FB} = check_for_string(B_TERM),
    {C_STR, FC} = check_for_string(C_TERM),
    {A_STR, B_STR, C_STR, FB, FC}.

check_for_string(Item) ->
    if
        is_binary(Item) == true ->
            {binary_to_list(Item), "~s"};
        is_integer(Item) == true ->
            {integer_to_list(Item), "~s"};
        is_list(Item) == true ->
            {Item, "~s"};
        true ->
            {Item, "~w"}
    end.
                    

% Compare a key against a query key, only comparing elements that are non-null
% in the Query key.  This is used for comparing against end keys in queries.
endkey_passed({EK1, null, null, null}, {CK1, _, _, _}) ->
    EK1 < CK1;
endkey_passed({EK1, EK2, null, null}, {CK1, CK2, _, _}) ->
    {EK1, EK2} < {CK1, CK2};
endkey_passed({EK1, EK2, EK3, null}, {CK1, CK2, CK3, _}) ->
    {EK1, EK2, EK3} < {CK1, CK2, CK3};
endkey_passed(EndKey, CheckingKey) ->
    EndKey < CheckingKey.

convert_indexspecs(IndexSpecs, Bucket, Key, SQN) ->
    lists:map(fun({IndexOp, IdxField, IdxValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        %% TODO: timestamp support
                                        {active, infinity};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        tomb
                                end,
                        {to_ledgerkey(Bucket, Key, ?IDX_TAG,
                                IdxField, IdxValue),
                            {SQN, Status, null}}
                    end,
                IndexSpecs).

generate_ledgerkv(PrimaryKey, SQN, Obj, Size) ->
    generate_ledgerkv(PrimaryKey, SQN, Obj, Size, infinity).

generate_ledgerkv(PrimaryKey, SQN, Obj, Size, TS) ->
    {Tag, Bucket, Key, _} = PrimaryKey,
    Status = case Obj of
                    delete ->
                        tomb;
                    _ ->
                        {active, TS}
                end,
    {Bucket,
        Key,
        {PrimaryKey, {SQN, Status, extract_metadata(Obj, Size, Tag)}}}.




extract_metadata(Obj, Size, ?RIAK_TAG) ->
    riak_extract_metadata(Obj, Size);
extract_metadata(Obj, Size, ?STD_TAG) ->
    {hash(Obj), Size}.

get_size(PK, Value) ->
    {Tag, _Bucket, _Key, _} = PK,
    {_, _, MD} = Value,
    case Tag of
        ?RIAK_TAG ->
            {_RMD, _VC, _Hash, Size} = MD,
            Size;
        ?STD_TAG ->
            {_Hash, Size} = MD,
            Size
    end.
    

build_metadata_object(PrimaryKey, MD) ->
    {Tag, Bucket, Key, null} = PrimaryKey,
    case Tag of
        ?RIAK_TAG ->
            riak_metadata_object(Bucket, Key, MD);
        ?STD_TAG ->
            MD
    end.




riak_metadata_object(Bucket, Key, MD) ->
    {RMD, VC, _Hash, _Size} = MD,
    Contents = lists:foldl(fun(X, Acc) -> Acc ++ [#r_content{metadata=X}] end,
                            [],
                            RMD),
    #r_object{contents=Contents, bucket=Bucket, key=Key, vclock=VC}.

riak_extract_metadata(delete, Size) ->
    {delete, null, null, Size};
riak_extract_metadata(Obj, Size) ->
    {get_metadatas(Obj), vclock(Obj), riak_hash(Obj), Size}.

riak_hash(Obj=#r_object{}) ->
    Vclock = vclock(Obj),
    UpdObj = set_vclock(Obj, lists:sort(Vclock)),
    erlang:phash2(term_to_binary(UpdObj)).

riakto_keydetails(Object) ->
    {Object#r_object.bucket, Object#r_object.key}.

get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

vclock(#r_object{vclock=VClock}) -> VClock.





%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


indexspecs_test() ->
    IndexSpecs = [{add, "t1_int", 456},
                    {add, "t1_bin", "adbc123"},
                    {remove, "t1_bin", "abdc456"}],
    Changes = convert_indexspecs(IndexSpecs, "Bucket", "Key2", 1),
    ?assertMatch({{i, "Bucket", {"t1_int", 456}, "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(1, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "adbc123"}, "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(2, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "abdc456"}, "Key2"},
                    {1, tomb, null}}, lists:nth(3, Changes)).

endkey_passed_test() ->
    TestKey = {i, null, null, null},
    K1 = {i, 123, {"a", "b"}, <<>>},
    K2 = {o, 123, {"a", "b"}, <<>>},
    ?assertMatch(false, endkey_passed(TestKey, K1)),
    ?assertMatch(true, endkey_passed(TestKey, K2)).

stringcheck_test() ->
    ?assertMatch({"Bucket", "~s"}, check_for_string("Bucket")),
    ?assertMatch({"Bucket", "~s"}, check_for_string(<<"Bucket">>)),
    ?assertMatch({bucket, "~w"}, check_for_string(bucket)).

-endif.