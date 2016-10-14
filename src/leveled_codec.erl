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
%% - o_rkv@v1 (riak objects)
%% - i (index entries)


-module(leveled_codec).

-include("../include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([strip_to_keyonly/1,
        strip_to_keyseqonly/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        strip_to_keyseqstatusonly/1,
        striphead_to_details/1,
        endkey_passed/2,
        key_dominates/2,
        print_key/1,
        to_ledgerkey/3,
        build_metadata_object/2,
        generate_ledgerkv/4,
        generate_ledgerkv/5,
        get_size/2,
        convert_indexspecs/4,
        riakto_keydetails/1]).         



strip_to_keyonly({keyonly, K}) -> K;
strip_to_keyonly({K, _V}) -> K.

strip_to_keyseqonly({K, {SeqN, _, _ }}) -> {K, SeqN}.

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
        
to_ledgerkey(Bucket, Key, Tag) ->
    {Tag, Bucket, Key, null}.

hash(Obj) ->
    erlang:phash2(term_to_binary(Obj)).

% Return a tuple of string to ease the printing of keys to logs
print_key(Key) ->
    case Key of
        {o, B, K, _SK} ->
            {"Object", B, K};
        {o_rkv@v1, B, K, _SK} ->
            {"RiakObject", B, K};
        {i, B, {F, _V}, _K} ->
            {"Index", B, F}
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
    lists:map(fun({IndexOp, IndexField, IndexValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        %% TODO: timestamp support
                                        {active, infinity};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        {tomb, infinity}
                                end,
                        {{i, Bucket, {IndexField, IndexValue}, Key},
                            {SQN, Status, null}}
                    end,
                IndexSpecs).

generate_ledgerkv(PrimaryKey, SQN, Obj, Size) ->
    generate_ledgerkv(PrimaryKey, SQN, Obj, Size, infinity).

generate_ledgerkv(PrimaryKey, SQN, Obj, Size, TS) ->
    {Tag, Bucket, Key, _} = PrimaryKey,
    {Bucket,
        Key,
        {PrimaryKey, {SQN, {active, TS}, extract_metadata(Obj, Size, Tag)}}}.


extract_metadata(Obj, Size, o_rkv@v1) ->
    riak_extract_metadata(Obj, Size);
extract_metadata(Obj, Size, o) ->
    {hash(Obj), Size}.

get_size(PK, Value) ->
    {Tag, _Bucket, _Key, _} = PK,
    {_, _, MD} = Value,
    case Tag of
        o_rkv@v1 ->
            {_RMD, _VC, _Hash, Size} = MD,
            Size;
        o ->
            {_Hash, Size} = MD,
            Size
    end.
    

build_metadata_object(PrimaryKey, MD) ->
    {Tag, Bucket, Key, null} = PrimaryKey,
    case Tag of
        o_rkv@v1 ->
            riak_metadata_object(Bucket, Key, MD);
        o ->
            MD
    end.




riak_metadata_object(Bucket, Key, MD) ->
    {RMD, VC, _Hash, _Size} = MD,
    Contents = lists:foldl(fun(X, Acc) -> Acc ++ [#r_content{metadata=X}] end,
                            [],
                            RMD),
    #r_object{contents=Contents, bucket=Bucket, key=Key, vclock=VC}.

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
                    {1, {tomb, infinity}, null}}, lists:nth(3, Changes)).
    
-endif.