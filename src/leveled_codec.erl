%% -------- Key Codec ---------
%%
%% Functions for manipulating keys and values within leveled.  These are
%% currently static functions, they cannot be overridden in the store other
%% than by changing them here.  The formats are focused on the problem of
%% supporting Riak KV

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
        extract_metadata/2,
        build_metadata_object/2,
        convert_indexspecs/3]).
        

strip_to_keyonly({keyonly, K}) -> K;
strip_to_keyonly({K, _V}) -> K.

strip_to_keyseqonly({K, {SeqN, _, _}}) -> {K, SeqN}.

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
        


get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

vclock(#r_object{vclock=VClock}) -> VClock.

to_binary(v0, Obj) ->
    term_to_binary(Obj).

hash(Obj=#r_object{}) ->
    Vclock = vclock(Obj),
    UpdObj = set_vclock(Obj, lists:sort(Vclock)),
    erlang:phash2(to_binary(v0, UpdObj)).

extract_metadata(Obj, Size) ->
    {get_metadatas(Obj), vclock(Obj), hash(Obj), Size}.


build_metadata_object(PrimaryKey, Head) ->
    {o, Bucket, Key, null} = PrimaryKey,
    {MD, VC, _, _} = Head,
    Contents = lists:foldl(fun(X, Acc) -> Acc ++ [#r_content{metadata=X}] end,
                            [],
                            MD),
    #r_object{contents=Contents, bucket=Bucket, key=Key, vclock=VC}.

convert_indexspecs(IndexSpecs, SQN, PrimaryKey) ->
    lists:map(fun({IndexOp, IndexField, IndexValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        %% TODO: timestamp support
                                        {active, infinity};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        {tomb, infinity}
                                end,
                        {o, B, K, _SK} = PrimaryKey,
                        {{i, B, {IndexField, IndexValue}, K},
                            {SQN, Status, null}}
                    end,
                IndexSpecs).

% Return a tuple of string to ease the printing of keys to logs
print_key(Key) ->
    case Key of
        {o, B, K, _SK} ->
            {"Object", B, K};
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



%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


indexspecs_test() ->
    IndexSpecs = [{add, "t1_int", 456},
                    {add, "t1_bin", "adbc123"},
                    {remove, "t1_bin", "abdc456"}],
    Changes = convert_indexspecs(IndexSpecs, 1, {o, "Bucket", "Key2", null}),
    ?assertMatch({{i, "Bucket", {"t1_int", 456}, "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(1, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "adbc123"}, "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(2, Changes)),
    ?assertMatch({{i, "Bucket", {"t1_bin", "abdc456"}, "Key2"},
                    {1, {tomb, infinity}, null}}, lists:nth(3, Changes)).
    
-endif.