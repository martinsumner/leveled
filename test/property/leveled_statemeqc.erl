%% -------------------------------------------------------------------
%%
%% leveld_eqc: basic statem for doing things to leveled
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(leveled_statemeqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-compile([export_all, nowarn_export_all, {nowarn_deprecated_function, 
            [{gen_fsm, send_event, 2}]}]).

-define(NUMTESTS, 1000).
-define(TEST_TIMEOUT, 300).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(CMD_VALID(State, Cmd, True, False),
        case is_valid_cmd(State, Cmd) of
            true -> True;
            false -> False
        end).
                       

eqc_test_() ->
    Timeout = ?TEST_TIMEOUT,
    {timeout, max(2 * Timeout, Timeout + 10),
     ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(Timeout, ?QC_OUT(prop_db()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_db())).

check() ->
    eqc:check(prop_db()).

iff(B1, B2) -> B1 == B2.
implies(B1, B2) -> (not B1 orelse B2).

%% start_opts should not be added to this map, it is added only when the system is started the first time.
initial_state() ->
    #{dir => {var, dir},
      sut => sut,
      leveled => undefined,   %% to make adapt happy after failing pre/1
      counter => 0,
      model => orddict:new(),
      previous_keys => [],
      deleted_keys => [],
      folders => []
     }.

%% --- Operation: init_backend ---
init_backend_pre(S) ->
    not is_leveled_open(S).

init_backend_args(#{dir := Dir, sut := Name} = S) ->
    case maps:get(start_opts, S, undefined) of
        undefined ->
            [ default(?RIAK_TAG, ?STD_TAG),  %% Just test one tag at a time
              [{root_path, Dir}, {log_level, error},
                {max_sstslots, 2}, {cache_size, 10}, {max_pencillercachesize, 40}, {max_journalsize, 20000} | gen_opts()], Name ];
        Opts ->
            %% root_path is part of existing options
            [ maps:get(tag, S), Opts, Name ]
    end.

init_backend_pre(S, [Tag, Options, _]) ->
    %% for shrinking
    PreviousOptions = maps:get(start_opts, S, undefined),
    maps:get(tag, S, Tag) == Tag andalso 
        PreviousOptions == undefined orelse PreviousOptions == Options.

init_backend_adapt(S, [Tag, Options, Name]) ->
    [ maps:get(tag, S, Tag), maps:get(start_opts, S, Options), Name].

%% @doc init_backend - The actual operation
%% Start the database and read data from disk
init_backend(_Tag, Options0, Name) ->
    % Options0 = proplists:delete(log_level, Options),
    case leveled_bookie:book_start(Options0) of
        {ok, Bookie} ->
            unlink(Bookie),
            erlang:register(Name, Bookie),
            Bookie;
        Error -> Error
    end.

init_backend_next(S, LevelEdPid, [Tag, Options, _]) ->
    S#{leveled => LevelEdPid, start_opts => Options, tag => Tag}.

init_backend_post(_S, [_, _Options, _], LevelEdPid) ->
    is_pid(LevelEdPid).

init_backend_features(_S, [_Tag, Options, _], _Res) ->
    [{start_options, Options}].


%% --- Operation: stop ---
stop_pre(S) ->
    is_leveled_open(S).

%% @doc stop_args - Argument generator
stop_args(#{leveled := Pid}) ->
    [Pid].

stop_pre(#{leveled := Leveled}, [Pid]) ->
    %% check during shrinking
    Pid == Leveled.

stop_adapt(#{leveled := Leveled}, [_]) ->
    [Leveled].

%% @doc stop - The actual operation
%% Stop the server, but the values are still on disk
stop(Pid) ->
    ok = leveled_bookie:book_close(Pid).

stop_next(S, _Value, [_Pid]) ->
    S#{leveled => undefined,
        iclerk => undefined,
        folders => [],
        used_folders => [],
        stop_folders => maps:get(folders, S, []) ++ maps:get(used_folders, S, [])}.  

stop_post(_S, [Pid], _Res) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            true
    after 5000 ->
            {still_a_pid, Pid}
    end.


%% --- Operation: updateload ---
updateload_pre(S) ->
    is_leveled_open(S).

%% updateload for specific bucket (doesn't overlap with get/put/delete)
updateload_args(#{leveled := Pid, tag := Tag}) ->
    ?LET(Categories, gen_categories(Tag),
    ?LET({{Key, Bucket}, Value, IndexSpec, MetaData}, 
         {{gen_key(), <<"LoadBucket">>}, gen_val(), [{add, Cat, gen_index_value()} || Cat <- Categories ], []},
        case Tag of
            ?STD_TAG -> [Pid, Bucket, Key, Value, Value, IndexSpec, Tag, MetaData];
            ?RIAK_TAG ->
                Obj = testutil:riak_object(Bucket, Key, Value, MetaData), %% this has a side effect inserting a random nr
                [Pid, Bucket, Key, Value, Obj, IndexSpec, Tag, MetaData]
        end
         )).

updateload_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Value, _Obj, _, _, _]) ->
    Pid == Leveled.

updateload_adapt(#{leveled := Leveled}, [_, Bucket, Key, Value, Obj, Spec, Tag, MetaData]) ->
    [ Leveled, Bucket, Key, Value, Obj, Spec, Tag, MetaData ].

%% @doc put - The actual operation
updateload(Pid, Bucket, Key, Value, Obj, Spec, Tag, MetaData) ->
    Values =
        case Tag of
                 ?STD_TAG -> multiply(100, Value); 
                 ?RIAK_TAG ->
                     lists:map(fun(V) -> testutil:riak_object(Bucket, Key, V, MetaData) end,
                                 multiply(100, Value))
        end ++ [Obj],
    lists:map(fun(V) -> leveled_bookie:book_put(Pid, Bucket, Key, V, Spec, Tag) end, Values).

multiply(1, _Value) ->
    [];
multiply(N, Value) when N > 1 ->
    <<Byte:8, Rest/binary>> = Value,
    NewValue = <<Rest/binary, Byte:8>>,
    [NewValue | multiply(N-1, NewValue)].

updateload_next(#{model := Model} = S, _V, [_Pid, Bucket, Key, _Value, Obj, Spec, _Tag, _MetaData]) ->
    ?CMD_VALID(S, put,
               begin
                   NewSpec = 
                       case orddict:find({Bucket, Key}, Model) of
                           error -> merge_index_spec([], Spec);
                           {ok, {_, OldSpec}} ->
                               merge_index_spec(OldSpec, Spec)
                       end,
                   S#{model => orddict:store({Bucket, Key}, {Obj, NewSpec}, Model)}
               end,
               S).

updateload_post(S, [_, _, _, _, _, _, _, _], Results) ->
    lists:all(fun(Res) -> ?CMD_VALID(S, put, lists:member(Res, [ok, pause]), Res == {unsupported_message, put}) end, Results).

updateload_features(#{previous_keys := PK} = S, [_Pid, Bucket, Key, _Value, _Obj, _, Tag, _], _Res) ->
    ?CMD_VALID(S, put,
               case 
                   lists:member({Key, Bucket}, PK) of
                   true ->
                       [{updateload, update, Tag}];
                   false ->
                       [{updateload, insert, Tag}]
               end,
               [{updateload, unsupported}]).


%% --- Operation: put ---
put_pre(S) ->
    is_leveled_open(S).

put_args(#{leveled := Pid, previous_keys := PK, tag := Tag}) ->
    ?LET(Categories, gen_categories(Tag),
    ?LET({{Key, Bucket}, Value, IndexSpec, MetaData}, 
         {gen_key_in_bucket(PK), gen_val(), [{add, Cat, gen_index_value()} || Cat <- Categories ], []},
         case Tag of
             ?STD_TAG -> [Pid, Bucket, Key, Value, IndexSpec, elements([none, Tag])]; 
             ?RIAK_TAG ->
                 Obj = testutil:riak_object(Bucket, Key, Value, MetaData),
                 [Pid, Bucket, Key, Obj, IndexSpec, Tag]  
         end)).

put_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Value, _, _]) ->
    Pid == Leveled.

put_adapt(#{leveled := Leveled}, [_, Bucket, Key, Value, Spec, Tag]) ->
    [ Leveled, Bucket, Key, Value, Spec, Tag ].

%% @doc put - The actual operation
put(Pid, Bucket, Key, Value, Spec, none) ->
    leveled_bookie:book_put(Pid, Bucket, Key, Value, Spec);
put(Pid, Bucket, Key, Value, Spec, Tag) ->
    leveled_bookie:book_put(Pid, Bucket, Key, Value, Spec, Tag).

put_next(#{model := Model, previous_keys := PK} = S, _Value, [_Pid, Bucket, Key, Value, Spec, _Tag]) ->
    ?CMD_VALID(S, put,
               begin
                   NewSpec = 
                       case orddict:find({Bucket, Key}, Model) of
                           error -> merge_index_spec([], Spec);
                           {ok, {_, OldSpec}} ->
                               merge_index_spec(OldSpec, Spec)
                       end,
                   S#{model => orddict:store({Bucket, Key}, {Value, NewSpec}, Model),
                      previous_keys => PK ++ [{Key, Bucket}]}
               end,
               S).

put_post(S, [_, _, _, _, _, _], Res) ->
    ?CMD_VALID(S, put, lists:member(Res, [ok, pause]), eq(Res, {unsupported_message, put})).

put_features(#{previous_keys := PK} = S, [_Pid, Bucket, Key, _Value, _, Tag], _Res) ->
    ?CMD_VALID(S, put,
               case 
                   lists:member({Key, Bucket}, PK) of
                   true ->
                       [{put, update, Tag}];
                   false ->
                       [{put, insert, Tag}]
               end,
               [{put, unsupported}]).

merge_index_spec(Spec, []) ->
    Spec;
merge_index_spec(Spec, [{add, Cat, Idx} | Rest]) -> 
    merge_index_spec(lists:delete({Cat, Idx}, Spec) ++ [{Cat, Idx}], Rest);
merge_index_spec(Spec, [{remove, Cat, Idx} | Rest]) -> 
    merge_index_spec(lists:delete({Cat, Idx}, Spec), Rest).


%% --- Operation: get ---
get_pre(S) ->
    is_leveled_open(S).

get_args(#{leveled := Pid, previous_keys := PK, tag := Tag}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key, case Tag of ?STD_TAG -> default(none, Tag); _ -> Tag end]).

%% @doc get - The actual operation
get(Pid, Bucket, Key, none) ->
    leveled_bookie:book_get(Pid, Bucket, Key);
get(Pid, Bucket, Key, Tag) ->
    leveled_bookie:book_get(Pid, Bucket, Key, Tag).

get_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Tag]) ->
    Pid == Leveled.

get_adapt(#{leveled := Leveled}, [_, Bucket, Key, Tag]) ->    
    [Leveled, Bucket, Key, Tag].

get_post(#{model := Model} = S, [_Pid, Bucket, Key, Tag], Res) ->
    ?CMD_VALID(S, get,
               case Res of
                   {ok, _} ->
                       {ok, {Value, _}} = orddict:find({Bucket, Key}, Model),
                       eq(Res, {ok, Value});
                   not_found ->
                       %% Weird to be able to supply a tag, but must be STD_TAG...
                       Tag =/= ?STD_TAG orelse orddict:find({Bucket, Key}, Model) == error
               end,
               eq(Res, {unsupported_message, get})).

get_features(#{deleted_keys := DK, previous_keys := PK}, [_Pid, Bucket, Key, _Tag], Res) ->
    case Res of
        not_found ->
            [{get, not_found, deleted} || lists:member({Key, Bucket}, DK)] ++ 
          [{get, not_found, not_inserted} || not lists:member({Key, Bucket}, PK)];
        {ok, B} when is_binary(B) ->
            [{get, found}];
        {unsupported_message, _} ->
            [{get, unsupported}]
    end.

%% --- Operation: mput ---
mput_pre(S) ->
    is_leveled_open(S).

%% @doc mput_args - Argument generator
%% Specification says: duplicated should be removed
%% "%% The list should be de-duplicated before it is passed to the bookie."
%% Wether this means that keys should be unique or even Action and values is unclear.
%% Slack discussion:
%% `[{add, B1, K1, SK1}, {add, B1, K1, SK2}]` should be fine (same bucket and key, different subkey)
%%
%% Really weird to have to specify a value in case of a remove action
mput_args(#{leveled := Pid, previous_keys := PK}) ->
    ?LET(Objs, list({gen_key_in_bucket(PK), nat()}),
         [Pid, [ {weighted_default({5, add}, {1, remove}), Bucket, Key, SubKey, gen_val()} || {{Key, Bucket}, SubKey} <- Objs ]]).


mput_pre(#{leveled := Leveled}, [Pid, ObjSpecs]) ->
    Pid == Leveled andalso no_key_dups(ObjSpecs) == ObjSpecs.

mput_adapt(#{leveled := Leveled}, [_, ObjSpecs]) ->
    [ Leveled, no_key_dups(ObjSpecs) ].

mput(Pid, ObjSpecs) ->
    leveled_bookie:book_mput(Pid, ObjSpecs).

mput_next(S, _, [_Pid, ObjSpecs]) ->
    ?CMD_VALID(S, mput,
               lists:foldl(fun({add, Bucket, Key, _SubKey, Value}, #{model := Model, previous_keys := PK} = Acc) ->
                                   Acc#{model => orddict:store({Bucket, Key}, {Value, []}, Model),
                                        previous_keys => PK ++ [{Key, Bucket}]};
                              ({remove, Bucket, Key, _SubKey, _Value}, #{model := Model} = Acc) ->
                                   Acc#{model => orddict:erase({Bucket, Key}, Model)}
                           end, S, ObjSpecs),
               S).

mput_post(S, [_, _], Res) ->
    ?CMD_VALID(S, mput, lists:member(Res, [ok, pause]), eq(Res, {unsupported_message, mput})).

mput_features(S, [_Pid, ObjSpecs], _Res) ->
    ?CMD_VALID(S, mput,
               {mput, [ element(1, ObjSpec) || ObjSpec <- ObjSpecs ]},
               [{mput, unsupported}]).

%% --- Operation: head ---
head_pre(S) ->
    is_leveled_open(S).

head_args(#{leveled := Pid, previous_keys := PK, tag := Tag}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key, Tag]).

head_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Tag]) ->
    Pid == Leveled.

head_adapt(#{leveled := Leveled}, [_, Bucket, Key, Tag]) ->    
    [Leveled, Bucket, Key, Tag].

head(Pid, Bucket, Key, none) ->
    leveled_bookie:book_head(Pid, Bucket, Key);
head(Pid, Bucket, Key, Tag) ->
    leveled_bookie:book_head(Pid, Bucket, Key, Tag).

head_post(#{model := Model} = S, [_Pid, Bucket, Key, Tag], Res) ->
    ?CMD_VALID(S, head,
               case Res of
                   {ok, _MetaData} ->
                       orddict:find({Bucket, Key}, Model) =/= error;
                   not_found ->
                       %% Weird to be able to supply a tag, but must be STD_TAG...
                       implies(lists:member(maps:get(start_opts, S), [{head_only, with_lookup}]), 
                               lists:member(Tag, [?STD_TAG, none, ?HEAD_TAG])) orelse 
                       orddict:find({Bucket, Key}, Model) == error;
                   {unsupported_message, head} ->
                       Tag =/= ?HEAD_TAG
               end,
               eq(Res, {unsupported_message, head})).

head_features(#{deleted_keys := DK, previous_keys := PK}, [_Pid, Bucket, Key, _Tag], Res) ->
    case Res of
        not_found ->
            [{head, not_found, deleted} || lists:member({Key, Bucket}, DK)] ++ 
          [{head, not_found, not_inserted} || not lists:member({Key, Bucket}, PK)];
        {ok, {_, _, _}} ->  %% Metadata
            [{head, found}];
        {ok, Bin} when is_binary(Bin) ->
            [{head, found_riak_object}];
        {unsupported_message, _} ->
            [{head, unsupported}]
    end.


%% --- Operation: delete ---
delete_pre(S) ->
    is_leveled_open(S).

delete_args(#{leveled := Pid, previous_keys := PK, tag := Tag}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key, [], Tag]).

delete_pre(#{leveled := Leveled, model := Model}, [Pid, Bucket, Key, Spec, _Tag]) ->
    Pid == Leveled andalso
        case orddict:find({Bucket, Key}, Model) of
            error -> true;
            {ok, {_, OldSpec}} ->
                Spec == OldSpec
        end.

delete_adapt(#{leveled := Leveled, model := Model}, [_, Bucket, Key, Spec, Tag]) ->
    NewSpec = 
        case orddict:find({Bucket, Key}, Model) of
            error -> Spec;
            {ok, {_, OldSpec}} ->
                Spec == OldSpec
        end,
    [ Leveled, Bucket, Key, NewSpec, Tag ].

delete(Pid, Bucket, Key, Spec, ?STD_TAG) ->
    leveled_bookie:book_delete(Pid, Bucket, Key, Spec);
delete(Pid, Bucket, Key, Spec, Tag) ->
    leveled_bookie:book_put(Pid, Bucket, Key, delete, Spec, Tag).

delete_next(#{model := Model, deleted_keys := DK} = S, _Value, [_Pid, Bucket, Key, _, _]) ->
    ?CMD_VALID(S, delete,
               S#{model => orddict:erase({Bucket, Key}, Model), 
                  deleted_keys => DK ++ [{Key, Bucket} || orddict:is_key({Key, Bucket}, Model)]},
               S).

delete_post(S, [_Pid, _Bucket, _Key, _, _], Res) ->
    ?CMD_VALID(S, delete,
               lists:member(Res, [ok, pause]),
               case Res of
                   {unsupported_message, _} -> true;
                   _ -> Res
               end).

delete_features(#{previous_keys := PK} = S, [_Pid, Bucket, Key, _, _], _Res) ->
    ?CMD_VALID(S, delete,
               case lists:member({Key, Bucket}, PK) of
                   true ->
                       [{delete, existing}];
                   false ->
                       [{delete, none_existing}]
               end,
               [{delete, unsupported}]).

%% --- Operation: is_empty ---
is_empty_pre(S) ->
    is_leveled_open(S).

is_empty_args(#{leveled := Pid, tag := Tag}) ->
    [Pid, Tag].


is_empty_pre(#{leveled := Leveled}, [Pid, _]) ->
    Pid == Leveled.

is_empty_adapt(#{leveled := Leveled}, [_, Tag]) ->
    [Leveled, Tag].

%% @doc is_empty - The actual operation
is_empty(Pid, Tag) ->
    leveled_bookie:book_isempty(Pid, Tag).

is_empty_post(#{model := Model}, [_Pid, _Tag], Res) ->
    Size = orddict:size(Model),
    case Res of
        true -> eq(0, Size);
        false when Size == 0 -> expected_empty;
        false when Size > 0  -> true
    end.

is_empty_features(_S, [_Pid, _], Res) ->
    [{empty, Res}].

%% --- Operation: drop ---
drop_pre(S) ->
    is_leveled_open(S).

drop_args(#{leveled := Pid, dir := Dir} = S) ->
    ?LET([Tag, _, Name], init_backend_args(S),
         [Pid, Tag, [{root_path, Dir} | gen_opts()], Name]).

drop_pre(#{leveled := Leveled} = S, [Pid, Tag, Opts, Name]) ->
    Pid == Leveled andalso init_backend_pre(S, [Tag, Opts, Name]).

drop_adapt(#{leveled := Leveled} = S, [_Pid, Tag, Opts, Name]) ->
    [Leveled | init_backend_adapt(S, [Tag, Opts, Name])].
    
%% @doc drop - The actual operation
%% Remove fles from disk (directory structure may remain) and start a new clean database
drop(Pid, Tag, Opts, Name) ->
    Mon = erlang:monitor(process, Pid),
    ok = leveled_bookie:book_destroy(Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            init_backend(Tag, Opts, Name)
    after 5000 ->
            {still_alive, Pid, Name}
    end.

drop_next(S, Value, [Pid, Tag, Opts, Name]) ->
    S1 = stop_next(S, Value, [Pid]),
    init_backend_next(S1#{model => orddict:new()}, 
                      Value, [Tag, Opts, Name]).

drop_post(_S, [_Pid, _Tag, _Opts, _], Res) ->
    case is_pid(Res) of
        true  -> true;
        false -> Res
    end.

drop_features(#{model := Model}, [_Pid, _Tag, _Opts, _], _Res) ->
    Size = orddict:size(Model),
    [{drop, empty} || Size == 0 ] ++ 
        [{drop, small} || Size > 0 andalso Size < 20 ] ++
        [{drop, medium} || Size >= 20 andalso Size < 1000 ] ++
        [{drop, large} || Size >= 1000 ].



%% --- Operation: kill ---
%% Test that killing the root Pid of leveled has the same effect as closing it nicely
%% that means, we don't loose data! Not even when parallel successful puts are going on.
kill_pre(S) ->
    is_leveled_open(S).

kill_args(#{leveled := Pid}) ->
    [Pid].

kill_pre(#{leveled := Leveled}, [Pid]) ->
    Pid == Leveled.

kill_adapt(#{leveled := Leveled}, [_]) ->
    [ Leveled ].

kill(Pid) ->
    exit(Pid, kill),
    timer:sleep(1).

kill_next(S, Value, [Pid]) ->
    stop_next(S, Value, [Pid]).




%% --- Operation: compacthappened ---

compacthappened(DataDir) ->
    PostCompact = filename:join(DataDir, "journal/journal_files/post_compact"),
    case filelib:is_dir(PostCompact) of
        true ->
            {ok, Files} = file:list_dir(PostCompact),
            Files;
        false ->
            []
    end.

ledgerpersisted(DataDir) ->
    LedgerPath = filename:join(DataDir, "ledger/ledger_files"),
    case filelib:is_dir(LedgerPath) of
        true ->
            {ok, Files} = file:list_dir(LedgerPath),
            Files;
        false ->
            []
    end.

journalwritten(DataDir) ->
    JournalPath = filename:join(DataDir, "journal/journal_files"),
    case filelib:is_dir(JournalPath) of
        true ->
            {ok, Files} = file:list_dir(JournalPath),
            Files;
        false ->
            []
    end.


%% --- Operation: compact journal ---

compact_pre(S) ->
    is_leveled_open(S).

compact_args(#{leveled := Pid}) ->
    [Pid, nat()].

compact_pre(#{leveled := Leveled}, [Pid, _TS]) ->
    Pid == Leveled.

compact_adapt(#{leveled := Leveled}, [_Pid, TS]) ->
    [ Leveled, TS ].

compact(Pid, TS) ->
    leveled_bookie:book_compactjournal(Pid, TS).

compact_next(S, R, [_Pid, _TS]) ->
    case {R, maps:get(previous_compact, S, undefined)} of
        {ok, undefined} ->
            S#{previous_compact => true};
        _ ->
            S
    end.

compact_post(S, [_Pid, _TS], Res) ->
    case Res of
        ok ->
            true;
        busy ->
            true == maps:get(previous_compact, S, undefined)
    end.

compact_features(S, [_Pid, _TS], _Res) ->
    case maps:get(previous_compact, S, undefined) of
        undefined ->
            [{compact, fresh}];
        _ ->
            [{compact, repeat}]
    end.


%% Testing fold:
%% Note async and sync mode!
%% see https://github.com/martinsumner/riak_kv/blob/mas-2.2.5-tictactaae/src/riak_kv_leveled_backend.erl#L238-L419

%% --- Operation: index folding ---
indexfold_pre(S) ->
    is_leveled_open(S).

indexfold_args(#{leveled := Pid, counter := Counter, previous_keys := PK}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, default(Bucket, {Bucket, Key}), gen_foldacc(3), 
          ?LET({[N], M}, {gen_index_value(), choose(0,2)}, {gen_category(), [N], [N+M]}), 
          {bool(),
           oneof([undefined, gen_index_value()])},
          Counter  %% add a unique counter
         ]).

indexfold_pre(#{leveled := Leveled}, [Pid, _Constraint, _FoldAccT, _Range, _TermHandling, _Counter]) ->
    %% Make sure we operate on an existing Pid when shrinking
    %% Check start options validity as well?
    Pid == Leveled.
    
indexfold_adapt(#{leveled := Leveled}, [_, Constraint, FoldAccT, Range, TermHandling, Counter]) ->
    %% Keep the counter!
    [Leveled, Constraint, FoldAccT, Range, TermHandling, Counter].

indexfold(Pid, Constraint, FoldAccT, Range, {_, undefined} = TermHandling, _Counter) ->
    {async, Folder} = leveled_bookie:book_indexfold(Pid, Constraint, FoldAccT, Range, TermHandling),
    Folder;
indexfold(Pid, Constraint, FoldAccT, Range, {ReturnTerms, RegExp}, _Counter) ->
    {ok, RE} = re:compile(RegExp),
    {async, Folder} = leveled_bookie:book_indexfold(Pid, Constraint, FoldAccT, Range, {ReturnTerms, RE}),
    Folder.

indexfold_next(#{folders := Folders} = S, SymFolder, 
               [_, Constraint, {Fun, Acc}, {Category, From, To}, {ReturnTerms, RegExp}, Counter]) ->
    ConstraintFun =
        fun(B, K, Bool) ->
                case Constraint of
                    {B, KStart} -> not Bool orelse K >= KStart;
                    B -> true;
                    _ -> false
                end
        end,
    S#{folders => 
           Folders ++ 
           [#{counter => Counter,
              type => indexfold,
              folder => SymFolder,
              reusable => true,
              result => fun(Model) ->
                                Select = 
                                    lists:sort(
                                      orddict:fold(fun({B, K}, {_V, Spec}, A) ->
                                                           [ {B, {Idx, K}}
                                                             || {Cat, Idx} <- Spec,
                                                                Idx >= From, Idx =< To,
                                                                Cat == Category,
                                                                ConstraintFun(B, K, Idx == From),
                                                                RegExp == undefined orelse string:find(Idx, RegExp) =/= nomatch
                                                           ] ++ A
                                                   end, [], Model)),
                                lists:foldl(fun({B, NK}, A) when ReturnTerms ->
                                                    Fun(B, NK, A);
                                               ({B, {_, NK}}, A) ->
                                                    Fun(B, NK, A)
                                            end, Acc, Select)
                        end 
             }],
       counter =>  Counter + 1}.

indexfold_post(_S, _, Res) ->
    is_function(Res).

indexfold_features(_S, [_Pid, Constraint, FoldAccT, _Range, {ReturnTerms, _}, _Counter], _Res) ->
    [{foldAccT, FoldAccT}] ++  %% This will be extracted for printing later
        [{index_fold, bucket} || not is_tuple(Constraint) ] ++
        [{index_fold, bucket_and_primary_key} || is_tuple(Constraint)] ++
        [{index_fold, return_terms, ReturnTerms} ].




%% --- Operation: keylist folding ---
%% slack discussion: "`book_keylist` only passes `Bucket` and `Key` into the accumulator, ignoring SubKey - 
%% so I don't think this can be used in head_only mode to return results that make sense"
%%
%% There are also keylist functions that take a specific bucket and range into account. Not considered yet.
keylistfold_pre(S) ->
    is_leveled_open(S).

keylistfold_args(#{leveled := Pid, counter := Counter, tag := Tag}) ->
    [Pid, Tag, gen_foldacc(3),
     Counter  %% add a unique counter
    ].

keylistfold_pre(#{leveled := Leveled}, [Pid, _Tag, _FoldAccT, _Counter]) ->
    %% Make sure we operate on an existing Pid when shrinking
    %% Check start options validity as well?
    Pid == Leveled.
    
keylistfold_adapt(#{leveled := Leveled}, [_, Tag, FoldAccT, Counter]) ->
    %% Keep the counter!
    [Leveled, Tag, FoldAccT, Counter].

keylistfold(Pid, Tag, FoldAccT, _Counter) ->
    {async, Folder} = leveled_bookie:book_keylist(Pid, Tag, FoldAccT),
    Folder.

keylistfold_next(#{folders := Folders, model := Model} = S, SymFolder, 
               [_, _Tag, {Fun, Acc}, Counter]) ->
    S#{folders => 
           Folders ++ 
           [#{counter => Counter,
              type => keylist,
              folder => SymFolder,
              reusable => false,
              result => fun(_) -> orddict:fold(fun({B, K}, _V, A) -> Fun(B, K, A) end, Acc, Model) end
             }],
       counter => Counter + 1}.

keylistfold_post(_S, _, Res) ->
    is_function(Res).

keylistfold_features(_S, [_Pid, _Tag, FoldAccT, _Counter], _Res) ->
    [{foldAccT, FoldAccT}]. %% This will be extracted for printing later


%% --- Operation: bucketlistfold ---
bucketlistfold_pre(S) ->
   is_leveled_open(S).

bucketlistfold_args(#{leveled := Pid, counter := Counter, tag := Tag}) ->
    [Pid, Tag, gen_foldacc(2), elements([first, all]), Counter].

bucketlistfold_pre(#{leveled := Leveled}, [Pid, _Tag, _FoldAccT, _Constraints, _]) ->
     Pid == Leveled.

bucketlistfold_adapt(#{leveled := Leveled}, [_Pid, Tag, FoldAccT, Constraints, Counter]) ->
    [Leveled, Tag, FoldAccT, Constraints, Counter].

bucketlistfold(Pid, Tag, FoldAccT, Constraints, _) ->
    {async, Folder} = leveled_bookie:book_bucketlist(Pid, Tag, FoldAccT, Constraints),
    Folder.

bucketlistfold_next(#{folders := Folders} = S, SymFolder, 
                    [_, _, {Fun, Acc}, Constraints, Counter]) ->
    S#{folders => 
           Folders ++ 
           [#{counter => Counter,
              type => bucketlist,
              folder => SymFolder, 
              reusable => true,
              result => fun(Model) ->
                                Bs = orddict:fold(fun({B, _K}, _V, A) -> A ++ [B || not lists:member(B, A)] end, [], Model),
                                case {Constraints, Bs} of
                                    {all, _} ->
                                        lists:foldl(fun(B, A) -> Fun(B, A) end, Acc, Bs);
                                    {first, []} ->
                                        Acc;
                                    {first, [First|_]} ->
                                        lists:foldl(fun(B, A) -> Fun(B, A) end, Acc, [First])
                                end
                        end        
             }],
       counter => Counter + 1}.

bucketlistfold_post(_S, [_Pid, _Tag, _FoldAccT, _Constraints, _], Res) ->
    is_function(Res).

bucketlistfold_features(_S, [_Pid, _Tag, FoldAccT, _Constraints, _], _Res) ->
    [ {foldAccT, FoldAccT} ].

%% --- Operation: objectfold ---
objectfold_pre(S) ->
    is_leveled_open(S).

objectfold_args(#{leveled := Pid, counter := Counter, tag := Tag}) ->
    [Pid, Tag, gen_foldacc(4), bool(), Counter].

objectfold_pre(#{leveled := Leveled}, [Pid, _Tag, _FoldAccT, _Snapshot, _Counter]) ->
    Leveled == Pid.

objectfold_adapt(#{leveled := Leveled}, [_Pid, Tag, FoldAccT, Snapshot, Counter]) ->
    [Leveled, Tag, FoldAccT, Snapshot, Counter].

objectfold(Pid, Tag, FoldAccT, Snapshot, _Counter) ->
    {async, Folder} = leveled_bookie:book_objectfold(Pid, Tag, FoldAccT, Snapshot),
    Folder.

objectfold_next(#{folders := Folders, model := Model} = S, SymFolder, 
                [_Pid, _Tag, {Fun, Acc}, Snapshot, Counter]) ->
    S#{folders => 
           Folders ++ 
           [#{counter => Counter,
              type => objectfold,
              folder => SymFolder, 
              reusable => not Snapshot,
              result => fun(M) ->
                                OnModel =  
                                    case Snapshot of
                                        true -> Model;
                                        false -> M
                                    end,
                                Objs = orddict:fold(fun({B, K}, {V, _}, A) -> [{B, K, V} | A] end, [], OnModel),
                                lists:foldr(fun({B, K, V}, A) -> Fun(B, K, V, A) end, Acc, Objs)
                        end
             }],
       counter => Counter + 1}.

objectfold_post(_S, [_Pid, _Tag, _FoldAccT, _Snapshot, _Counter], Res) ->
    is_function(Res).

objectfold_features(_S, [_Pid, _Tag, FoldAccT, _Snapshot, _Counter], _Res) ->
    [{foldAccT, FoldAccT}]. %% This will be extracted for printing later




%% --- Operation: fold_run ---
fold_run_pre(S) ->
    maps:get(folders, S, []) =/= [].

fold_run_args(#{folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

fold_run_pre(#{folders := Folders}, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    get_foldobj(Folders, Counter) =/= undefined.

fold_run(_, Folder) ->
    catch Folder().

fold_run_next(#{folders := Folders} = S, _Value, [Counter, _Folder]) ->
    %% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
    FoldObj = get_foldobj(Folders, Counter),
    case FoldObj of
        #{reusable := false} ->
            UsedFolders = maps:get(used_folders, S, []),
            S#{folders => Folders -- [FoldObj],
               used_folders => UsedFolders ++ [FoldObj]};
        _ -> 
            S
    end.
    
fold_run_post(#{folders := Folders, leveled := Leveled, model := Model}, [Count, _], Res) ->
    case Leveled of 
        undefined ->
            is_exit(Res);
        _ ->
            #{result := ResFun} = get_foldobj(Folders, Count),
            eq(Res, ResFun(Model))
    end.

fold_run_features(#{folders := Folders, leveled := Leveled}, [Count, _Folder], Res) ->
    #{type := Type} = get_foldobj(Folders, Count),
    [ {fold_run, Type} || Leveled =/= undefined ] ++
        [ fold_run_on_stopped_leveled || Leveled == undefined ] ++
        [ {fold_run, found_list, length(Res)}|| is_list(Res) ] ++
         [ {fold_run, found_integer}|| is_integer(Res) ].

               
%% --- Operation: fold_run on already used folder ---
%% A fold that has already ran to completion should results in an exception when re-used.
%% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
noreuse_fold_pre(S) ->
    maps:get(used_folders, S, []) =/= [].

noreuse_fold_args(#{used_folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

noreuse_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    lists:member(Counter, 
                 [ maps:get(counter, Used) || Used <- maps:get(used_folders, S, []) ]).

noreuse_fold(_, Folder) ->
    catch Folder().

noreuse_fold_post(_S, [_, _], Res) ->
    is_exit(Res).

noreuse_fold_features(_, [_, _], _) ->
    [ reuse_fold ].


%% --- Operation: fold_run on folder that survived a crash ---
%% A fold that has already ran to completion should results in an exception when re-used.
stop_fold_pre(S) ->
    maps:get(stop_folders, S, []) =/= [].

stop_fold_args(#{stop_folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

stop_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    lists:member(Counter, 
                 [ maps:get(counter, Used) || Used <- maps:get(stop_folders, S, []) ]).

stop_fold(_, Folder) ->
    catch Folder().

stop_fold_post(_S, [_Counter, _], Res) ->
    is_exit(Res).

stop_fold_features(S, [_, _], _) ->
    [ case maps:get(leveled, S) of
          undefined -> 
              stop_fold_when_closed;
          _ ->
              stop_fold_when_open
      end ].


weight(#{previous_keys := []}, get) ->
    1;
weight(#{previous_keys := []}, delete) ->
    1;
weight(S, C) when C == get;
                  C == put;
                  C == delete;
                  C == updateload ->
    ?CMD_VALID(S, put, 10, 1);
weight(_S, stop) ->
    1;
weight(_, _) ->
    1.


is_valid_cmd(S, put) ->
    not in_head_only_mode(S);
is_valid_cmd(S, delete) ->
    is_valid_cmd(S, put);
is_valid_cmd(S, get) ->
    not in_head_only_mode(S);
is_valid_cmd(S, head) ->
    not lists:member({head_only, no_lookup}, maps:get(start_opts, S, []));
is_valid_cmd(S, mput) ->
    in_head_only_mode(S).



%% @doc check that the implementation of leveled is equivalent to a
%% sorted dict at least
-spec prop_db() -> eqc:property().
prop_db() ->
    Dir = "./leveled_data",
    eqc:dont_print_counterexample( 
    ?LET(Shrinking, parameter(shrinking, false),
    ?FORALL({Kind, Cmds}, oneof([{seq, more_commands(20, commands(?MODULE))}, 
                                    {par, more_commands(2, parallel_commands(?MODULE))}
                                ]),
    begin
        delete_level_data(Dir),
        ?IMPLIES(empty_dir(Dir),
        ?ALWAYS(if Shrinking -> 10; true -> 1 end,
        begin
            Procs = erlang:processes(),
            StartTime = erlang:system_time(millisecond),

            RunResult = execute(Kind, Cmds, [{dir, Dir}]),
            %% Do not extract the 'state' from this tuple, since parallel commands
            %% miss the notion of final state.
            CallFeatures = [ Feature || Feature <- call_features(history(RunResult)), 
                                        not is_foldaccT(Feature),
                                        not (is_tuple(Feature) andalso element(1, Feature) == start_options) 
                           ],
            StartOptionFeatures = [ lists:keydelete(root_path, 1, Feature) || {start_options, Feature} <- call_features(history(RunResult)) ],

            timer:sleep(1000),
            CompactionFiles = compacthappened(Dir),
            LedgerFiles = ledgerpersisted(Dir),
            JournalFiles = journalwritten(Dir),
            % io:format("File counts: Compacted ~w Journal ~w Ledger ~w~n", [length(CompactionFiles), length(LedgerFiles), length(JournalFiles)]),
            


            case whereis(maps:get(sut, initial_state())) of
                undefined -> 
                    % io:format("Init state undefined - deleting~n"),
                    delete_level_data(Dir);
                Pid when is_pid(Pid) ->
                    % io:format("Init state defined - destroying~n"),
                    leveled_bookie:book_destroy(Pid)
            end,

            Wait = wait_for_procs(Procs, 12000),
                % Wait at least for delete_pending timeout + 1s
                % However, even then can hit the issue of the Quviq license 
                % call spawning processes
            lists:foreach(fun(P) ->
                                io:format("~nProcess info for ~w:~n~w~n",
                                            [P, process_info(P)]),
                                io:format("Stacktrace:~n ~w~n",
                                            [process_info(P, current_stacktrace)]),
                                io:format("Monitored by:~n ~w~n",
                                            [process_info(P, monitored_by)]),
                                io:format("~n")
                            end,
                            Wait),
            RunTime = erlang:system_time(millisecond) - StartTime,

            %% Since in parallel commands we don't have access to the state, we retrieve functions
            %% from the features
            FoldAccTs = [ FoldAccT || Entry <- history(RunResult),
                                      {foldAccT, FoldAccT} <- eqc_statem:history_features(Entry)],

            pretty_commands(?MODULE, Cmds, RunResult,
            measure(time_per_test, RunTime,
            aggregate(command_names(Cmds),
            collect(Kind,
            measure(compaction_files, length(CompactionFiles),
            measure(ledger_files, length(LedgerFiles),
            measure(journal_files, length(JournalFiles),
            aggregate(with_title('Features'), CallFeatures,
            aggregate(with_title('Start Options'), StartOptionFeatures,
            features(CallFeatures,
                      conjunction([{result, 
                                    ?WHENFAIL([ begin
                                                    eqc:format("~p with acc ~p:\n~s\n", [F, Acc,
                                                                                         show_function(F)])
                                                end || {F, Acc} <- FoldAccTs ],
                                              result(RunResult) == ok)},
                                   {data_cleanup, 
                                    ?WHENFAIL(eqc:format("~s\n", [os:cmd("ls -Rl " ++ Dir)]),
                                              empty_dir(Dir))},
                                   {pid_cleanup, equals(Wait, [])}])))))))))))
        end))
    end))).

history({H, _, _}) -> H.
result({_, _, Res}) -> Res.

execute(seq, Cmds, Env) ->
    run_commands(Cmds, Env);
execute(par, Cmds, Env) ->
    run_parallel_commands(Cmds, Env).

is_exit({'EXIT', _}) ->
    true;
is_exit(Other) ->
    {expected_exit, Other}.

is_foldaccT({foldAccT, _}) ->
    true;
is_foldaccT(_) ->
    false.

show_function(F) ->
    case proplists:get_value(module, erlang:fun_info(F)) of
        eqc_fun ->
            eqc_fun:show_function(F);
        _ ->
            proplists:get_value(name, erlang:fun_info(F))
    end.


%% slack discussion:
%% `max_journalsize` should be at least 2048 + byte_size(smallest_object) + byte_size(smallest_object's key) + overhead (which is a few bytes per K/V pair).
gen_opts() ->
    options([%% {head_only, elements([false, no_lookup, with_lookup])} we don't test head_only mode
              {compression_method, elements([native, lz4])}
            , {compression_point, elements([on_compact, on_receipt])}
            %% , {max_journalsize, ?LET(N, nat(), 2048 + 1000 + 32 + 16 + 16 + N)}
            %% , {cache_size, oneof([nat(), 2048, 2060, 5000])}
            ]).

options(GenList) ->
    ?LET(Bools, vector(length(GenList), bool()),
         [ Opt || {Opt, true} <- lists:zip(GenList, Bools)]).


gen_key() ->
    binary(16).

%% Cannot be atoms!
%% key() type specified: should be binary().
gen_bucket() -> 
    elements([<<"bucket1">>, <<"bucket2">>, <<"bucket3">>]).

gen_val() ->
    noshrink(binary(256)).

gen_categories(?RIAK_TAG) ->
    sublist(categories());
gen_categories(_) ->
    %% for ?STD_TAG this seems to make little sense
    [].


categories() ->
    [dep, lib].

gen_category() ->
    elements(categories()).

gen_index_value() ->
    %% Carefully selected to have one out-layer and several border cases.
    [elements("arts")].

gen_key_in_bucket([]) ->
    {gen_key(), gen_bucket()};
gen_key_in_bucket(Previous) ->
    ?LET({K, B}, elements(Previous),
         frequency([{1, gen_key_in_bucket([])},
                    {1, {K, gen_bucket()}},
                    {2, {K, B}}])).

gen_foldacc(2) ->
    ?SHRINK(oneof([{eqc_fun:function2(int()), int()},
                   {eqc_fun:function2(list(int())), list(int())}]),
            [fold_buckets()]);
gen_foldacc(3) ->
    ?SHRINK(oneof([{eqc_fun:function3(int()), int()},
                   {eqc_fun:function3(list(int())), list(int())}]),
            [fold_collect()]);
gen_foldacc(4) ->
    ?SHRINK(oneof([{eqc_fun:function4(int()), int()},
                   {eqc_fun:function4(list(int())), list(int())}]),
            [fold_objects()]).



fold_buckets() ->
    {fun(B, Acc) -> [B | Acc] end, []}.
             
fold_collect() ->
    {fun(X, Y, Acc) -> [{X, Y} | Acc] end, []}.

fold_objects() ->
    {fun(X, Y, Z, Acc) -> [{X, Y, Z} | Acc] end, []}.

%% This makes system fall over
fold_collect_no_acc() ->
    fun(X, Y, Z) -> [{X, Y} | Z] end.

fold_count() ->
    {fun(_X, _Y, Z) -> Z + 1 end, 0}.

fold_keys() ->
    {fun(X, _Y, Z) -> [X | Z] end, []}.


empty_dir(Dir) ->
    case file:list_dir(Dir) of
        {error, enoent} -> true;
        {ok, Ds} ->
            lists:all(fun(D) -> empty_dir(filename:join(Dir, D)) end, Ds);
        _ ->
            false
    end.

get_foldobj([], _Counter) ->
    undefined;
get_foldobj([#{counter := Counter} = Map | _Rest], Counter) ->
    Map;
get_foldobj([_ | Rest], Counter) ->
    get_foldobj(Rest, Counter).
                

%% Helper for all those preconditions that just check that leveled Pid
%% is populated in state. (We cannot check with is_pid, since that's
%% symbolic in test case generation!).
is_leveled_open(S) ->
    maps:get(leveled, S, undefined) =/= undefined.

in_head_only_mode(S) ->
    proplists:get_value(head_only, maps:get(start_opts, S, []), false) =/= false.

wait_for_procs(Known, Timeout) ->
    case filtered_processes(Known) of
        [] -> [];
        _NonEmptyList ->
            if
                Timeout > 0 ->
                    timer:sleep(200),
                    wait_for_procs(Known, Timeout - 200);
                true ->
                    filtered_processes(Known)
            end
    end.

filtered_processes(Known) ->
    FilterFun = 
        fun(P) ->
            case process_info(P, current_stacktrace) of
                {current_stacktrace, ST} ->
                    case lists:keymember(eqc_licence, 1, ST) or 
                            lists:keymember(eqc_group_commands, 1, ST) of
                        true ->
                            % This is an eqc background process
                            false;
                        false ->
                            case process_info(P, dictionary) of
                                {dictionary, PD} ->
                                    HTTPCall = {'$initial_call',{httpc_handler,init,1}},
                                    DNSCall = {'$initial_call',{supervisor_bridge,inet_gethost_native,1}},
                                    case lists:member(HTTPCall, PD)
                                            or lists:member(DNSCall, PD) of
                                        true ->
                                            % This is the HTTP/DNS call for the licence
                                            false;
                                        _ ->
                                            case process_info(P, current_function) of
                                                {current_function,{inet_gethost_native,main_loop,1}} ->
                                                    % This is par tof the HTTP/DNS call
                                                    false;
                                                _ ->
                                                    true
                                            end
                                    end;
                                undefined ->
                                    %Eh ?
                                    true
                            end
                    end;
                undefined ->
                    % Eh?
                    true
            end
        end,
    lists:filter(FilterFun, erlang:processes() -- Known).

delete_level_data(Dir) ->
    os:cmd("rm -rf " ++ Dir).

%% Slack discussion:
%% `[{add, B1, K1, SK1}, {add, B1, K1, SK2}]` should be fine (same bucket and key, different subkey)
no_key_dups([]) ->
    [];
no_key_dups([{_Action, Bucket, Key, SubKey, _Value} = E | Es]) ->
    [E | no_key_dups([ {A, B, K, SK, V} || {A, B, K, SK, V} <- Es,
                                           {B, K, SK} =/= {Bucket, Key, SubKey}])].

-endif.