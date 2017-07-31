-module(testutil).

-include("../include/leveled.hrl").

-export([book_riakput/3,
            book_riakdelete/4,
            book_riakget/3,
            book_riakhead/3,
            riakload/2,
            reset_filestructure/0,
            reset_filestructure/1,
            check_bucket_stats/2,
            check_forlist/2,
            check_forlist/3,
            check_formissinglist/2,
            check_forobject/2,
            check_formissingobject/3,
            generate_testobject/0,
            generate_testobject/5,
            generate_compressibleobjects/2,
            generate_smallobjects/2,
            generate_objects/2,
            generate_objects/5,
            generate_objects/6,
            set_object/5,
            get_key/1,
            get_value/1,
            get_vclock/1,
            get_compressiblevalue/0,
            get_compressiblevalue_andinteger/0,
            get_randomindexes_generator/1,
            name_list/0,
            load_objects/5,
            load_objects/6,
            put_indexed_objects/3,
            put_altered_indexed_objects/3,
            put_altered_indexed_objects/4,
            check_indexed_objects/4,
            rotating_object_check/3,
            corrupt_journal/5,
            restore_file/2,
            restore_topending/2,
            find_journals/1,
            wait_for_compaction/1,
            foldkeysfun/3,
            foldkeysfun_returnbucket/3,
            sync_strategy/0]).

-define(RETURN_TERMS, {true, undefined}).
-define(SLOWOFFER_DELAY, 5).
-define(V1_VERS, 1).
-define(MAGIC, 53). % riak_kv -> riak_object
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).
-define(EMPTY_VTAG_BIN, <<"e">>).
-define(ROOT_PATH, "test").

%% =================================================
%% From riak_object

to_binary(v1, #r_object{contents=Contents, vclock=VClock}) ->
    new_v1(VClock, Contents).

new_v1(Vclock, Siblings) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
            VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta, value=Val}) ->
    ValBin = encode_maybe_binary(Val),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary,
            MetaLen:32/integer, MetaBin:MetaLen/binary>>.

bin_contents(Contents) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MD) ->
    {{VTagVal, Deleted, LastModVal}, RestBin} =
        dict:fold(fun fold_meta_to_bin/3,
                    {{undefined, <<0>>, undefined}, <<>>},
                    MD),
    VTagBin = case VTagVal of
                  undefined ->  ?EMPTY_VTAG_BIN;
                  _ -> list_to_binary(VTagVal)
              end,
    VTagLen = byte_size(VTagBin),
    LastModBin = case LastModVal of
                     undefined ->
                        <<0:32/integer, 0:32/integer, 0:32/integer>>;
                     {Mega,Secs,Micro} ->
                        <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>
                 end,
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.

fold_meta_to_bin(?MD_VTAG, Value, {{_Vt,Del,Lm},RestBin}) ->
    {{Value, Del, Lm}, RestBin};
fold_meta_to_bin(?MD_LASTMOD, Value, {{Vt,Del,_Lm},RestBin}) ->
     {{Vt, Del, Value}, RestBin};
fold_meta_to_bin(?MD_DELETED, true, {{Vt,_Del,Lm},RestBin})->
     {{Vt, <<1>>, Lm}, RestBin};
fold_meta_to_bin(?MD_DELETED, "true", Acc) ->
    fold_meta_to_bin(?MD_DELETED, true, Acc);
fold_meta_to_bin(?MD_DELETED, _, {{Vt,_Del,Lm},RestBin}) ->
    {{Vt, <<0>>, Lm}, RestBin};
fold_meta_to_bin(Key, Value, {{_Vt,_Del,_Lm}=Elems,RestBin}) ->
    ValueBin = encode_maybe_binary(Value),
    ValueLen = byte_size(ValueBin),
    KeyBin = encode_maybe_binary(Key),
    KeyLen = byte_size(KeyBin),
    MetaBin = <<KeyLen:32/integer, KeyBin/binary,
                    ValueLen:32/integer, ValueBin/binary>>,
    {Elems, <<RestBin/binary, MetaBin/binary>>}.

encode_maybe_binary(Bin) when is_binary(Bin) ->
    <<1, Bin/binary>>;
encode_maybe_binary(Bin) ->
    <<0, (term_to_binary(Bin))/binary>>.

%% =================================================

sync_strategy() ->
    case erlang:system_info(otp_release) of
        "17" ->
            sync;
        "18" ->
            sync;
        "19" ->
            sync;
        "16" ->
            none
    end.


book_riakput(Pid, RiakObject, IndexSpecs) ->
    leveled_bookie:book_put(Pid,
                            RiakObject#r_object.bucket,
                            RiakObject#r_object.key,
                            to_binary(v1, RiakObject),
                            IndexSpecs,
                            ?RIAK_TAG).

book_riakdelete(Pid, Bucket, Key, IndexSpecs) ->
    leveled_bookie:book_put(Pid, Bucket, Key, delete, IndexSpecs, ?RIAK_TAG).

book_riakget(Pid, Bucket, Key) ->
    leveled_bookie:book_get(Pid, Bucket, Key, ?RIAK_TAG).

book_riakhead(Pid, Bucket, Key) ->
    leveled_bookie:book_head(Pid, Bucket, Key, ?RIAK_TAG).


riakload(Bookie, ObjectList) ->
    lists:foreach(fun({_RN, Obj, Spc}) ->
                            R = book_riakput(Bookie, Obj, Spc),
                            case R of
                                ok -> ok;
                                pause -> timer:sleep(?SLOWOFFER_DELAY)
                            end
                            end,
                    ObjectList).


reset_filestructure() ->
    reset_filestructure(0, ?ROOT_PATH).
    
reset_filestructure(Wait) when is_integer(Wait) ->
    reset_filestructure(Wait, ?ROOT_PATH);
reset_filestructure(RootPath) when is_list(RootPath) ->
    reset_filestructure(0, RootPath).

reset_filestructure(Wait, RootPath) ->
    io:format("Waiting ~w ms to give a chance for all file closes " ++
                 "to complete~n", [Wait]),
    timer:sleep(Wait),
    filelib:ensure_dir(RootPath ++ "/journal/"),
    filelib:ensure_dir(RootPath ++ "/ledger/"),
    leveled_inker:clean_testdir(RootPath ++ "/journal"),
    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    RootPath.

wait_for_compaction(Bookie) ->
    F = fun leveled_bookie:book_islastcompactionpending/1,
    lists:foldl(fun(X, Pending) ->
                        case Pending of
                            false ->
                                false;
                            true ->
                                io:format("Loop ~w waiting for journal "
                                    ++ "compaction to complete~n", [X]),
                                timer:sleep(5000),
                                F(Bookie)
                        end end,
                    true,
                    lists:seq(1, 15)).

check_bucket_stats(Bookie, Bucket) ->
    FoldSW1 = os:timestamp(),
    io:format("Checking bucket size~n"),
    {async, Folder1} = leveled_bookie:book_returnfolder(Bookie,
                                                        {riakbucket_stats,
                                                            Bucket}),
    {B1Size, B1Count} = Folder1(),
    io:format("Bucket fold completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(), FoldSW1)]),
    io:format("Bucket ~s has size ~w and count ~w~n",
                [Bucket, B1Size, B1Count]),
    {B1Size, B1Count}.


check_forlist(Bookie, ChkList) ->
    check_forlist(Bookie, ChkList, false).

check_forlist(Bookie, ChkList, Log) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    if
                        Log == true ->
                            io:format("Fetching Key ~s~n", [Obj#r_object.key]);
                        true ->
                            ok
                    end,
                    R = book_riakget(Bookie,
                                        Obj#r_object.bucket,
                                        Obj#r_object.key),
                    true = case R of
                                {ok, Val} ->
                                    to_binary(v1, Obj) == Val;
                                not_found ->
                                    io:format("Object not found for key ~s~n",
                                                [Obj#r_object.key]),
                                    error
                            end
                    end,
                ChkList),
    io:format("Fetch check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_formissinglist(Bookie, ChkList) ->
    SW = os:timestamp(),
    lists:foreach(fun({_RN, Obj, _Spc}) ->
                    R = book_riakget(Bookie,
                                        Obj#r_object.bucket,
                                        Obj#r_object.key),
                    R = not_found end,
                ChkList),
    io:format("Miss check took ~w microseconds checking list of length ~w~n",
                    [timer:now_diff(os:timestamp(), SW), length(ChkList)]).

check_forobject(Bookie, TestObject) ->
    TestBinary = to_binary(v1, TestObject),
    {ok, TestBinary} = book_riakget(Bookie,
                                        TestObject#r_object.bucket,
                                        TestObject#r_object.key),
    {ok, HeadBinary} = book_riakhead(Bookie,
                                        TestObject#r_object.bucket,
                                        TestObject#r_object.key),
    {{_SibMetaBin, Vclock, _Hash, size}, _LMS}
        = leveled_codec:riak_extract_metadata(HeadBinary, size),
    true = binary_to_term(Vclock) == TestObject#r_object.vclock.

check_formissingobject(Bookie, Bucket, Key) ->
    not_found = book_riakget(Bookie, Bucket, Key),
    not_found = book_riakhead(Bookie, Bucket, Key).


generate_testobject() ->
    {B1, K1, V1, Spec1, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                [{"MDK1", "MDV1"}]},
    generate_testobject(B1, K1, V1, Spec1, MD).

generate_testobject(B, K, V, Spec, MD) ->
    MD0 = [{?MD_LASTMOD, os:timestamp()}|MD],
    Content = #r_content{metadata=dict:from_list(MD0), value=V},
    {#r_object{bucket=B,
                key=K,
                contents=[Content],
                vclock=generate_vclock()},
        Spec}.


generate_compressibleobjects(Count, KeyNumber) ->
    V = get_compressiblevalue(),
    generate_objects(Count, KeyNumber, [], V).


get_compressiblevalue_andinteger() ->
    {leveled_rand:uniform(1000), get_compressiblevalue()}.

get_compressiblevalue() ->
    S1 = "111111111111111",
    S2 = "222222222222222",
    S3 = "333333333333333",
    S4 = "aaaaaaaaaaaaaaa",
    S5 = "AAAAAAAAAAAAAAA",
    S6 = "GGGGGGGGGGGGGGG",
    S7 = "===============",
    S8 = "...............",
    Selector = [{1, S1}, {2, S2}, {3, S3}, {4, S4},
                {5, S5}, {6, S6}, {7, S7}, {8, S8}],
    L = lists:seq(1, 1024),
    lists:foldl(fun(_X, Acc) ->
                    {_, Str} = lists:keyfind(leveled_rand:uniform(8), 1, Selector),
                    Acc ++ Str end,
                "",
                L).

generate_smallobjects(Count, KeyNumber) ->
    generate_objects(Count, KeyNumber, [], leveled_rand:rand_bytes(512)).

generate_objects(Count, KeyNumber) ->
    generate_objects(Count, KeyNumber, [], leveled_rand:rand_bytes(4096)).


generate_objects(Count, KeyNumber, ObjL, Value) ->
    generate_objects(Count, KeyNumber, ObjL, Value, fun() -> [] end).

generate_objects(Count, KeyNumber, ObjL, Value, IndexGen) ->
    generate_objects(Count, KeyNumber, ObjL, Value, IndexGen, "Bucket").

generate_objects(0, _KeyNumber, ObjL, _Value, _IndexGen, _Bucket) ->
    ObjL;
generate_objects(Count, binary_uuid, ObjL, Value, IndexGen, Bucket) ->
    {Obj1, Spec1} = set_object(list_to_binary(Bucket),
                                list_to_binary(leveled_codec:generate_uuid()),
                                Value,
                                IndexGen),
    generate_objects(Count - 1,
                        binary_uuid,
                        ObjL ++ [{leveled_rand:uniform(), Obj1, Spec1}],
                        Value,
                        IndexGen,
                        Bucket);
generate_objects(Count, uuid, ObjL, Value, IndexGen, Bucket) ->
    {Obj1, Spec1} = set_object(Bucket,
                                leveled_codec:generate_uuid(),
                                Value,
                                IndexGen),
    generate_objects(Count - 1,
                        uuid,
                        ObjL ++ [{leveled_rand:uniform(), Obj1, Spec1}],
                        Value,
                        IndexGen,
                        Bucket);
generate_objects(Count, KeyNumber, ObjL, Value, IndexGen, Bucket) ->
    {Obj1, Spec1} = set_object(Bucket,
                                "Key" ++ integer_to_list(KeyNumber),
                                Value,
                                IndexGen),
    generate_objects(Count - 1,
                        KeyNumber + 1,
                        ObjL ++ [{leveled_rand:uniform(), Obj1, Spec1}],
                        Value,
                        IndexGen,
                        Bucket).

set_object(Bucket, Key, Value, IndexGen) ->
    set_object(Bucket, Key, Value, IndexGen, []).

set_object(Bucket, Key, Value, IndexGen, Indexes2Remove) ->
    
    Obj = {Bucket,
            Key,
            Value,
            IndexGen() ++ lists:map(fun({add, IdxF, IdxV}) ->
                                            {remove, IdxF, IdxV} end,
                                        Indexes2Remove),
            [{"MDK", "MDV" ++ Key},
                {"MDK2", "MDV" ++ Key},
                {?MD_LASTMOD, os:timestamp()}]},
    {B1, K1, V1, Spec1, MD} = Obj,
    Content = #r_content{metadata=dict:from_list(MD), value=V1},
    {#r_object{bucket=B1,
                key=K1,
                contents=[Content],
                vclock=generate_vclock()},
        Spec1}.


generate_vclock() ->
    lists:map(fun(X) ->
                    {_, Actor} = lists:keyfind(leveled_rand:uniform(10),
                                                1,
                                                actor_list()),
                    {Actor, X} end,
                    lists:seq(1, leveled_rand:uniform(8))).


actor_list() ->
    [{1, albert}, {2, bertie}, {3, clara}, {4, dave}, {5, elton},
        {6, fred}, {7, george}, {8, harry}, {9, isaac}, {10, leila}].

get_key(Object) ->
    Object#r_object.key.

get_value(ObjectBin) ->
    <<_Magic:8/integer, _Vers:8/integer, VclockLen:32/integer,
            Rest1/binary>> = ObjectBin,
    <<_VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> = Rest1,
    case SibCount of
        1 ->
            <<SibLength:32/integer, Rest2/binary>> = SibsBin,
            <<ContentBin:SibLength/binary, _MetaBin/binary>> = Rest2,
            case ContentBin of
                <<0, ContentBin0/binary>> ->
                    binary_to_term(ContentBin0)
            end;
        N ->
            io:format("SibCount of ~w with ObjectBin ~w~n", [N, ObjectBin]),
            error
    end.

get_vclock(ObjectBin) ->
    <<_Magic:8/integer, _Vers:8/integer, VclockLen:32/integer,
            Rest1/binary>> = ObjectBin,
    <<VclockBin:VclockLen/binary, _Bin/binary>> = Rest1,
    binary_to_term(VclockBin).    

load_objects(ChunkSize, GenList, Bookie, TestObject, Generator) ->
    load_objects(ChunkSize, GenList, Bookie, TestObject, Generator, 1000).

load_objects(ChunkSize, GenList, Bookie, TestObject, Generator, SubListL) ->
    lists:map(fun(KN) ->
                    ObjListA = Generator(ChunkSize, KN),
                    StartWatchA = os:timestamp(),
                    riakload(Bookie, ObjListA),
                    Time = timer:now_diff(os:timestamp(), StartWatchA),
                    io:format("~w objects loaded in ~w seconds~n",
                                [ChunkSize, Time/1000000]),
                    if
                        TestObject == no_check ->
                            ok;
                        true ->
                            check_forobject(Bookie, TestObject)
                    end,
                    lists:sublist(ObjListA, SubListL) end,
                GenList).


get_randomindexes_generator(Count) ->
    Generator = fun() ->
            lists:map(fun(X) ->
                                {add,
                                    "idx" ++ integer_to_list(X) ++ "_bin",
                                    get_randomdate() ++ get_randomname()} end,
                        lists:seq(1, Count))
        end,
    Generator.

name_list() ->
    [{1, "Sophia"}, {2, "Emma"}, {3, "Olivia"}, {4, "Ava"},
            {5, "Isabella"}, {6, "Mia"}, {7, "Zoe"}, {8, "Lily"},
            {9, "Emily"}, {10, "Madelyn"}, {11, "Madison"}, {12, "Chloe"},
            {13, "Charlotte"}, {14, "Aubrey"}, {15, "Avery"},
            {16, "Abigail"}].

get_randomname() ->
    NameList = name_list(),
    N = leveled_rand:uniform(16),
    {N, Name} = lists:keyfind(N, 1, NameList),
    Name.

get_randomdate() ->
    LowTime = 60000000000,
    HighTime = 70000000000,
    RandPoint = LowTime + leveled_rand:uniform(HighTime - LowTime),
    Date = calendar:gregorian_seconds_to_datetime(RandPoint),
    {{Year, Month, Day}, {Hour, Minute, Second}} = Date,
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0w~2..0w~2..0w~2..0w",
                                    [Year, Month, Day, Hour, Minute, Second])).


foldkeysfun(_Bucket, Item, Acc) -> Acc ++ [Item].

foldkeysfun_returnbucket(Bucket, {Term, Key}, Acc) ->
    Acc ++ [{Term, {Bucket, Key}}];
foldkeysfun_returnbucket(Bucket, Key, Acc) ->
    Acc ++ [{Bucket, Key}].

check_indexed_objects(Book, B, KSpecL, V) ->
    % Check all objects match, return what should be the results of an all
    % index query
    IdxR = lists:map(fun({K, Spc}) ->
                            {ok, O} = book_riakget(Book, B, K),
                            V = testutil:get_value(O),
                            {add,
                                "idx1_bin",
                                IdxVal} = lists:keyfind(add, 1, Spc),
                            {IdxVal, K} end,
                        KSpecL),
    % Check the all index query matches expectations
    R = leveled_bookie:book_returnfolder(Book,
                                            {index_query,
                                                B,
                                                {fun foldkeysfun/3, []},
                                                {"idx1_bin",
                                                    "0",
                                                    "|"},
                                                ?RETURN_TERMS}),
    SW = os:timestamp(),
    {async, Fldr} = R,
    QR0 = Fldr(),
    io:format("Query match found of length ~w in ~w microseconds " ++
                    "expected ~w ~n",
                [length(QR0),
                    timer:now_diff(os:timestamp(), SW),
                    length(IdxR)]),
    QR = lists:sort(QR0),
    ER = lists:sort(IdxR),
    
    ok = if
                ER == QR ->
                    ok
            end,
    ok.


put_indexed_objects(Book, Bucket, Count) ->
    V = testutil:get_compressiblevalue(),
    IndexGen = testutil:get_randomindexes_generator(1),
    SW = os:timestamp(),
    ObjL1 = testutil:generate_objects(Count,
                                        uuid,
                                        [],
                                        V,
                                        IndexGen,
                                        Bucket),
    KSpecL = lists:map(fun({_RN, Obj, Spc}) ->
                            book_riakput(Book, Obj, Spc),
                            {testutil:get_key(Obj), Spc}
                            end,
                        ObjL1),
    io:format("Put of ~w objects with ~w index entries "
                    ++
                    "each completed in ~w microseconds~n",
                [Count, 1, timer:now_diff(os:timestamp(), SW)]),
    {KSpecL, V}.


put_altered_indexed_objects(Book, Bucket, KSpecL) ->
    put_altered_indexed_objects(Book, Bucket, KSpecL, true).

put_altered_indexed_objects(Book, Bucket, KSpecL, RemoveOld2i) ->
    IndexGen = testutil:get_randomindexes_generator(1),
    V = testutil:get_compressiblevalue(),
    RplKSpecL = lists:map(fun({K, Spc}) ->
                                AddSpc = if
                                            RemoveOld2i == true ->
                                                [lists:keyfind(add, 1, Spc)];
                                            RemoveOld2i == false ->
                                                []
                                        end,
                                {O, AltSpc} = testutil:set_object(Bucket,
                                                                    K,
                                                                    V,
                                                                    IndexGen,
                                                                    AddSpc),
                                case book_riakput(Book, O, AltSpc) of
                                    ok -> ok;
                                    pause -> timer:sleep(?SLOWOFFER_DELAY)
                                end,
                                {K, AltSpc} end,
                            KSpecL),
    {RplKSpecL, V}.

rotating_object_check(RootPath, B, NumberOfObjects) ->
    BookOpts = [{root_path, RootPath},
                    {cache_size, 1000},
                    {max_journalsize, 5000000},
                    {sync_strategy, sync_strategy()}],
    {ok, Book1} = leveled_bookie:book_start(BookOpts),
    {KSpcL1, V1} = testutil:put_indexed_objects(Book1, B, NumberOfObjects),
    ok = testutil:check_indexed_objects(Book1, B, KSpcL1, V1),
    {KSpcL2, V2} = testutil:put_altered_indexed_objects(Book1, B, KSpcL1),
    ok = testutil:check_indexed_objects(Book1, B, KSpcL2, V2),
    {KSpcL3, V3} = testutil:put_altered_indexed_objects(Book1, B, KSpcL2),
    ok = leveled_bookie:book_close(Book1),
    {ok, Book2} = leveled_bookie:book_start(BookOpts),
    ok = testutil:check_indexed_objects(Book2, B, KSpcL3, V3),
    {KSpcL4, V4} = testutil:put_altered_indexed_objects(Book2, B, KSpcL3),
    ok = testutil:check_indexed_objects(Book2, B, KSpcL4, V4),
    Query = {keylist, ?RIAK_TAG, B, {fun foldkeysfun/3, []}},
    {async, BList} = leveled_bookie:book_returnfolder(Book2, Query),
    true = NumberOfObjects == length(BList()),
    ok = leveled_bookie:book_close(Book2),
    ok.
    
corrupt_journal(RootPath, FileName, Corruptions, BasePosition, GapSize) ->
    OriginalPath = RootPath ++ "/journal/journal_files/" ++ FileName,
    BackupPath = RootPath ++ "/journal/journal_files/" ++
                    filename:basename(FileName, ".cdb") ++ ".bak",
    io:format("Corruption attempt to be made to filename ~s ~w ~w~n",
                [FileName,
                    filelib:is_file(OriginalPath),
                    filelib:is_file(BackupPath)]),
    {ok, _BytesCopied} = file:copy(OriginalPath, BackupPath),
    {ok, Handle} = file:open(OriginalPath, [binary, raw, read, write]),
    lists:foreach(fun(X) ->
                        Position = X * GapSize + BasePosition,
                        ok = file:pwrite(Handle, Position, <<0:8/integer>>)
                        end,
                    lists:seq(1, Corruptions)),
    ok = file:close(Handle).


restore_file(RootPath, FileName) ->
    OriginalPath = RootPath ++ "/journal/journal_files/" ++ FileName,
    BackupPath = RootPath ++ "/journal/journal_files/" ++
                    filename:basename(FileName, ".cdb") ++ ".bak",
    file:copy(BackupPath, OriginalPath).

restore_topending(RootPath, FileName) ->
    OriginalPath = RootPath ++ "/journal/journal_files/" ++ FileName,
    PndPath = RootPath ++ "/journal/journal_files/" ++
                    filename:basename(FileName, ".cdb") ++ ".pnd",
    ok = file:rename(OriginalPath, PndPath),
    false = filelib:is_file(OriginalPath).

find_journals(RootPath) ->
    {ok, FNsA_J} = file:list_dir(RootPath ++ "/journal/journal_files"),
    {ok, Regex} = re:compile(".*\.cdb"),
    CDBFiles = lists:foldl(fun(FN, Acc) -> case re:run(FN, Regex) of
                                                nomatch ->
                                                    Acc;
                                                _ ->
                                                    [FN|Acc]
                                            end
                                            end,
                                [],
                                FNsA_J),
    CDBFiles.

