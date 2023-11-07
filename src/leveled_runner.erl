%% -------- RUNNER ---------
%%
%% A bookie's runner would traditionally allow remote actors to place bets 
%% via the runner.  In this case the runner will allow a remote actor to 
%% have query access to the ledger or journal.  Runners provide a snapshot of
%% the book for querying the backend.  
%%
%% Runners implement the {async, Folder} within Riak backends - returning an 
%% {async, Runner}.  Runner is just a function that provides access to a 
%% snapshot of the database to allow for a particular query.  The
%% Runner may make the snapshot at the point it is called, or the snapshot can
%% be generated and encapsulated within the function (known as snap_prefold).   
%%
%% Runners which view only the Ledger (the Penciller view of the state) may 
%% have a CheckPresence boolean - which causes the function to perform a basic 
%% check that the item is available in the Journal via the Inker as part of 
%% the fold.  This may be useful for anti-entropy folds 


-module(leveled_runner).

-include("include/leveled.hrl").

-export([
            bucket_sizestats/3,
            bucket_list/4,
            bucket_list/5,
            index_query/3,
            bucketkey_query/4,
            bucketkey_query/6,
            hashlist_query/3,
            tictactree/5,
            foldheads_allkeys/7,
            foldobjects_allkeys/4,
            foldheads_bybucket/8,
            foldobjects_bybucket/4,
            foldobjects_byindex/3
        ]).

-define(CHECKJOURNAL_PROB, 0.2).

-type key_range() 
            :: {leveled_codec:ledger_key()|null, 
                    leveled_codec:ledger_key()|null}.
-type foldacc() :: any().
    % Can't currently be specific about what an acc might be

-type fold_objects_fun()
    :: fun((leveled_codec:key(), leveled_codec:key(), any(), foldacc())
                -> foldacc()).
-type fold_keys_fun()
    :: fun((leveled_codec:key(), leveled_codec:key(), foldacc())
                -> foldacc()).
-type fold_buckets_fun()
    :: fun((leveled_codec:key(), foldacc()) -> foldacc()).
-type fold_filter_fun()
    :: fun((leveled_codec:key(), leveled_codec:key()) -> accumulate|pass).

-type snap_fun()
    :: fun(() -> {ok, pid(), pid()|null}).
-type runner_fun()
    :: fun(() -> foldacc()).
-type acc_fun()
    :: fun((leveled_codec:key(), any(), foldacc()) -> foldacc()).
-type mp()
    :: {re_pattern, term(), term(), term(), term()}.


%%%============================================================================
%%% External functions
%%%============================================================================


-spec bucket_sizestats(snap_fun(),leveled_codec:key(), leveled_codec:tag())
                                                    -> {async, runner_fun()}.
%% @doc
%% Fold over a bucket accumulating the count of objects and their total sizes
bucket_sizestats(SnapFun, Bucket, Tag) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    AccFun = accumulate_size(),
    Runner = 
        fun() ->
            {ok, LedgerSnap, _JournalSnap, AfterFun} = SnapFun(),
            Acc = 
                leveled_penciller:pcl_fetchkeys(
                    LedgerSnap, StartKey, EndKey, AccFun, {0, 0}, as_pcl),
            AfterFun(),
            Acc
        end,
    {async, Runner}.

-spec bucket_list(snap_fun(),
                    leveled_codec:tag(),
                    fold_buckets_fun(), foldacc()) -> {async, runner_fun()}.
%% @doc
%% List buckets for tag, assuming bucket names are all either binary, ascii 
%% strings or integers 
bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc) ->
    bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc, -1).

-spec bucket_list(snap_fun(),
                    leveled_codec:tag(),
                    fold_buckets_fun(), foldacc(), 
                    integer()) -> {async, runner_fun()}.
%% @doc
%% set Max Buckets to -1 to list all buckets, otherwise will only return
%% MaxBuckets (use 1 to confirm that there exists any bucket for a given Tag)
bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc, MaxBuckets) ->
    Runner = 
        fun() ->
            {ok, LedgerSnapshot, _JournalSnapshot, AfterFun} = SnapFun(),
            BucketAcc = 
                get_nextbucket(
                    null, null, Tag, LedgerSnapshot, [], {0, MaxBuckets}),
            FoldRunner =
                fun() ->
                    lists:foldr(
                        fun({B, _K}, Acc) -> FoldBucketsFun(B, Acc) end,
                        InitAcc,
                        BucketAcc)
                        % Buckets in reverse alphabetical order so foldr
                end,
            % For this fold, the fold over the store is actually completed
            % before results are passed to the FoldBucketsFun to be
            % accumulated.  Using a throw to exit the fold early will not
            % in this case save significant time.
            wrap_runner(FoldRunner, AfterFun)
        end,
    {async, Runner}.

-spec index_query(snap_fun(), 
                    {leveled_codec:ledger_key(), 
                        leveled_codec:ledger_key(), 
                        {boolean(), undefined|mp()|iodata()}}, 
                    {fold_keys_fun(), foldacc()})
                        -> {async, runner_fun()}.
%% @doc
%% Secondary index query
%% This has the special capability that it will expect a message to be thrown
%% during the query - and handle this without crashing the penciller snapshot
%% This allows for this query to be used with a max_results check in the 
%% applictaion - and to throw a stop message to be caught by the worker 
%% handling the runner.  This behaviour will not prevent the snapshot from
%% closing neatly, allowing delete_pending files to be cleared without waiting
%% for a timeout
index_query(SnapFun, {StartKey, EndKey, TermHandling}, FoldAccT) ->
    {FoldKeysFun, InitAcc} = FoldAccT,
    {ReturnTerms, TermRegex} = TermHandling,
    AddFun = 
        case ReturnTerms of
            true ->
                fun add_terms/2;
            _ ->
                fun add_keys/2
        end,

    Runner = 
        fun() ->
            {ok, LedgerSnapshot, _JournalSnapshot, AfterFun} = SnapFun(),
            Folder =
                leveled_penciller:pcl_fetchkeys(
                    LedgerSnapshot,
                    StartKey,
                    EndKey,
                    accumulate_index(TermRegex, AddFun, FoldKeysFun),
                    InitAcc,
                    by_runner),
            wrap_runner(Folder, AfterFun)
        end,
    {async, Runner}.

-spec bucketkey_query(snap_fun(),
                        leveled_codec:tag(),
                        leveled_codec:key()|null, 
                        key_range(),
                        {fold_keys_fun(), foldacc()},
                        leveled_codec:regular_expression())
                        -> {async, runner_fun()}.
%% @doc
%% Fold over all keys in `KeyRange' under tag (restricted to a given bucket)
bucketkey_query(SnapFun, Tag, Bucket, 
                    {StartKey, EndKey}, 
                    {FoldKeysFun, InitAcc},
                    TermRegex) ->
    SK = leveled_codec:to_ledgerkey(Bucket, StartKey, Tag),
    EK = leveled_codec:to_ledgerkey(Bucket, EndKey, Tag),
    AccFun = accumulate_keys(FoldKeysFun, TermRegex),
    Runner =
        fun() ->
            {ok, LedgerSnapshot, _JournalSnapshot, AfterFun} = SnapFun(),
            Folder =
                leveled_penciller:pcl_fetchkeys(
                    LedgerSnapshot, SK, EK, AccFun, InitAcc, by_runner),
            wrap_runner(Folder, AfterFun)
        end,
    {async, Runner}.

-spec bucketkey_query(snap_fun(),
                        leveled_codec:tag(),
                        leveled_codec:key()|null,
                        {fold_keys_fun(), foldacc()}) -> {async, runner_fun()}.
%% @doc
%% Fold over all keys under tag (potentially restricted to a given bucket)
bucketkey_query(SnapFun, Tag, Bucket, FunAcc) ->
    bucketkey_query(SnapFun, Tag, Bucket, {null, null}, FunAcc, undefined).

-spec hashlist_query(snap_fun(),
                        leveled_codec:tag(),
                        boolean()) -> {async, runner_fun()}.
%% @doc
%% Fold over the keys under a given Tag accumulating the hashes
hashlist_query(SnapFun, Tag, JournalCheck) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    Runner = 
        fun() ->
            {ok, LedgerSnapshot, JournalSnapshot, AfterFun} = SnapFun(),
            AccFun = accumulate_hashes(JournalCheck, JournalSnapshot),    
            Acc =
                leveled_penciller:pcl_fetchkeys(
                    LedgerSnapshot, StartKey, EndKey, AccFun, []),
            AfterFun(),
            Acc
        end,
    {async, Runner}.

-spec tictactree(snap_fun(), 
                    {leveled_codec:tag(), leveled_codec:key(), tuple()},
                    boolean(), atom(), fold_filter_fun()) 
                    -> {async, runner_fun()}.
%% @doc
%% Return a merkle tree from the fold, directly accessing hashes cached in the 
%% metadata
tictactree(SnapFun, {Tag, Bucket, Query}, JournalCheck, TreeSize, Filter) ->
    % Journal check can be used for object key folds to confirm that the
    % object is still indexed within the journal
    Tree = leveled_tictac:new_tree(temp, TreeSize),
    Runner =
        fun() ->
            {ok, LedgerSnap, JournalSnap, AfterFun} = SnapFun(),
            % The start key and end key will vary depending on whether the
            % fold is to fold over an index or a key range
            EnsureKeyBinaryFun = 
                fun(K, T) -> 
                    case is_binary(K) of 
                        true ->
                            {K, T};
                        false ->
                            {leveled_util:t2b(K), T}
                    end 
                end,
            {StartKey, EndKey, ExtractFun} =
                case Tag of
                    ?IDX_TAG ->
                        {IdxFld, StartIdx, EndIdx} = Query,
                        KeyDefFun = fun leveled_codec:to_ledgerkey/5,
                        {KeyDefFun(Bucket, null, ?IDX_TAG, IdxFld, StartIdx),
                            KeyDefFun(Bucket, null, ?IDX_TAG, IdxFld, EndIdx),
                            EnsureKeyBinaryFun};
                    _ ->
                        {StartOKey, EndOKey} = Query,
                        {leveled_codec:to_ledgerkey(Bucket, StartOKey, Tag),
                            leveled_codec:to_ledgerkey(Bucket, EndOKey, Tag),
                            fun(K, H) -> 
                                V = {is_hash, H},
                                EnsureKeyBinaryFun(K, V)
                            end}
                end,
            AccFun = 
                accumulate_tree(Filter, JournalCheck, JournalSnap, ExtractFun),
            Acc = 
                leveled_penciller:pcl_fetchkeys(
                    LedgerSnap,  StartKey, EndKey, AccFun, Tree),
            AfterFun(),
            Acc
        end,
    {async, Runner}.

-spec foldheads_allkeys(snap_fun(), leveled_codec:tag(), 
                        fold_objects_fun()|{fold_objects_fun(), foldacc()}, 
                        boolean(), false|list(integer()),
                        false|leveled_codec:lastmod_range(),
                        false|pos_integer()) -> {async, runner_fun()}.
%% @doc
%% Fold over all heads in the store for a given tag - applying the passed 
%% function to each proxy object
foldheads_allkeys(SnapFun, Tag, FoldFun, JournalCheck,
                    SegmentList, LastModRange, MaxObjectCount) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    foldobjects(SnapFun, 
                Tag, 
                [{StartKey, EndKey}], 
                FoldFun, 
                {true, JournalCheck}, 
                SegmentList,
                LastModRange,
                MaxObjectCount).

-spec foldobjects_allkeys(snap_fun(),
                            leveled_codec:tag(),
                            fold_objects_fun()|{fold_objects_fun(), foldacc()}, 
                            key_order|sqn_order)
                                -> {async, runner_fun()}.
%% @doc
%% Fold over all objects for a given tag
foldobjects_allkeys(SnapFun, Tag, FoldFun, key_order) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    foldobjects(SnapFun, 
                Tag, 
                [{StartKey, EndKey}], 
                FoldFun, 
                false, 
                false);
foldobjects_allkeys(SnapFun, Tag, FoldObjectsFun, sqn_order) ->
    % Fold over the journal in order of receipt
    {FoldFun, InitAcc} =
        case is_tuple(FoldObjectsFun) of
            true ->
                % FoldObjectsFun is already a tuple with a Fold function and an
                % initial accumulator
                FoldObjectsFun;
            false ->
                % no initial accumulator passed, and so should be just a list
                {FoldObjectsFun, []}
        end,
    
    FilterFun =
        fun(JKey, JVal, _Pos, Acc, ExtractFun) ->
            {SQN, InkTag, LedgerKey} = JKey,
            case {InkTag, leveled_codec:from_ledgerkey(Tag, LedgerKey)} of 
                {?INKT_STND, {B, K}} ->
                    % Ignore tombstones and non-matching Tags and Key changes
                    % objects.  
                    {MinSQN, MaxSQN, BatchAcc} = Acc,
                    case SQN of
                        SQN when SQN < MinSQN ->
                            {loop, Acc};
                        SQN when SQN > MaxSQN ->
                            {stop, Acc};
                        _ ->
                            {VBin, _VSize} = ExtractFun(JVal),
                            {Obj, _IdxSpecs} =
                                leveled_codec:split_inkvalue(VBin),
                            ToLoop = 
                                case SQN  of 
                                    MaxSQN -> stop;
                                    _ -> loop
                                end,
                            {ToLoop, 
                                {MinSQN, MaxSQN, [{B, K, SQN, Obj}|BatchAcc]}}
                    end;
                _ ->
                    {loop, Acc}
            end 
        end,

    InitAccFun = fun(_FN, _SQN) -> [] end,

    Folder =
        fun() ->
            {ok, LedgerSnapshot, JournalSnapshot, AfterFun} = SnapFun(),
            {ok, JournalSQN} = leveled_inker:ink_getjournalsqn(JournalSnapshot),
            IsValidFun = 
                fun(Bucket, Key, SQN) ->
                    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
                    CheckSQN =
                        leveled_penciller:pcl_checksequencenumber(
                            LedgerSnapshot, LedgerKey, SQN),
                    % Need to check that we have not folded past the point
                    % at which the snapshot was taken
                    (JournalSQN >= SQN) and (CheckSQN == current)
                end,

            BatchFoldFun = 
                fun(BatchAcc, ObjAcc) ->
                    ObjFun = 
                        fun({B, K, SQN, Obj}, Acc) -> 
                            case IsValidFun(B, K, SQN) of
                                true ->
                                    FoldFun(B, K, Obj, Acc);
                                false ->
                                    Acc
                            end 
                        end,
                    leveled_log:log(r0001, [length(BatchAcc)]),
                    lists:foldr(ObjFun, ObjAcc, BatchAcc)
                end,
            
            InkFolder = 
                leveled_inker:ink_fold(
                    JournalSnapshot, 
                    0,
                    {FilterFun, InitAccFun, BatchFoldFun},
                    InitAcc),
            wrap_runner(InkFolder, AfterFun) 
        end,
    {async, Folder}.
            

-spec foldobjects_bybucket(snap_fun(), 
                            leveled_codec:tag(), 
                            list(key_range()), 
                            fold_objects_fun()|{fold_objects_fun(), foldacc()})
                                -> {async, runner_fun()}.
%% @doc
%% Fold over all objects within a given key range in a bucket
foldobjects_bybucket(SnapFun, Tag, KeyRanges, FoldFun) ->
    foldobjects(
        SnapFun, Tag, KeyRanges, FoldFun, false, false).

-spec foldheads_bybucket(snap_fun(), 
                            leveled_codec:tag(), 
                            list(key_range()), 
                            fold_objects_fun()|{fold_objects_fun(), foldacc()},
                            boolean(),
                            false|list(integer()),
                            false|leveled_codec:lastmod_range(),
                            false|pos_integer()) 
                                -> {async, runner_fun()}.
%% @doc
%% Fold over all object metadata within a given key range in a bucket
foldheads_bybucket(SnapFun, 
                    Tag, 
                    KeyRanges, 
                    FoldFun, 
                    JournalCheck,
                    SegmentList, LastModRange, MaxObjectCount) ->
    foldobjects(SnapFun, 
                Tag, 
                KeyRanges, 
                FoldFun, 
                {true, JournalCheck}, 
                SegmentList,
                LastModRange,
                MaxObjectCount).

-spec foldobjects_byindex(snap_fun(),
                            tuple(),
                            fold_objects_fun()|{fold_objects_fun(), foldacc()})
                                -> {async, runner_fun()}.
%% @doc
%% Folds over an index, fetching the objects associated with the keys returned 
%% and passing those objects into the fold function
foldobjects_byindex(SnapFun, {Tag, Bucket, Field, FromTerm, ToTerm}, FoldFun) ->
    StartKey =
        leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field, FromTerm),
    EndKey =
        leveled_codec:to_ledgerkey(Bucket, null, ?IDX_TAG, Field, ToTerm),
    foldobjects(SnapFun, 
                Tag, 
                [{StartKey, EndKey}], 
                FoldFun, 
                false, 
                false).



%%%============================================================================
%%% Internal functions
%%%============================================================================

get_nextbucket(_NextB, _NextK, _Tag, _LS, BKList, {Limit, Limit}) ->
    lists:reverse(BKList);
get_nextbucket(NextBucket, NextKey, Tag, LedgerSnapshot, BKList, {C, L}) ->
    StartKey = leveled_codec:to_ledgerkey(NextBucket, NextKey, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    ExtractFun =
        fun(LK, V, _Acc) ->
            {leveled_codec:from_ledgerkey(LK), V}
        end,
    R = leveled_penciller:pcl_fetchnextkey(LedgerSnapshot,
                                                    StartKey,
                                                    EndKey,
                                                    ExtractFun,
                                                    null),
    case R of
        {1, null} ->
            leveled_log:log(b0008,[]),
            BKList;
        {0, {{B, K}, _V}} ->
            leveled_log:log(b0009,[B]),
            get_nextbucket(leveled_codec:next_key(B),
                            null,
                            Tag,
                            LedgerSnapshot,
                            [{B, K}|BKList],
                            {C + 1, L})
    end.


-spec foldobjects(snap_fun(),
                    atom(),
                    list(), 
                    fold_objects_fun()|{fold_objects_fun(), foldacc()}, 
                    false|{true, boolean()}, false|list(integer())) ->
                                                        {async, runner_fun()}.
foldobjects(SnapFun, Tag, KeyRanges, FoldObjFun, DeferredFetch, SegmentList) ->
    foldobjects(SnapFun, Tag, KeyRanges,
                    FoldObjFun, DeferredFetch, SegmentList, false, false).

-spec foldobjects(snap_fun(), atom(), list(),
                    fold_objects_fun()|{fold_objects_fun(), foldacc()}, 
                    false|{true, boolean()},
                    false|list(integer()),
                    false|leveled_codec:lastmod_range(),
                    false|pos_integer()) -> {async, runner_fun()}.
%% @doc
%% The object folder should be passed DeferredFetch.
%% DeferredFetch can either be false (which will return to the fold function
%% the full object), or {true, CheckPresence} - in which case a proxy object
%% will be created that if understood by the fold function will allow the fold
%% function to work on the head of the object, and defer fetching the body in
%% case such a fetch is unecessary.
foldobjects(SnapFun, Tag, KeyRanges, FoldObjFun, DeferredFetch, 
                                SegmentList, LastModRange, MaxObjectCount) ->
    {FoldFun, InitAcc} =
        case is_tuple(FoldObjFun) of
            true ->
                % FoldObjectsFun is already a tuple with a Fold function and an
                % initial accumulator
                FoldObjFun;
            false ->
                % no initial accumulator passed, and so should be just a list
                {FoldObjFun, []}
        end,
    {LimitByCount, InitAcc0} = 
        case MaxObjectCount of
            false ->
                {false, InitAcc};
            MOC when is_integer(MOC) ->
                {true, {MOC, InitAcc}}
        end,
    
    Folder =
        fun() ->
            {ok, LedgerSnapshot, JournalSnapshot, AfterFun} = SnapFun(),
            AccFun =
                accumulate_objects(
                    FoldFun, JournalSnapshot, Tag, DeferredFetch),
            FoldFunGen = 
                fun({StartKey, EndKey}, FoldAcc) ->
                    leveled_penciller:pcl_fetchkeysbysegment(LedgerSnapshot,
                                                                StartKey,
                                                                EndKey,
                                                                AccFun,
                                                                FoldAcc, 
                                                                SegmentList,
                                                                LastModRange,
                                                                LimitByCount)
                end,
            ListFoldFun = 
                fun(KeyRange, Acc) ->
                    Folder = FoldFunGen(KeyRange, Acc),
                    Folder()
                end,
            FolderToWrap =
                fun() -> lists:foldl(ListFoldFun, InitAcc0, KeyRanges) end,
            wrap_runner(FolderToWrap, AfterFun)
        end,
    {async, Folder}.


accumulate_size() ->
    AccFun =
        fun(Key, Value, {Size, Count}) ->
            {Size + leveled_codec:get_size(Key, Value), Count + 1}
        end,
    AccFun.

accumulate_hashes(JournalCheck, InkerClone) ->
    AddKeyFun =
        fun(B, K, H, Acc) ->
            [{B, K, H}|Acc]
        end,
    get_hashaccumulator(JournalCheck,
                            InkerClone,
                            AddKeyFun).

accumulate_tree(FilterFun, JournalCheck, InkerClone, HashFun) ->
    AddKeyFun =
        fun(B, K, H, Tree) ->
            case FilterFun(B, K) of
                accumulate ->
                    leveled_tictac:add_kv(Tree, K, H, HashFun);
                pass ->
                    Tree
            end
        end,
    get_hashaccumulator(JournalCheck,
                        InkerClone,
                        AddKeyFun).

get_hashaccumulator(JournalCheck, InkerClone, AddKeyFun) ->
    AccFun =
        fun(LK, V, Acc) ->
            {B, K, H} = leveled_codec:get_keyandobjhash(LK, V),
            Check = leveled_rand:uniform() < ?CHECKJOURNAL_PROB,
            case JournalCheck and Check of
                true ->
                    case check_presence(LK, V, InkerClone) of
                        true ->
                            AddKeyFun(B, K, H, Acc);
                        false ->
                            Acc
                    end;
                _ ->
                    AddKeyFun(B, K, H, Acc)
            end
        end,
    AccFun.

-spec accumulate_objects(fold_objects_fun(),
                            pid()|null,
                            leveled_codec:tag(),
                            false|{true, boolean()})
                                -> acc_fun().
accumulate_objects(FoldObjectsFun, InkerClone, Tag, DeferredFetch) ->
    AccFun =
        fun(LK, V, Acc) ->
            % The function takes the Ledger Key and the value from the
            % ledger (with the value being the object metadata)
            %
            % Need to check if this is an active object (so TTL has not
            % expired).
            % If this is a deferred_fetch (i.e. the fold is a fold_heads not
            % a fold_objects), then a metadata object needs to be built to be
            % returned - but a quick check that Key is present in the Journal
            % is made first
            {SQN, _St, _MH, MD} =
                leveled_codec:striphead_to_v1details(V),
            {B, K} =
                case leveled_codec:from_ledgerkey(LK) of
                    {B0, K0} ->
                        {B0, K0};
                    {B0, K0, _T0} ->
                        {B0, K0}
                end,
            JK = {leveled_codec:to_ledgerkey(B, K, Tag), SQN},
            case DeferredFetch of
                {true, JournalCheck} ->
                    ProxyObj = 
                        leveled_codec:return_proxy(Tag, MD, InkerClone, JK),
                    case JournalCheck of
                        true ->
                            InJournal =
                                leveled_inker:ink_keycheck(InkerClone,
                                                            LK,
                                                            SQN),
                            case InJournal of
                                probably ->
                                    FoldObjectsFun(B, K, ProxyObj, Acc);
                                missing ->
                                    Acc
                            end;
                        false ->
                            FoldObjectsFun(B, K, ProxyObj, Acc)
                    end;
                false ->
                    R = leveled_bookie:fetch_value(InkerClone, JK),
                    case R of
                        not_present ->
                            Acc;
                        Value ->
                            FoldObjectsFun(B, K, Value, Acc)
                    end
            end
        end,
    AccFun.


check_presence(Key, Value, InkerClone) ->
    {LedgerKey, SQN} = leveled_codec:strip_to_keyseqonly({Key, Value}),
    case leveled_inker:ink_keycheck(InkerClone, LedgerKey, SQN) of
        probably ->
            true;
        missing ->
            false
    end.

accumulate_keys(FoldKeysFun, TermRegex) ->
    AccFun = 
        fun(Key, _Value, Acc) ->
            {B, K} = leveled_codec:from_ledgerkey(Key),
            case TermRegex of
                undefined ->
                    FoldKeysFun(B, K, Acc);
                Re ->
                    case re:run(K, Re) of
                        nomatch ->
                            Acc;
                        _ ->
                            FoldKeysFun(B, K, Acc)
                    end
            end
        end,
    AccFun.

add_keys(ObjKey, _IdxValue) ->
    ObjKey.

add_terms(ObjKey, IdxValue) ->
    {IdxValue, ObjKey}.

accumulate_index(TermRe, AddFun, FoldKeysFun) ->
    case TermRe of
        undefined ->
            fun(Key, _Value, Acc) ->
                {Bucket, ObjKey, IdxValue} = leveled_codec:from_ledgerkey(Key),
                FoldKeysFun(Bucket, AddFun(ObjKey, IdxValue), Acc)
            end;
        TermRe ->
            fun(Key, _Value, Acc) ->
                {Bucket, ObjKey, IdxValue} = leveled_codec:from_ledgerkey(Key),
                case re:run(IdxValue, TermRe) of
                    nomatch ->
                        Acc;
                    _ ->
                        FoldKeysFun(Bucket, AddFun(ObjKey, IdxValue), Acc)
                end
            end
    end.

-spec wrap_runner(fun(), fun()) -> any().
%% @doc
%% Allow things to be thrown in folds, and ensure clean-up action is still
%% undertaken if they are.
%%
%% It is assumed this is only used at present by index queries and key folds,
%% but the wrap could be applied more generally with further work
wrap_runner(FoldAction, AfterAction) ->
    try FoldAction()
    catch throw:Throw ->
        throw(Throw)
    after AfterAction()
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Note in OTP 22 we see a compile error with the assertException if using the
%% eqc_cover parse_transform.  This test is excluded in the eqc profle, due to
%% this error

-ifdef(EQC).

throw_test() ->
    StoppedFolder =
        fun() ->
            throw(stop_fold)
        end,
    CompletedFolder =
        fun() ->
            {ok, ['1']}
        end,
    AfterAction =
        fun() ->
            error
        end,
    ?assertMatch({ok, ['1']}, 
                    wrap_runner(CompletedFolder, AfterAction)),
    ?assertException(throw, stop_fold, 
                        wrap_runner(StoppedFolder, AfterAction)).

-endif.

-endif.

    
    
