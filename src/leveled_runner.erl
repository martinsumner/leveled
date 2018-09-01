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
            bucketkey_query/5,
            hashlist_query/3,
            tictactree/5,
            foldheads_allkeys/5,
            foldobjects_allkeys/4,
            foldheads_bybucket/6,
            foldobjects_bybucket/4,
            foldobjects_byindex/3
        ]).


-include_lib("eunit/include/eunit.hrl").

-define(CHECKJOURNAL_PROB, 0.2).

-type key_range() 
            :: {leveled_codec:ledger_key()|null, 
                    leveled_codec:ledger_key()|null}.
-type fun_and_acc()
            :: {fun(), any()}.

%%%============================================================================
%%% External functions
%%%============================================================================


-spec bucket_sizestats(fun(), any(), leveled_codec:tag()) -> {async, fun()}.
%% @doc
%% Fold over a bucket accumulating the count of objects and their total sizes
bucket_sizestats(SnapFun, Bucket, Tag) ->
    StartKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(Bucket, null, Tag),
    AccFun = accumulate_size(),
    Runner = 
        fun() ->
            {ok, LedgerSnap, _JournalSnap} = SnapFun(),
            Acc = leveled_penciller:pcl_fetchkeys(LedgerSnap,
                                                    StartKey,
                                                    EndKey,
                                                    AccFun,
                                                    {0, 0}),
            ok = leveled_penciller:pcl_close(LedgerSnap),
            Acc
        end,
    {async, Runner}.

-spec bucket_list(fun(), leveled_codec:tag(), fun(), any()) 
                                                            -> {async, fun()}.
%% @doc
%% List buckets for tag, assuming bucket names are all either binary, ascii 
%% strings or integers 
bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc) ->
    bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc, -1).

-spec bucket_list(fun(), leveled_codec:tag(), fun(), any(), integer()) 
                                                    -> {async, fun()}.
%% @doc
%% set Max Buckets to -1 to list all buckets, otherwise will only return
%% MaxBuckets (use 1 to confirm that there exists any bucket for a given Tag)
bucket_list(SnapFun, Tag, FoldBucketsFun, InitAcc, MaxBuckets) ->
    Runner = 
        fun() ->
            {ok, LedgerSnapshot, _JournalSnapshot} = SnapFun(),
            BucketAcc = 
                get_nextbucket(null, null, 
                                Tag, LedgerSnapshot, [], {0, MaxBuckets}),
            ok = leveled_penciller:pcl_close(LedgerSnapshot),
            lists:foldl(fun({B, _K}, Acc) -> FoldBucketsFun(B, Acc) end,
                            InitAcc,
                            BucketAcc)
        end,
    {async, Runner}.

-spec index_query(fun(), 
                    {leveled_codec:ledger_key(), 
                        leveled_codec:ledger_key(), 
                        {boolean(), undefined|re:mp()|iodata()}}, 
                    fun_and_acc()) -> {async, fun()}.
%% @doc
%% Secondary index query
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
    AccFun = accumulate_index(TermRegex, AddFun, FoldKeysFun),
    Runner = 
        fun() ->
            {ok, LedgerSnapshot, _JournalSnapshot} = SnapFun(),
            Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                    StartKey,
                                                    EndKey,
                                                    AccFun,
                                                    InitAcc),
            ok = leveled_penciller:pcl_close(LedgerSnapshot),
            Acc
        end,
    {async, Runner}.

-spec bucketkey_query(fun(), leveled_codec:tag(), any(), 
                        key_range(), fun_and_acc()) -> {async, fun()}.
%% @doc
%% Fold over all keys in `KeyRange' under tag (restricted to a given bucket)
bucketkey_query(SnapFun, Tag, Bucket, 
                    {StartKey, EndKey}, 
                    {FoldKeysFun, InitAcc}) ->
    SK = leveled_codec:to_ledgerkey(Bucket, StartKey, Tag),
    EK = leveled_codec:to_ledgerkey(Bucket, EndKey, Tag),
    AccFun = accumulate_keys(FoldKeysFun),
    Runner =
        fun() ->
                {ok, LedgerSnapshot, _JournalSnapshot} = SnapFun(),
                Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                      SK,
                                                      EK,
                                                      AccFun,
                                                      InitAcc),
                ok = leveled_penciller:pcl_close(LedgerSnapshot),
                Acc
        end,
    {async, Runner}.

-spec bucketkey_query(fun(), leveled_codec:tag(), any(), fun_and_acc()) 
                                                        -> {async, fun()}.
%% @doc
%% Fold over all keys under tag (potentially restricted to a given bucket)
bucketkey_query(SnapFun, Tag, Bucket, FunAcc) ->
    bucketkey_query(SnapFun, Tag, Bucket, {null, null}, FunAcc).

-spec hashlist_query(fun(), leveled_codec:tag(), boolean()) -> {async, fun()}.
%% @doc
%% Fold pver the key accumulating the hashes
hashlist_query(SnapFun, Tag, JournalCheck) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    Runner = 
        fun() ->
            {ok, LedgerSnapshot, JournalSnapshot} = SnapFun(),
            AccFun = accumulate_hashes(JournalCheck, JournalSnapshot),    
            Acc = leveled_penciller:pcl_fetchkeys(LedgerSnapshot,
                                                        StartKey,
                                                        EndKey,
                                                        AccFun,
                                                        []),
            ok = leveled_penciller:pcl_close(LedgerSnapshot),
            case JournalCheck of
                false ->
                    ok;
                true ->
                    leveled_inker:ink_close(JournalSnapshot)
            end,
            Acc
        end,
    {async, Runner}.

-spec tictactree(fun(), 
                    {leveled_codec:tag(), any(), tuple()},
                    boolean(), atom(), fun()) 
                    -> {async, fun()}.
%% @doc
%% Return a merkle tree from the fold, directly accessing hashes cached in the 
%% metadata
tictactree(SnapFun, {Tag, Bucket, Query}, JournalCheck, TreeSize, Filter) ->
    % Journal check can be used for object key folds to confirm that the
    % object is still indexed within the journal
    Tree = leveled_tictac:new_tree(temp, TreeSize),
    Runner =
        fun() ->
            {ok, LedgerSnap, JournalSnap} = SnapFun(),
            % The start key and end key will vary depending on whether the
            % fold is to fold over an index or a key range
            EnsureKeyBinaryFun = 
                fun(K, T) -> 
                    case is_binary(K) of 
                        true ->
                            {K, T};
                        false ->
                            {term_to_binary(K), T}
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
                leveled_penciller:pcl_fetchkeys(LedgerSnap, 
                                                StartKey, EndKey,
                                                AccFun, Tree),

            % Close down snapshot when complete so as not to hold removed
            % files open
            ok = leveled_penciller:pcl_close(LedgerSnap),
            case JournalCheck of
                false ->
                    ok;
                true ->
                    leveled_inker:ink_close(JournalSnap)
            end,
            Acc
        end,
    {async, Runner}.

-spec foldheads_allkeys(fun(), leveled_codec:tag(), 
                        fun(), boolean(), false|list(integer())) 
                                                            -> {async, fun()}.
%% @doc
%% Fold over all heads in the store for a given tag - applying the passed 
%% function to each proxy object
foldheads_allkeys(SnapFun, Tag, FoldFun, JournalCheck, SegmentList) ->
    StartKey = leveled_codec:to_ledgerkey(null, null, Tag),
    EndKey = leveled_codec:to_ledgerkey(null, null, Tag),
    foldobjects(SnapFun, 
                Tag, 
                [{StartKey, EndKey}], 
                FoldFun, 
                {true, JournalCheck}, 
                SegmentList).

-spec foldobjects_allkeys(fun(), leveled_codec:tag(), fun(), 
                            key_order|sqn_order) -> {async, fun()}.
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
                % no initial accumulatr passed, and so should be just a list
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
                            {Obj, _IdxSpecs} = leveled_codec:split_inkvalue(VBin),
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

            {ok, LedgerSnapshot, JournalSnapshot} = SnapFun(),
            IsValidFun = 
                fun(Bucket, Key, SQN) ->
                    LedgerKey = leveled_codec:to_ledgerkey(Bucket, Key, Tag),
                    leveled_penciller:pcl_checksequencenumber(LedgerSnapshot, 
                                                                LedgerKey, 
                                                                SQN)
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
                    leveled_log:log("R0001", [length(BatchAcc)]),
                    lists:foldr(ObjFun, ObjAcc, BatchAcc)
                end,
            
            Acc = 
                leveled_inker:ink_fold(JournalSnapshot, 
                                        0,
                                        {FilterFun, InitAccFun, BatchFoldFun},
                                        InitAcc),
            ok = leveled_penciller:pcl_close(LedgerSnapshot),
            ok = leveled_inker:ink_close(JournalSnapshot),
            Acc 
        end,
    {async, Folder}.
            

-spec foldobjects_bybucket(fun(), 
                            leveled_codec:tag(), 
                            list(key_range()), 
                            fun()) -> {async, fun()}.
%% @doc
%% Fold over all objects within a given key range in a bucket
foldobjects_bybucket(SnapFun, Tag, KeyRanges, FoldFun) ->
    foldobjects(SnapFun, 
                Tag, 
                KeyRanges, 
                FoldFun, 
                false, 
                false).

-spec foldheads_bybucket(fun(), 
                            atom(), 
                            list({any(), any()}), 
                            fun(), 
                            boolean(), false|list(integer())) 
                                                        -> {async, fun()}.
%% @doc
%% Fold over all object metadata within a given key range in a bucket
foldheads_bybucket(SnapFun, 
                    Tag, 
                    KeyRanges, 
                    FoldFun, 
                    JournalCheck, SegmentList) ->
    foldobjects(SnapFun, 
                Tag, 
                KeyRanges, 
                FoldFun, 
                {true, JournalCheck}, 
                SegmentList).

-spec foldobjects_byindex(fun(), tuple(), fun()) -> {async, fun()}.
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
    BKList;
get_nextbucket(NextBucket, NextKey, Tag, LedgerSnapshot, BKList, {C, L}) ->
    Now = leveled_util:integer_now(),
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
        null ->
            leveled_log:log("B0008",[]),
            BKList;
        {{B, K}, V} ->
            case leveled_codec:is_active({Tag, B, K, null}, V, Now) of
                true ->
                    leveled_log:log("B0009",[B]),
                    get_nextbucket(leveled_codec:next_key(B),
                                    null,
                                    Tag,
                                    LedgerSnapshot,
                                    [{B, K}|BKList],
                                    {C + 1, L});
                false ->
                    NK = 
                        case Tag of
                            ?HEAD_TAG ->
                                {PK, SK} = K,
                                {PK, leveled_codec:next_key(SK)};
                            _ ->
                                leveled_codec:next_key(K)
                        end,
                    get_nextbucket(B, NK, Tag, LedgerSnapshot, BKList, {C, L})
            end;
        {NB, _V} ->
            leveled_log:log("B0010",[NB]),
            []
    end.


-spec foldobjects(fun(), atom(), list(), fun(), 
                    false|{true, boolean()}, false|list(integer())) ->
                                                            {async, fun()}.
%% @doc
%% The object folder should be passed DeferredFetch.
%% DeferredFetch can either be false (which will return to the fold function
%% the full object), or {true, CheckPresence} - in which case a proxy object
%% will be created that if understood by the fold function will allow the fold
%% function to work on the head of the object, and defer fetching the body in
%% case such a fetch is unecessary.
foldobjects(SnapFun, Tag, KeyRanges, FoldObjFun, DeferredFetch, SegmentList) ->
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
    
    Folder =
        fun() ->
            {ok, LedgerSnapshot, JournalSnapshot} = SnapFun(),

            AccFun = accumulate_objects(FoldFun,
                                        JournalSnapshot,
                                        Tag,
                                        DeferredFetch),

            ListFoldFun = 
                fun({StartKey, EndKey}, FoldAcc) ->
                    leveled_penciller:pcl_fetchkeysbysegment(LedgerSnapshot,
                                                                StartKey,
                                                                EndKey,
                                                                AccFun,
                                                                FoldAcc, 
                                                                SegmentList)
                end,
            Acc = lists:foldl(ListFoldFun, InitAcc, KeyRanges),
            ok = leveled_penciller:pcl_close(LedgerSnapshot),
            case DeferredFetch of 
                {true, false} ->
                    ok;
                _ ->
                    ok = leveled_inker:ink_close(JournalSnapshot)
            end,
            Acc
        end,
    {async, Folder}.


accumulate_size() ->
    Now = leveled_util:integer_now(),
    AccFun = fun(Key, Value, {Size, Count}) ->
                    case leveled_codec:is_active(Key, Value, Now) of
                            true ->
                                {Size + leveled_codec:get_size(Key, Value),
                                    Count + 1};
                            false ->
                                {Size, Count}
                        end
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
    Now = leveled_util:integer_now(),
    AccFun =
        fun(LK, V, Acc) ->
            case leveled_codec:is_active(LK, V, Now) of
                true ->
                    {B, K, H} = leveled_codec:get_keyandobjhash(LK, V),
                    Check = leveled_rand:uniform() < ?CHECKJOURNAL_PROB,
                    case {JournalCheck, Check} of
                        {true, true} ->
                            case check_presence(LK, V, InkerClone) of
                                true ->
                                    AddKeyFun(B, K, H, Acc);
                                false ->
                                    Acc
                            end;
                        _ ->
                            AddKeyFun(B, K, H, Acc)
                    end;
                false ->
                    Acc
            end
        end,
    AccFun.


accumulate_objects(FoldObjectsFun, InkerClone, Tag, DeferredFetch) ->
    Now = leveled_util:integer_now(),
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
            case leveled_codec:is_active(LK, V, Now) of
                true ->
                    {SQN, _St, _MH, MD} =
                        leveled_codec:striphead_to_details(V),
                    {B, K} =
                        case leveled_codec:from_ledgerkey(LK) of
                            {B0, K0} ->
                                {B0, K0};
                            {B0, K0, _T0} ->
                                {B0, K0}
                        end,
                    JK = {leveled_codec:to_ledgerkey(B, K, Tag), SQN},
                    case DeferredFetch of
                        {true, true} ->
                            InJournal =
                                leveled_inker:ink_keycheck(InkerClone,
                                                            LK,
                                                            SQN),
                            case InJournal of
                                probably ->
                                    ProxyObj = 
                                        make_proxy_object(Tag, 
                                                            LK, JK, MD, V,
                                                            InkerClone),
                                    FoldObjectsFun(B, K, ProxyObj, Acc);
                                missing ->
                                    Acc
                            end;
                        {true, false} ->
                            ProxyObj = 
                                make_proxy_object(Tag, 
                                                    LK, JK, MD, V,
                                                    InkerClone),
                            FoldObjectsFun(B, K, ProxyObj, Acc);
                        false ->
                            R = leveled_bookie:fetch_value(InkerClone, JK),
                            case R of
                                not_present ->
                                    Acc;
                                Value ->
                                    FoldObjectsFun(B, K, Value, Acc)

                            end
                    end;
                false ->
                    Acc
            end
        end,
    AccFun.


make_proxy_object(?HEAD_TAG, _LK, _JK, MD, _V, _InkerClone) ->
    MD;
make_proxy_object(_Tag, LK, JK, MD, V, InkerClone) ->
    Size = leveled_codec:get_size(LK, V),
    MDBin = leveled_codec:build_metadata_object(LK, MD),
    term_to_binary({proxy_object,
                    MDBin,
                    Size,
                    {fun leveled_bookie:fetch_value/2, InkerClone, JK}}).

check_presence(Key, Value, InkerClone) ->
    {LedgerKey, SQN} = leveled_codec:strip_to_keyseqonly({Key, Value}),
    case leveled_inker:ink_keycheck(InkerClone, LedgerKey, SQN) of
        probably ->
            true;
        missing ->
            false
    end.

accumulate_keys(FoldKeysFun) ->
    Now = leveled_util:integer_now(),
    AccFun = fun(Key, Value, Acc) ->
                    case leveled_codec:is_active(Key, Value, Now) of
                        true ->
                            {B, K} = leveled_codec:from_ledgerkey(Key),
                            FoldKeysFun(B, K, Acc);
                        false ->
                            Acc
                    end
                end,
    AccFun.

add_keys(ObjKey, _IdxValue) ->
    ObjKey.

add_terms(ObjKey, IdxValue) ->
    {IdxValue, ObjKey}.

accumulate_index(TermRe, AddFun, FoldKeysFun) ->
    Now = leveled_util:integer_now(),
    case TermRe of
        undefined ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value, Now) of
                    true ->
                        {Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        FoldKeysFun(Bucket, AddFun(ObjKey, IdxValue), Acc);
                    false ->
                        Acc
                end end;
        TermRe ->
            fun(Key, Value, Acc) ->
                case leveled_codec:is_active(Key, Value, Now) of
                    true ->
                        {Bucket,
                            ObjKey,
                            IdxValue} = leveled_codec:from_ledgerkey(Key),
                        case re:run(IdxValue, TermRe) of
                            nomatch ->
                                Acc;
                            _ ->
                                FoldKeysFun(Bucket,
                                            AddFun(ObjKey, IdxValue),
                                            Acc)
                        end;
                    false ->
                        Acc
                end end
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).



-endif.

    
    
