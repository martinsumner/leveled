%% -------- PENCILLER ---------
%%
%% The penciller is responsible for writing and re-writing the ledger - a
%% persisted, ordered view of non-recent Keys and Metadata which have been
%% added to the store.
%% - The penciller maintains a manifest of all the files within the current
%% Ledger. 
%% - The Penciller provides re-write (compaction) work up to be managed by
%% the Penciller's Clerk
%% - The Penciller can be cloned and maintains a register of clones who have
%% requested snapshots of the Ledger
%% - The accepts new dumps (in the form of a leveled_tree accomponied by
%% an array of hash-listing binaries) from the Bookie, and responds either 'ok'
%% to the bookie if the information is accepted nad the Bookie can refresh its
%% memory, or 'returned' if the bookie must continue without refreshing as the
%% Penciller is not currently able to accept the update (potentially due to a
%% backlog of compaction work)
%% - The Penciller's persistence of the ledger may not be reliable, in that it
%% may lose data but only in sequence from a particular sequence number.  On
%% startup the Penciller will inform the Bookie of the highest sequence number
%% it has, and the Bookie should load any missing data from that point out of
%% the journal.
%%
%% -------- LEDGER ---------
%%
%% The Ledger is divided into many levels
%% - L0: New keys are received from the Bookie and and kept in the levelzero
%% cache, until that cache is the size of a SST file, and it is then persisted
%% as a SST file at this level.  L0 SST files can be larger than the normal 
%% maximum size - so we don't have to consider problems of either having more
%% than one L0 file (and handling what happens on a crash between writing the
%% files when the second may have overlapping sequence numbers), or having a
%% remainder with overlapping in sequence numbers in memory after the file is
%% written.   Once the persistence is completed, the L0 cache can be erased.
%% There can be only one SST file at Level 0, so the work to merge that file
%% to the lower level must be the highest priority, as otherwise writes to the
%% ledger will stall, when there is next a need to persist.
%% - L1 TO L7: May contain multiple processes managing non-overlapping SST
%% files.  Compaction work should be sheduled if the number of files exceeds
%% the target size of the level, where the target size is 8 ^ n.
%%
%% The most recent revision of a Key can be found by checking each level until
%% the key is found.  To check a level the correct file must be sought from the
%% manifest for that level, and then a call is made to that file.  If the Key
%% is not present then every level should be checked.
%%
%% If a compaction change takes the size of a level beyond the target size,
%% then compaction work for that level + 1 should be added to the compaction
%% work queue.
%% Compaction work is fetched by the Penciller's Clerk because:
%% - it has timed out due to a period of inactivity
%% - it has been triggered by the a cast to indicate the arrival of high
%% priority compaction work
%% The Penciller's Clerk (which performs compaction worker) will always call
%% the Penciller to find out the highest priority work currently required
%% whenever it has either completed work, or a timeout has occurred since it
%% was informed there was no work to do.
%%
%% When the clerk picks work it will take the current manifest, and the
%% Penciller assumes the manifest sequence number is to be incremented.
%% When the clerk has completed the work it can request that the manifest
%% change be committed by the Penciller.  The commit is made through changing
%% the filename of the new manifest - so the Penciller is not held up by the
%% process of wiritng a file, just altering file system metadata.
%%
%% ---------- PUSH ----------
%%
%% The Penciller must support the PUSH of a dump of keys from the Bookie.  The
%% call to PUSH should be immediately acknowledged, and then work should be
%% completed to merge the cache update into the L0 cache.
%%
%% The Penciller MUST NOT accept a new PUSH if the Clerk has commenced the
%% conversion of the current L0 cache into a SST file, but not completed this
%% change.  The Penciller in this case returns the push, and the Bookie should
%% continue to grow the cache before trying again.
%%
%% ---------- FETCH ----------
%%
%% On request to fetch a key the Penciller should look first in the in-memory
%% L0 tree, then look in the SST files Level by Level (including level 0),
%% consulting the Manifest to determine which file should be checked at each
%% level.
%%
%% ---------- SNAPSHOT ----------
%%
%% Iterators may request a snapshot of the database.  A snapshot is a cloned
%% Penciller seeded not from disk, but by the in-memory L0 gb_tree and the
%% in-memory manifest, allowing for direct reference for the SST file processes.
%%
%% Clones formed to support snapshots are registered by the Penciller, so that
%% SST files valid at the point of the snapshot until either the iterator is
%% completed or has timed out.
%%
%% ---------- ON STARTUP ----------
%%
%% On Startup the Bookie with ask the Penciller to initiate the Ledger first.
%% To initiate the Ledger the must consult the manifest, and then start a SST
%% management process for each file in the manifest.
%%
%% The penciller should then try and read any Level 0 file which has the
%% manifest sequence number one higher than the last store in the manifest.
%%
%% The Bookie will ask the Inker for any Keys seen beyond that sequence number
%% before the startup of the overall store can be completed.
%%
%% ---------- ON SHUTDOWN ----------
%%
%% On a controlled shutdown the Penciller should attempt to write any in-memory
%% ETS table to a L0 SST file, assuming one is nto already pending.  If one is
%% already pending then the Penciller will not persist this part of the Ledger.
%%
%% ---------- FOLDER STRUCTURE ----------
%%
%% The following folders are used by the Penciller
%% $ROOT/ledger/ledger_manifest/ - used for keeping manifest files
%% $ROOT/ledger/ledger_files/ - containing individual SST files
%%
%% In larger stores there could be a large number of files in the ledger_file
%% folder - perhaps o(1000).  It is assumed that modern file systems should
%% handle this efficiently.
%%
%% ---------- COMPACTION & MANIFEST UPDATES ----------
%%
%% The Penciller can have one and only one Clerk for performing compaction
%% work.  When the Clerk has requested and taken work, it should perform the
%5 compaction work starting the new SST process to manage the new Ledger state
%% and then write a new manifest file that represents that state with using
%% the next Manifest sequence number as the filename:
%% - nonzero_<ManifestSQN#>.pnd
%% 
%% The Penciller on accepting the change should rename the manifest file to -
%% - nonzero_<ManifestSQN#>.crr
%%
%% On startup, the Penciller should look for the nonzero_*.crr file with the
%% highest such manifest sequence number.  This will be started as the
%% manifest, together with any _0_0.sst file found at that Manifest SQN.
%% Level zero files are not kept in the persisted manifest, and adding a L0
%% file does not advanced the Manifest SQN.
%%
%% The pace at which the store can accept updates will be dependent on the
%% speed at which the Penciller's Clerk can merge files at lower levels plus
%% the time it takes to merge from Level 0.  As if a clerk has commenced
%% compaction work at a lower level and then immediately a L0 SST file is
%% written the Penciller will need to wait for this compaction work to
%% complete and the L0 file to be compacted before the ETS table can be
%% allowed to again reach capacity
%%
%% The writing of L0 files do not require the involvement of the clerk.
%% The L0 files are prompted directly by the penciller when the in-memory tree
%% has reached capacity.  This places the penciller in a levelzero_pending
%% state, and in this state it must return new pushes.  Once the SST file has
%% been completed it will confirm completion to the penciller which can then
%% revert the levelzero_pending state, add the file to the manifest and clear
%% the current level zero in-memory view.
%%


-module(leveled_penciller).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        format_status/2]).

-export([
        pcl_snapstart/1,
        pcl_start/1,
        pcl_pushmem/2,
        pcl_fetchlevelzero/3,
        pcl_fetch/4,
        pcl_fetchkeys/5,
        pcl_fetchkeys/6,
        pcl_fetchkeysbysegment/8,
        pcl_fetchnextkey/5,
        pcl_checksequencenumber/3,
        pcl_workforclerk/1,
        pcl_manifestchange/2,
        pcl_confirml0complete/5,
        pcl_confirmdelete/3,
        pcl_close/1,
        pcl_doom/1,
        pcl_releasesnapshot/2,
        pcl_registersnapshot/5,
        pcl_getstartupsequencenumber/1,
        pcl_checkbloomtest/2,
        pcl_checkforwork/1,
        pcl_persistedsqn/1,
        pcl_loglevel/2,
        pcl_addlogs/2,
        pcl_removelogs/2]).

-export([
        sst_rootpath/1,
        sst_filename/3]).

-export([pcl_getsstpids/1, pcl_getclerkpid/1]).

-ifdef(TEST).
-export([clean_testdir/1]).
-endif.

-define(MAX_WORK_WAIT, 300).
-define(MANIFEST_FP, "ledger_manifest").
-define(FILES_FP, "ledger_files").
-define(CURRENT_FILEX, "crr").
-define(PENDING_FILEX, "pnd").
-define(SST_FILEX, ".sst").
-define(ARCHIVE_FILEX, ".bak").
-define(SUPER_MAX_TABLE_SIZE, 40000).
-define(PROMPT_WAIT_ONL0, 5).
-define(WORKQUEUE_BACKLOG_TOLERANCE, 4).
-define(COIN_SIDECOUNT, 4).
-define(SLOW_FETCH, 500000). % Log a very slow fetch - longer than 500ms
-define(FOLD_SCANWIDTH, 32).
-define(ITERATOR_SCANWIDTH, 4).
-define(ITERATOR_MINSCANWIDTH, 1).
-define(TIMING_SAMPLECOUNTDOWN, 10000).
-define(TIMING_SAMPLESIZE, 100).
-define(SHUTDOWN_LOOPS, 10).
-define(SHUTDOWN_PAUSE, 10000).
    % How long to wait for snapshots to be released on shutdown
    % before forcing closure of snapshots
    % 10s may not be long enough for all snapshots, but avoids crashes of
    % short-lived queries racing with the shutdown

-record(state, {manifest ::
                    leveled_pmanifest:manifest() | undefined | redacted,
                query_manifest :: 
                    {list(),
                        leveled_codec:ledger_key(),
                        leveled_codec:ledger_key()} | undefined,
                    % Slimmed down version of the manifest containing part
                    % related to  specific query, and the StartKey/EndKey
                    % used to extract this part

                persisted_sqn = 0 :: integer(), % The highest SQN persisted          
                ledger_sqn = 0 :: integer(), % The highest SQN added to L0
                
                levelzero_pending = false :: boolean(),
                levelzero_constructor :: pid() | undefined,
                levelzero_cache = [] :: levelzero_cache() | redacted,
                levelzero_size = 0 :: integer(),
                levelzero_maxcachesize :: integer() | undefined,
                levelzero_cointoss = false :: boolean(),
                levelzero_index ::
                    leveled_pmem:index_array() | undefined | redacted,
                levelzero_astree :: list() | undefined | redacted,

                root_path = "test" :: string(),
                clerk :: pid() | undefined,
                
                is_snapshot = false :: boolean(),
                snapshot_fully_loaded = false :: boolean(),
                snapshot_time :: pos_integer() | undefined,
                source_penciller :: pid() | undefined,
		        bookie_monref :: reference() | undefined,
                
                work_ongoing = false :: boolean(), % i.e. compaction work
                work_backlog = false :: boolean(), % i.e. compaction work

                pending_removals = [] :: list(string()),
                maybe_release = false :: boolean(),
                
                snaptimeout_short :: pos_integer()|undefined,
                snaptimeout_long :: pos_integer()|undefined,

                monitor = {no_monitor, 0} :: leveled_monitor:monitor(),

                sst_options = #sst_options{} :: sst_options(),
            
                shutdown_loops = ?SHUTDOWN_LOOPS :: non_neg_integer()
            }).


-type penciller_options() :: #penciller_options{}.
-type bookies_memory() :: {tuple()|empty_cache,
                            array:array()|empty_array,
                            integer()|infinity,
                            integer()}.
-type pcl_state() :: #state{}.
-type levelzero_cacheentry() :: {pos_integer(), leveled_tree:leveled_tree()}.
-type levelzero_cache() :: list(levelzero_cacheentry()).
-type bad_ledgerkey() :: list().
-type sqn_check() :: current|replaced|missing.
-type sst_fetchfun() ::
        fun((pid(),
                leveled_codec:ledger_key(),
                leveled_codec:segment_hash(),
                non_neg_integer()) -> 
                    leveled_codec:ledger_kv()|not_present).
-type levelzero_returnfun() :: fun((levelzero_cacheentry()) -> ok).
-type pclacc_fun() ::
        fun((leveled_codec:ledger_key(),
                leveled_codec:ledger_value(),
                term()) -> term()).
-type sst_options() :: #sst_options{}.

-export_type(
    [levelzero_cacheentry/0,
        levelzero_returnfun/0,
        sqn_check/0,
        pclacc_fun/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec pcl_start(penciller_options()) -> {ok, pid()}.
%% @doc
%% Start a penciller using a penciller options record.  The start_snapshot
%% option should be used if this is to be a clone of an existing penciller,
%% otherwise the penciller will look in root path for a manifest and
%% associated sst files to start-up from a previous persisted state.
%%
%% When starting a clone a query can also be passed.  This prevents the whole
%% Level Zero memory space from being copied to the snapshot, instead the
%% query is run against the level zero space and just the query results are
%% copied into the clone.  
pcl_start(PCLopts) ->
    gen_server:start_link(?MODULE, [leveled_log:get_opts(), PCLopts], []).

-spec pcl_snapstart(penciller_options()) -> {ok, pid()}.
%% @doc
%% Don't link to the bookie - this is a snpashot
pcl_snapstart(PCLopts) ->
    gen_server:start(?MODULE, [leveled_log:get_opts(), PCLopts], []).

-spec pcl_pushmem(pid(), bookies_memory()) -> ok|returned.
%% @doc
%% Load the contents of the Bookie's memory of recent additions to the Ledger
%% to the Ledger proper.
%%
%% The load is made up of a cache in the form of a leveled_skiplist tuple (or
%% the atom empty_cache if no cache is present), an index of entries in the
%% skiplist in the form of leveled_pmem index (or empty_index), the minimum
%% sequence number in the cache and the maximum sequence number.
%%
%% If the penciller does not have capacity for the pushed cache it will
%% respond with the atom 'returned'.  This is a signal to hold the memory
%% at the Bookie, and try again soon.  This normally only occurs when there
%% is a backlog of merges - so the bookie should backoff for longer each time.
pcl_pushmem(Pid, LedgerCache) ->
    %% Bookie to dump memory onto penciller
    gen_server:call(Pid, {push_mem, LedgerCache}, infinity).

-spec pcl_fetchlevelzero(pid(),
                            non_neg_integer(),
                            fun((levelzero_cacheentry()) -> ok))
                        -> ok.
%% @doc
%% Allows a single slot of the penciller's levelzero cache to be fetched.  The
%% levelzero cache can be up to 40K keys - sending this to the process that is
%% persisting this in a SST file in a single cast will lock the process for
%% 30-40ms.  This allows that process to fetch this slot by slot, so that
%% this is split into a series of smaller events.
%%
%% The return value will be a leveled_skiplist that forms that part of the
%% cache
pcl_fetchlevelzero(Pid, Slot, ReturnFun) ->
    % Timeout to cause crash of L0 file when it can't get the close signal
    % as it is deadlocked making this call.
    %
    % If the timeout gets hit outside of close scenario the Penciller will
    % be stuck in L0 pending
    gen_server:cast(Pid, {fetch_levelzero, Slot, ReturnFun}).

-spec pcl_fetch(pid(), 
                leveled_codec:ledger_key(), 
                leveled_codec:segment_hash(),
                boolean()) -> leveled_codec:ledger_kv()|not_present.
%% @doc
%% Fetch a key, return the first (highest SQN) occurrence of that Key along
%% with  the value.
%%
%% Hash should be result of leveled_codec:segment_hash(Key)
pcl_fetch(Pid, Key, Hash, UseL0Index) ->
    gen_server:call(Pid, {fetch, Key, Hash, UseL0Index}, infinity).

-spec pcl_fetchkeys(pid(), 
                    leveled_codec:ledger_key(), 
                    leveled_codec:ledger_key(), 
                    pclacc_fun(), any(), as_pcl|by_runner) -> any().
%% @doc
%% Run a range query between StartKey and EndKey (inclusive).  This will cover
%% all keys in the range - so must only be run against snapshots of the
%% penciller to avoid blocking behaviour.
%%
%% Comparison with the upper-end of the range (EndKey) is done using
%% leveled_codec:endkey_passed/2 - so use nulls within the tuple to manage
%% the top of the range.  Comparison with the start of the range is based on
%% Erlang term order.
pcl_fetchkeys(Pid, StartKey, EndKey, AccFun, InitAcc) ->
    pcl_fetchkeys(Pid, StartKey, EndKey, AccFun, InitAcc, as_pcl).

pcl_fetchkeys(Pid, StartKey, EndKey, AccFun, InitAcc, By) ->
    gen_server:call(Pid,
                    {fetch_keys, 
                        StartKey, EndKey, 
                        AccFun, InitAcc, 
                        false, false, -1,
                        By},
                    infinity).


-spec pcl_fetchkeysbysegment(pid(), 
                                leveled_codec:ledger_key(), 
                                leveled_codec:ledger_key(), 
                                pclacc_fun(), any(), 
                                leveled_codec:segment_list(),
                                false | leveled_codec:lastmod_range(),
                                boolean()) -> any().
%% @doc
%% Run a range query between StartKey and EndKey (inclusive).  This will cover
%% all keys in the range - so must only be run against snapshots of the
%% penciller to avoid blocking behaviour.  
%%
%% This version allows an additional input of a SegChecker.  This is a list 
%% of 16-bit integers representing the segment IDs  band ((2 ^ 16) -1) that
%% are interesting to the fetch
%%
%% Note that segment must be false unless the object Tag supports additional
%% indexing by segment.  This cannot be used on ?IDX_TAG and other tags that
%% use the no_lookup hash
pcl_fetchkeysbysegment(Pid, StartKey, EndKey, AccFun, InitAcc,
                                SegmentList, LastModRange, LimitByCount) ->
    {MaxKeys, InitAcc0} = 
        case LimitByCount of
            true ->
                % The passed in accumulator should have the Max Key Count
                % as the first element of a tuple with the actual accumulator
                InitAcc;
            false ->
                {-1, InitAcc}
        end,
    gen_server:call(Pid,
                    {fetch_keys, 
                        StartKey, EndKey, AccFun, InitAcc0, 
                        SegmentList, LastModRange, MaxKeys, 
                        by_runner},
                    infinity).

-spec pcl_fetchnextkey(pid(), 
                        leveled_codec:ledger_key(), 
                        leveled_codec:ledger_key(), 
                        pclacc_fun(), any()) -> any().
%% @doc
%% Run a range query between StartKey and EndKey (inclusive).  This has the
%% same constraints as pcl_fetchkeys/5, but will only return the first key
%% found in erlang term order.
pcl_fetchnextkey(Pid, StartKey, EndKey, AccFun, InitAcc) ->
    gen_server:call(Pid,
                    {fetch_keys, 
                        StartKey, EndKey, 
                        AccFun, InitAcc, 
                        false, false, 1,
                        as_pcl},
                    infinity).

-spec pcl_checksequencenumber(pid(), 
                                leveled_codec:ledger_key()|bad_ledgerkey(), 
                                integer()) -> sqn_check().
%% @doc
%% Check if the sequence number of the passed key is not replaced by a change
%% after the passed sequence number.  Will return:
%% - current
%% - replaced
%% - missing
pcl_checksequencenumber(Pid, Key, SQN) ->
    Hash = leveled_codec:segment_hash(Key),
    if
        Hash /= no_lookup ->
            gen_server:call(Pid, {check_sqn, Key, Hash, SQN}, infinity)
    end.

-spec pcl_workforclerk(pid()) -> ok.
%% @doc
%% A request from the clerk to check for work.  If work is present the
%% Penciller will cast back to the clerk, no response is sent to this
%% request.
pcl_workforclerk(Pid) ->
    gen_server:cast(Pid, work_for_clerk).

-spec pcl_manifestchange(pid(), leveled_pmanifest:manifest()) -> ok.
%% @doc
%% Provide a manifest record (i.e. the output of the leveled_pmanifest module)
%% that is required to become the new manifest.
pcl_manifestchange(Pid, Manifest) ->
    gen_server:cast(Pid, {manifest_change, Manifest}).

-spec pcl_confirml0complete(pid(), 
                            string(), 
                            leveled_codec:ledger_key(), 
                            leveled_codec:ledger_key(), 
                            binary()) -> ok.
%% @doc
%% Allows a SST writer that has written a L0 file to confirm that the file 
%% is now complete, so the filename and key ranges can be added to the 
%% manifest and the file can be used in place of the in-memory levelzero
%% cache.
pcl_confirml0complete(Pid, FN, StartKey, EndKey, Bloom) ->
    gen_server:cast(Pid, {levelzero_complete, FN, StartKey, EndKey, Bloom}).

-spec pcl_confirmdelete(pid(), string(), pid()) -> ok.
%% @doc
%% Poll from a delete_pending file requesting a message if the file is now
%% ready for deletion (i.e. all snapshots which depend on the file have
%% finished)
pcl_confirmdelete(Pid, FileName, FilePid) ->
    gen_server:cast(Pid, {confirm_delete, FileName, FilePid}).

-spec pcl_getstartupsequencenumber(pid()) -> integer().
%% @doc
%% At startup the penciller will get the largest sequence number that is 
%% within the persisted files.  This function allows for this sequence number
%% to be fetched - so that it can be used to determine parts of the Ledger
%% which  may have been lost in the last shutdown (so that the ledger can
%% be reloaded from that point in the Journal)
pcl_getstartupsequencenumber(Pid) ->
    gen_server:call(Pid, get_startup_sqn, infinity).

-spec pcl_registersnapshot(pid(),
                            pid(),
                            no_lookup|{tuple(), tuple()}|undefined,
                            bookies_memory(),
                            boolean())
                                -> {ok, pcl_state()}.
%% @doc
%% Register a snapshot of the penciller, returning a state record from the
%% penciller for the snapshot to use as its LoopData
pcl_registersnapshot(Pid, Snapshot, Query, BookiesMem, LR) ->
    gen_server:call(Pid,
                    {register_snapshot, Snapshot, Query, BookiesMem, LR},
                    infinity).

-spec pcl_releasesnapshot(pid(), pid()) -> ok.
%% @doc
%% Inform the primary penciller that a snapshot is finished, so that the 
%% penciller can allow deletes to proceed if appropriate.
pcl_releasesnapshot(Pid, Snapshot) ->
    gen_server:cast(Pid, {release_snapshot, Snapshot}).


-spec pcl_persistedsqn(pid()) -> integer().
%% @doc
%% Return the persisted SQN, the highest SQN which has been persisted into the 
%% Ledger
pcl_persistedsqn(Pid) ->
    gen_server:call(Pid, persisted_sqn, infinity).

-spec pcl_close(pid()) -> ok.
%% @doc
%% Close the penciller neatly, trying to persist to disk anything in the memory
pcl_close(Pid) ->
    gen_server:call(Pid, close, infinity).

-spec pcl_snapclose(pid()) -> ok.
%% @doc
%% Specifically to be used when closing snpashots on shutdown, will handle a
%% scenario where a snapshot has already exited
pcl_snapclose(Pid) ->
    try
        pcl_close(Pid)
    catch
        exit:{noproc, _CallDetails} ->
            ok
    end.

-spec pcl_doom(pid()) -> {ok, list()}.
%% @doc
%% Close the penciller neatly, trying to persist to disk anything in the memory
%% Return a list of filepaths from where files exist for this penciller (should
%% the calling process which to erase the store).
pcl_doom(Pid) ->
    gen_server:call(Pid, doom, infinity).

-spec pcl_checkbloomtest(pid(), tuple()) -> boolean().
%% @doc
%% Function specifically added to help testing.  In particular to make sure 
%% that blooms are still available after pencllers have been re-loaded from
%% disk.
pcl_checkbloomtest(Pid, Key) ->
    Hash = leveled_codec:segment_hash(Key),
    if
        Hash /= no_lookup ->
            gen_server:call(Pid, {checkbloom_fortest, Key, Hash}, 2000)
    end.

-spec pcl_checkforwork(pid()) -> boolean().
%% @doc
%% Used in test only to confim compaction work complete before closing
pcl_checkforwork(Pid) ->
    gen_server:call(Pid, check_for_work, 2000).

-spec pcl_loglevel(pid(), leveled_log:log_level()) -> ok.
%% @doc
%% Change the log level of the Journal
pcl_loglevel(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec pcl_addlogs(pid(), list(string())) -> ok.
%% @doc
%% Add to the list of forced logs, a list of more forced logs
pcl_addlogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {add_logs, ForcedLogs}).

-spec pcl_removelogs(pid(), list(string())) -> ok.
%% @doc
%% Remove from the list of forced logs, a list of forced logs
pcl_removelogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {remove_logs, ForcedLogs}).

-spec pcl_getsstpids(pid()) -> list(pid()).
%% @doc
%% Used for profiling in tests - get a list of SST PIDs to profile
pcl_getsstpids(Pid) ->
    gen_server:call(Pid, get_sstpids).

-spec pcl_getclerkpid(pid()) -> pid().
%% @doc
%% Used for profiling in tests - get the clerk PID to profile
pcl_getclerkpid(Pid) ->
    gen_server:call(Pid, get_clerkpid).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([LogOpts, PCLopts]) ->
    leveled_log:save(LogOpts),
    leveled_rand:seed(),
    case {PCLopts#penciller_options.root_path,
            PCLopts#penciller_options.start_snapshot,
            PCLopts#penciller_options.snapshot_query,
            PCLopts#penciller_options.bookies_mem} of
        {undefined, _Snapshot=true, Query, BookiesMem} ->
            SrcPenciller = PCLopts#penciller_options.source_penciller,
            LongRunning = PCLopts#penciller_options.snapshot_longrunning,
            %% monitor the bookie, and close the snapshot when bookie
            %% exits
	        BookieMonitor = 
                erlang:monitor(process, PCLopts#penciller_options.bookies_pid),
            {ok, State} =
                pcl_registersnapshot(
                    SrcPenciller, self(), Query, BookiesMem, LongRunning),
            leveled_log:log(p0001, [self()]),
            {ok,
                State#state{
                    is_snapshot = true,
                    clerk = undefined,
                    bookie_monref = BookieMonitor,
                    source_penciller = SrcPenciller}};
        {_RootPath, _Snapshot=false, _Q, _BM} ->
            start_from_file(PCLopts)
    end.    
    

handle_call({push_mem, {LedgerTable, PushedIdx, MinSQN, MaxSQN}},
                _From,
                State=#state{is_snapshot=Snap}) when Snap == false ->
    % The push_mem process is as follows:
    %
    % 1. If either the penciller is still waiting on the last L0 file to be
    % written, or there is a work backlog - the cache is returned with the
    % expectation that PUTs should be slowed.  Also if the cache has reached
    % the maximum number of lines (by default after 31 pushes from the bookie)
    % 
    % 2. If (1) does not apply, the bookie's cache will be added to the
    % penciller's cache.
    SW = os:timestamp(),

    L0Pending = State#state.levelzero_pending,
    WorkBacklog = State#state.work_backlog,
    CacheAlreadyFull = leveled_pmem:cache_full(State#state.levelzero_cache),
    L0Size = State#state.levelzero_size,

    % The clerk is prompted into action as there may be a L0 write required
    ok = leveled_pclerk:clerk_prompt(State#state.clerk),

    case L0Pending or WorkBacklog or CacheAlreadyFull of
        true ->
            % Cannot update the cache, or roll the memory so reply `returned`
            % The Bookie must now retain the lesger cache and try to push the
            % updated cache at a later time
            leveled_log:log(
                p0018,
                [L0Size, L0Pending, WorkBacklog, CacheAlreadyFull]),
            {reply, returned, State};
        false ->
            % Return ok as cache has been updated on State and the Bookie
            % should clear its ledger cache which is now with the Penciller
            PushedTree =
                case is_tuple(LedgerTable) of
                    true ->
                        LedgerTable;
                    false ->
                        leveled_tree:from_orderedset(LedgerTable, ?CACHE_TYPE)
                end,
            case leveled_pmem:add_to_cache(
                    L0Size,
                    {PushedTree, MinSQN, MaxSQN},
                    State#state.ledger_sqn,
                    State#state.levelzero_cache,
                    true) of
                empty_push ->
                    {reply, ok, State};
                {UpdMaxSQN, NewL0Size, UpdL0Cache} ->
                    UpdL0Index =
                        leveled_pmem:add_to_index(
                            PushedIdx,
                            State#state.levelzero_index,
                            length(State#state.levelzero_cache) + 1),
                    leveled_log:log_randomtimer(
                        p0031,
                        [NewL0Size, true, true, MinSQN, MaxSQN], SW, 0.1),
                    {reply,
                        ok,
                        State#state{
                            levelzero_cache = UpdL0Cache,
                            levelzero_size = NewL0Size,
                            levelzero_index = UpdL0Index,
                            ledger_sqn = UpdMaxSQN}}
            end
    end;
handle_call({fetch, Key, Hash, UseL0Index}, _From, State) ->
    L0Idx = 
        case UseL0Index of 
            true ->
                State#state.levelzero_index;
            false ->
                none
        end,
    R = 
        timed_fetch_mem(
            Key, Hash, State#state.manifest,
            State#state.levelzero_cache, L0Idx,
            State#state.monitor),
    {reply, R, State};
handle_call({check_sqn, Key, Hash, SQN}, _From, State) ->
    {reply,
        compare_to_sqn(
            fetch_sqn(
                Key,
                Hash,
                State#state.manifest,
                State#state.levelzero_cache,
                State#state.levelzero_index),
                SQN),
        State};
handle_call({fetch_keys, 
                    StartKey, EndKey, 
                    AccFun, InitAcc, 
                    SegmentList, LastModRange, MaxKeys, By},
                _From,
                State=#state{snapshot_fully_loaded=Ready})
                                                        when Ready == true ->
    LastModRange0 =
        case LastModRange of
            false ->
                ?OPEN_LASTMOD_RANGE;
            R ->
                R
        end,
    SW = os:timestamp(),
    L0AsList =
        case State#state.levelzero_astree of
            undefined ->
                leveled_pmem:merge_trees(
                    StartKey,
                    EndKey,
                    State#state.levelzero_cache,
                    leveled_tree:empty(?CACHE_TYPE));
            List ->
                List
        end,
    SegChecker = 
        leveled_sst:segment_checker(leveled_sst:tune_seglist(SegmentList)),
    FilteredL0 = 
        case SegChecker of
            false ->
                L0AsList;
            {Min, Max, CheckFun} ->
                FilterFun =
                    fun(LKV) ->
                        CheckSeg = 
                            leveled_sst:extract_hash(
                                leveled_codec:strip_to_segmentonly(LKV)),
                        case CheckSeg of
                            CheckSeg when CheckSeg >= Min, CheckSeg =< Max ->
                                CheckFun(CheckSeg);
                            _ ->
                                false
                        end
                    end,
                lists:filter(FilterFun, L0AsList)
        end,
    
    leveled_log:log_randomtimer(
        p0037, [State#state.levelzero_size], SW, 0.01),
    
    %% Rename any reference to loop state that may be used by the function
    %% to be returned - https://github.com/martinsumner/leveled/issues/326
    SSTiter =
        case State#state.query_manifest of
            undefined ->
                leveled_pmanifest:query_manifest(
                    State#state.manifest, StartKey, EndKey);
            {QueryManifest, StartKeyQM, EndKeyQM}
                    when StartKey >= StartKeyQM, EndKey =< EndKeyQM ->
                QueryManifest
        end,    
    SnapshotTime = State#state.snapshot_time,
    PersistedIterator = maps:from_list(SSTiter),
    Folder = 
        fun() -> 
            keyfolder(
                maps:put(-1, FilteredL0, PersistedIterator),
                {StartKey, EndKey},
                {AccFun, InitAcc, SnapshotTime},
                {SegChecker, LastModRange0, MaxKeys})
        end,
    case By of 
        as_pcl ->
            {reply, Folder(), State};
        by_runner ->
            {reply, Folder, State}
    end;
handle_call(get_startup_sqn, _From, State) ->
    {reply, State#state.persisted_sqn, State};
handle_call({register_snapshot, Snapshot, Query, BookiesMem, LongRunning},
                                                            _From, State) ->
    % Register and load a snapshot
    %
    % For setup of the snapshot to be efficient should pass a query
    % of (StartKey, EndKey) - this will avoid a fully copy of the penciller's
    % memory being required to be trasnferred to the clone.  However, this
    % will not be a valid clone for fetch
    
    TimeO = 
        case LongRunning of
            true ->
                State#state.snaptimeout_long;
            false ->
                State#state.snaptimeout_short
        end,
    Manifest0 =
        leveled_pmanifest:add_snapshot(State#state.manifest, Snapshot, TimeO),

    {BookieIncrTree, BookieIdx, MinSQN, MaxSQN} = BookiesMem,
    LM1Cache =
        case BookieIncrTree of
            empty_cache ->
                leveled_tree:empty(?CACHE_TYPE);
            _ ->
                BookieIncrTree
        end,

    {CloneState, ManifestClone, QueryManifest} = 
        case Query of
            no_lookup ->
                {UpdMaxSQN, UpdSize, L0Cache} =
                    leveled_pmem:add_to_cache(
                        State#state.levelzero_size,
                        {LM1Cache, MinSQN, MaxSQN},
                        State#state.ledger_sqn,
                        State#state.levelzero_cache,
                        false),
                {#state{levelzero_cache = L0Cache,
                        ledger_sqn = UpdMaxSQN,
                        levelzero_size = UpdSize,
                        persisted_sqn = State#state.persisted_sqn},
                    leveled_pmanifest:copy_manifest(State#state.manifest),
                    undefined};
            {StartKey, EndKey} ->
                SW = os:timestamp(),
                L0AsTree =
                    leveled_pmem:merge_trees(StartKey,
                                                EndKey,
                                                State#state.levelzero_cache,
                                                LM1Cache),
                leveled_log:log_randomtimer(
                    p0037, [State#state.levelzero_size], SW, 0.01),
                {#state{levelzero_astree = L0AsTree,
                        ledger_sqn = MaxSQN,
                        persisted_sqn = State#state.persisted_sqn},
                    undefined,
                    {leveled_pmanifest:query_manifest(
                        State#state.manifest, StartKey, EndKey),
                        StartKey,
                        EndKey}};
            undefined ->
                {UpdMaxSQN, UpdSize, L0Cache} =
                    leveled_pmem:add_to_cache(
                        State#state.levelzero_size,
                        {LM1Cache, MinSQN, MaxSQN},
                        State#state.ledger_sqn,
                        State#state.levelzero_cache,
                        false),
                LM1Idx =
                    case BookieIdx of
                        empty_index ->
                            leveled_pmem:new_index();
                        _ ->
                            BookieIdx
                    end,
                L0Index =
                    leveled_pmem:add_to_index(
                        LM1Idx, State#state.levelzero_index, length(L0Cache)),
                {#state{levelzero_cache = L0Cache,
                        levelzero_index = L0Index,
                        levelzero_size = UpdSize,
                        ledger_sqn = UpdMaxSQN,
                        persisted_sqn = State#state.persisted_sqn},
                    leveled_pmanifest:copy_manifest(State#state.manifest),
                    undefined}
        end,
    {reply,
        {ok,
            CloneState#state{snapshot_fully_loaded = true,
                                snapshot_time = leveled_util:integer_now(),
                                manifest = ManifestClone,
                                query_manifest = QueryManifest}},
        State#state{manifest = Manifest0}};
handle_call(close, _From, State=#state{is_snapshot=Snap}) when Snap == true ->
    ok = pcl_releasesnapshot(State#state.source_penciller, self()),
    {stop, normal, ok, State};
handle_call(close, From, State) ->
    % Level 0 files lie outside of the manifest, and so if there is no L0
    % file present it is safe to write the current contents of memory.  If
    % there is a L0 file present - then the memory can be dropped (it is
    % recoverable from the ledger, and there should not be a lot to recover
    % as presumably the ETS file has been recently flushed, hence the presence
    % of a L0 file).
    %
    % The penciller should close each file in the manifest, and call a close
    % on the clerk.
    ok = leveled_pclerk:clerk_close(State#state.clerk),
    leveled_log:log(p0008, [close]),
    L0Left = State#state.levelzero_size > 0,
    case (not State#state.levelzero_pending and L0Left) of
        true ->
            Man0 = State#state.manifest,
            {Constructor, _} =
                roll_memory(
                    leveled_pmanifest:get_manifest_sqn(Man0) + 1,
                    State#state.ledger_sqn,
                    State#state.root_path,
                    State#state.levelzero_cache,
                    length(State#state.levelzero_cache),
                    State#state.sst_options,
                    true),
            ok = leveled_sst:sst_close(Constructor);
        false ->
            leveled_log:log(p0010, [State#state.levelzero_size])
    end,
    gen_server:cast(self(), {maybe_defer_shutdown, close, From}),
    {noreply, State};
handle_call(doom, From, State) ->
    leveled_log:log(p0030, []),
    ok = leveled_pclerk:clerk_close(State#state.clerk),
    gen_server:cast(self(), {maybe_defer_shutdown, doom, From}),
    {noreply, State};
handle_call({checkbloom_fortest, Key, Hash}, _From, State) ->
    Manifest = State#state.manifest,
    FoldFun = 
        fun(Level, Acc) ->
            case Acc of 
                true ->
                    true;
                false ->
                    case leveled_pmanifest:key_lookup(Manifest, Level, Key) of
                        false ->
                            false;
                        FP ->
                            leveled_pmanifest:check_bloom(Manifest, FP, Hash)
                    end
            end
        end,
    {reply, lists:foldl(FoldFun, false, lists:seq(0, ?MAX_LEVELS)), State};
handle_call(check_for_work, _From, State) ->
    {_WL, WC} = leveled_pmanifest:check_for_work(State#state.manifest),
    {reply, WC > 0, State};
handle_call(persisted_sqn, _From, State) ->
    {reply, State#state.persisted_sqn, State};
handle_call(get_sstpids, _From, State) ->
    {reply, leveled_pmanifest:get_sstpids(State#state.manifest), State};
handle_call(get_clerkpid, _From, State) ->
    {reply, State#state.clerk, State}.

handle_cast({manifest_change, Manifest}, State) ->
    NewManSQN = leveled_pmanifest:get_manifest_sqn(Manifest),
    OldManSQN = leveled_pmanifest:get_manifest_sqn(State#state.manifest),
    leveled_log:log(p0041, [OldManSQN, NewManSQN]),
    % Only safe to update the manifest if the SQN increments
    if NewManSQN > OldManSQN ->
        ok =
            leveled_pclerk:clerk_promptdeletions(State#state.clerk, NewManSQN),
            % This is accepted as the new manifest, files may be deleted
        UpdManifest0 =
            leveled_pmanifest:merge_snapshot(State#state.manifest, Manifest),
            % Need to preserve the penciller's view of snapshots stored in
            % the manifest
        UpdManifest1 =
            leveled_pmanifest:clear_pending(
                UpdManifest0,
                lists:usort(State#state.pending_removals),
                State#state.maybe_release),
        {noreply,
            State#state{
                manifest=UpdManifest1,
                pending_removals = [],
                maybe_release = false,
                work_ongoing=false}}
    end;
handle_cast({release_snapshot, Snapshot}, State) ->
    Manifest0 =
        leveled_pmanifest:release_snapshot(State#state.manifest, Snapshot),
    leveled_log:log(p0003, [Snapshot]),
    {noreply, State#state{manifest=Manifest0}};
handle_cast({confirm_delete, PDFN, FilePid}, State=#state{is_snapshot=Snap})
                                                        when Snap == false ->
    % This is a two stage process.  A file that is ready for deletion can be
    % checked against the manifest to prompt the deletion, however it must also
    % be removed from the manifest's list of pending deletes.  This is only
    % possible when the manifest is in control of the penciller not the clerk.
    % When work is ongoing (i.e. the manifest is under control of the clerk),
    % any removals from the manifest need to be stored temporarily (in
    % pending_removals) until such time that the manifest is in control of the
    % penciller and can be updated.
    % The maybe_release boolean on state is used if any file is not ready to
    % delete, and there is work ongoing.  This will then trigger a check to
    % ensure any timed out snapshots are released, in case this is the factor
    % blocking the delete confirmation
    % When an updated manifest is submitted by the clerk, the pending_removals
    % will be cleared from pending using the maybe_release boolean
    case leveled_pmanifest:ready_to_delete(State#state.manifest, PDFN) of
        true ->
            leveled_log:log(p0005, [PDFN]),
            ok = leveled_sst:sst_deleteconfirmed(FilePid),
            case State#state.work_ongoing of 
                true ->
                    {noreply,
                        State#state{
                            pending_removals =
                                [PDFN|State#state.pending_removals]}};
                false ->
                    UpdManifest =
                        leveled_pmanifest:clear_pending(
                            State#state.manifest,
                            [PDFN],
                            false),
                    {noreply,
                        State#state{manifest = UpdManifest}}
            end;
        false ->
            case State#state.work_ongoing of
                true ->
                    {noreply, State#state{maybe_release = true}};
                false ->
                    UpdManifest =
                        leveled_pmanifest:clear_pending(
                            State#state.manifest,
                            [],
                            true),
                    {noreply,
                        State#state{manifest = UpdManifest}}
            end
    end;
handle_cast({levelzero_complete, FN, StartKey, EndKey, Bloom}, State) ->
    leveled_log:log(p0029, []),
    ManEntry = #manifest_entry{start_key=StartKey,
                                end_key=EndKey,
                                owner=State#state.levelzero_constructor,
                                filename=FN,
                                bloom=Bloom},
    ManifestSQN = leveled_pmanifest:get_manifest_sqn(State#state.manifest) + 1,
    UpdMan =
        leveled_pmanifest:insert_manifest_entry(
            State#state.manifest, ManifestSQN, 0, ManEntry),
    % Prompt clerk to ask about work - do this for every L0 roll
    ok = leveled_pclerk:clerk_prompt(State#state.clerk),
    {noreply, State#state{levelzero_cache=[],
                            levelzero_index=[],
                            levelzero_pending=false,
                            levelzero_constructor=undefined,
                            levelzero_size=0,
                            manifest=UpdMan,
                            persisted_sqn=State#state.ledger_sqn}};
handle_cast(work_for_clerk, State) ->
    case {(State#state.levelzero_pending or State#state.work_ongoing),
            leveled_pmanifest:levelzero_present(State#state.manifest)} of
        {true, _L0Present} ->
            % Work is blocked by ongoing activity
            {noreply, State};
        {false, true} ->
            % If L0 present, and no work ongoing - dropping L0 to L1 is the
            % priority
            ok = leveled_pclerk:clerk_push(
                State#state.clerk, {0, State#state.manifest}),
            {noreply, State#state{work_ongoing=true}};
        {false, false} ->
            % No impediment to work - see what other work may be required
            % See if the in-memory cache requires rolling now
            CacheOverSize =
                maybe_cache_too_big(
                    State#state.levelzero_size,
                    State#state.levelzero_maxcachesize,
                    State#state.levelzero_cointoss),
            CacheAlreadyFull =
                leveled_pmem:cache_full(State#state.levelzero_cache),
            % Check for a backlog of work
            {WL, WC} = leveled_pmanifest:check_for_work(State#state.manifest),
            case {WC, (CacheAlreadyFull or CacheOverSize)} of
                {0, false} ->
                    % No work required
                    {noreply, State#state{work_backlog = false}};
                {WC, true} when WC < ?WORKQUEUE_BACKLOG_TOLERANCE ->
                    % Rolling the memory to create a new Level Zero file
                    % Must not do this if there is a work backlog beyond the 
                    % tolerance, as then the backlog may never be addressed.
                    NextSQN =
                        leveled_pmanifest:get_manifest_sqn(
                            State#state.manifest) + 1,
                    {Constructor, none} =
                        roll_memory(
                            NextSQN,
                            State#state.ledger_sqn,
                            State#state.root_path,
                            none,
                            length(State#state.levelzero_cache),
                            State#state.sst_options,
                            false),
                    {noreply,
                        State#state{
                            levelzero_pending = true,
                            levelzero_constructor = Constructor,
                            work_backlog = false}}; 
                {WC, L0Full} ->
                    % Address the backlog of work, either because there is no
                    % L0 work to do, or because the backlog has grown beyond
                    % tolerance
                    Backlog = WC >= ?WORKQUEUE_BACKLOG_TOLERANCE,
                    leveled_log:log(p0024, [WC, Backlog, L0Full]),
                    [TL|_Tail] = WL,
                    ok =
                        leveled_pclerk:clerk_push(
                            State#state.clerk, {TL, State#state.manifest}),
                    {noreply,
                        State#state{
                            work_backlog = Backlog, work_ongoing = true}}
            end
    end;
handle_cast({fetch_levelzero, Slot, ReturnFun}, State) ->
    ReturnFun(lists:nth(Slot, State#state.levelzero_cache)),
    {noreply, State};
handle_cast({log_level, LogLevel}, State) ->
    update_clerk(
        State#state.clerk, fun leveled_pclerk:clerk_loglevel/2, LogLevel),
    SSTopts = State#state.sst_options,
    SSTopts0 = SSTopts#sst_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{sst_options = SSTopts0}};
handle_cast({add_logs, ForcedLogs}, State) ->
    update_clerk(
        State#state.clerk, fun leveled_pclerk:clerk_addlogs/2, ForcedLogs),
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    SSTopts = State#state.sst_options,
    SSTopts0 = SSTopts#sst_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{sst_options = SSTopts0}};
handle_cast({remove_logs, ForcedLogs}, State) ->
    update_clerk(
        State#state.clerk, fun leveled_pclerk:clerk_removelogs/2, ForcedLogs),
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    SSTopts = State#state.sst_options,
    SSTopts0 = SSTopts#sst_options{log_options = leveled_log:get_opts()},
    {noreply, State#state{sst_options = SSTopts0}};
handle_cast({maybe_defer_shutdown, ShutdownType, From}, State) ->
    case length(leveled_pmanifest:snapshot_pids(State#state.manifest)) of
        0 ->
            gen_server:cast(self(), {complete_shutdown, ShutdownType, From}),
            {noreply, State};
        N ->
            % Whilst this process sleeps, then any remaining snapshots may
            % release and have their release messages queued before the
            % complete_shutdown cast is sent
            case State#state.shutdown_loops of
                LoopCount when LoopCount > 0 ->
                    leveled_log:log(p0042, [N]),
                    timer:sleep(?SHUTDOWN_PAUSE div ?SHUTDOWN_LOOPS),
                    gen_server:cast(
                        self(), {maybe_defer_shutdown, ShutdownType, From}),
                    {noreply, State#state{shutdown_loops = LoopCount - 1}};
                0 ->
                    gen_server:cast(
                        self(), {complete_shutdown, ShutdownType, From}),
                    {noreply, State}
            end
    end;
handle_cast({complete_shutdown, ShutdownType, From}, State) ->
    lists:foreach(
        fun(Snap) -> ok = pcl_snapclose(Snap) end,
        leveled_pmanifest:snapshot_pids(State#state.manifest)),
    shutdown_manifest(State#state.manifest, State#state.levelzero_constructor),
    case ShutdownType of
        doom ->
            ManifestFP = State#state.root_path ++ "/" ++ ?MANIFEST_FP ++ "/",
            FilesFP = State#state.root_path ++ "/" ++ ?FILES_FP ++ "/",
            gen_server:reply(From, {ok, [ManifestFP, FilesFP]});
        close ->
            gen_server:reply(From, ok)
    end,
    {stop, normal, State}.

%% handle the bookie stopping and stop this snapshot
handle_info({'DOWN', BookieMonRef, process, _BookiePid, _Info},
	    State=#state{bookie_monref = BookieMonRef}) ->
    ok = pcl_releasesnapshot(State#state.source_penciller, self()),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State=#state{is_snapshot=Snap}) when Snap == true ->
    leveled_log:log(p0007, [Reason]);
terminate(Reason, _State) ->
    leveled_log:log(p0011, [Reason]).

format_status(normal, [_PDict, State]) ->
    State;
format_status(terminate, [_PDict, State]) ->
    State#state{
        manifest = redacted, 
        levelzero_cache = redacted,
        levelzero_index = redacted,
        levelzero_astree = redacted}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Path functions
%%%============================================================================

sst_rootpath(RootPath) ->
    FP = RootPath ++ "/" ++ ?FILES_FP,
    filelib:ensure_dir(FP ++ "/"),
    FP.

sst_filename(ManSQN, Level, Count) ->
    lists:flatten(
        io_lib:format("./~w_~w_~w" ++ ?SST_FILEX, [ManSQN, Level, Count])).
    

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec update_clerk(pid()|undefined, fun((pid(), term()) -> ok), term()) -> ok.
update_clerk(undefined, _F, _T) ->
    ok;
update_clerk(Clerk, F, T) when is_pid(Clerk) ->
    F(Clerk, T).

-spec start_from_file(penciller_options()) -> {ok, pcl_state()}.
%% @doc
%% Normal start of a penciller (i.e. not a snapshot), needs to read the 
%% filesystem and reconstruct the ledger from the files that it finds
start_from_file(PCLopts) ->
    RootPath = PCLopts#penciller_options.root_path,
    MaxTableSize = PCLopts#penciller_options.max_inmemory_tablesize,
    OptsSST = PCLopts#penciller_options.sst_options,
    Monitor = PCLopts#penciller_options.monitor,

    SnapTimeoutShort = PCLopts#penciller_options.snaptimeout_short,
    SnapTimeoutLong = PCLopts#penciller_options.snaptimeout_long,
    
    {ok, MergeClerk} = leveled_pclerk:clerk_new(self(), RootPath, OptsSST),
    
    CoinToss = PCLopts#penciller_options.levelzero_cointoss,
    % Used to randomly defer the writing of L0 file.  Intended to help with
    % vnode syncronisation issues (e.g. stop them all by default merging to
    % level zero concurrently)
    
    InitState =
        #state{
            clerk = MergeClerk,
            root_path = RootPath,
            levelzero_maxcachesize = MaxTableSize,
            levelzero_cointoss = CoinToss,
            levelzero_index = [],
            snaptimeout_short = SnapTimeoutShort,
            snaptimeout_long = SnapTimeoutLong,
            sst_options = OptsSST,
            monitor = Monitor},
    
    %% Open manifest
    Manifest0 = leveled_pmanifest:open_manifest(RootPath),
    OpenFun =
        fun(FN, Level) ->
            {ok, Pid, {_FK, _LK}, Bloom} = 
                leveled_sst:sst_open(sst_rootpath(RootPath),
                                        FN, OptsSST, Level),
            {Pid, Bloom}
        end,
    SQNFun = fun leveled_sst:sst_getmaxsequencenumber/1,
    {MaxSQN, Manifest1, FileList} = 
        leveled_pmanifest:load_manifest(Manifest0, OpenFun, SQNFun),
    leveled_log:log(p0014, [MaxSQN]),
    ManSQN = leveled_pmanifest:get_manifest_sqn(Manifest1),
    leveled_log:log(p0035, [ManSQN]),
    %% Find any L0 files
    L0FN = sst_filename(ManSQN + 1, 0, 0),
    {State0, FileList0} = 
        case filelib:is_file(filename:join(sst_rootpath(RootPath), L0FN)) of
            true ->
                leveled_log:log(p0015, [L0FN]),
                L0Open =
                    leveled_sst:sst_open(
                        sst_rootpath(RootPath), L0FN, OptsSST, 0),
                {ok, L0Pid, {L0StartKey, L0EndKey}, Bloom} = L0Open,
                L0SQN = leveled_sst:sst_getmaxsequencenumber(L0Pid),
                L0Entry =
                    #manifest_entry{
                        start_key = L0StartKey,
                        end_key = L0EndKey,
                        filename = L0FN,
                        owner = L0Pid,
                        bloom = Bloom},
                Manifest2 = 
                    leveled_pmanifest:insert_manifest_entry(
                        Manifest1, ManSQN + 1, 0, L0Entry),
                leveled_log:log(p0016, [L0SQN]),
                LedgerSQN = max(MaxSQN, L0SQN),
                {InitState#state{
                        manifest = Manifest2,
                        ledger_sqn = LedgerSQN,
                        persisted_sqn = LedgerSQN},
                    [L0FN|FileList]};
            false ->
                leveled_log:log(p0017, []),
                {InitState#state{
                        manifest = Manifest1,
                        ledger_sqn = MaxSQN,
                        persisted_sqn = MaxSQN},
                    FileList}
        end,
    ok = archive_files(RootPath, FileList0),
    {ok, State0}.


-spec shutdown_manifest(leveled_pmanifest:manifest(), pid()|undefined) -> ok.
%% @doc
%% Shutdown all the SST files within the manifest
shutdown_manifest(Manifest, L0Constructor) ->
    EntryCloseFun =
        fun(ME) ->
            Owner =
                case is_record(ME, manifest_entry) of
                    true ->
                        ME#manifest_entry.owner;
                    false ->
                        case ME of
                            {_SK, ME0} ->
                                ME0#manifest_entry.owner;
                            ME ->
                                ME
                        end
                end,
            ok = 
                case check_alive(Owner) of
                    true ->
                        leveled_sst:sst_close(Owner);
                    false ->
                        ok
                end
        end,
    leveled_pmanifest:close_manifest(Manifest, EntryCloseFun),
    EntryCloseFun(L0Constructor).

-spec check_alive(pid()|undefined) -> boolean().
%% @doc
%% Double-check a processis active before attempting to terminate
check_alive(Owner) when is_pid(Owner) ->
    is_process_alive(Owner);
check_alive(_Owner) ->
    false.

-spec archive_files(list(), list()) -> ok.
%% @doc
%% Archive any sst files in the folder that have not been used to build the 
%% ledger at startup.  They may have not deeleted as expected, so this saves
%% them off as non-SST fies to make it easier for an admin to garbage collect 
%% theses files
archive_files(RootPath, UsedFileList) ->
    {ok, AllFiles} = file:list_dir(sst_rootpath(RootPath)),
    FileCheckFun =
        fun(FN, UnusedFiles) ->
            FN0 = "./" ++ FN,
            case filename:extension(FN0) of 
                ?SST_FILEX ->
                    case lists:member(FN0, UsedFileList) of 
                        true ->
                            UnusedFiles;
                        false ->
                            leveled_log:log(p0040, [FN0]),
                            [FN0|UnusedFiles]
                    end;
                _ ->
                    UnusedFiles
            end
        end,
    RenameFun =
        fun(FN) ->
            AltName = filename:join(sst_rootpath(RootPath), 
                                        filename:basename(FN, ?SST_FILEX))
                        ++ ?ARCHIVE_FILEX,
            file:rename(filename:join(sst_rootpath(RootPath), FN), 
                            AltName)
        end,
    FilesToArchive = lists:foldl(FileCheckFun, [], AllFiles),
    lists:foreach(RenameFun, FilesToArchive),
    ok.


-spec maybe_cache_too_big(
    pos_integer(), pos_integer(), boolean()) -> boolean().
%% @doc
%% Is the cache too big - should it be flushed to on-disk Level 0
%% There exists some jitter to prevent all caches from flushing concurrently
%% where there are multiple leveled instances on one machine.
maybe_cache_too_big(NewL0Size, L0MaxSize, CoinToss) ->
    CacheTooBig = NewL0Size > L0MaxSize,
    CacheMuchTooBig = 
        NewL0Size > min(?SUPER_MAX_TABLE_SIZE, 2 * L0MaxSize),
    RandomFactor =
        case CoinToss of
            true ->
                case leveled_rand:uniform(?COIN_SIDECOUNT) of
                    1 ->
                        true;
                    _ ->
                        false
                end;
            false ->
                true
        end,
    CacheTooBig and (RandomFactor or CacheMuchTooBig).

-spec roll_memory(
    pos_integer(), non_neg_integer(), string(),
    levelzero_cache()|none, pos_integer(),
    sst_options(), boolean())
        -> {pid(), leveled_ebloom:bloom()|none}.
%% @doc
%% Roll the in-memory cache into a L0 file.  If this is done synchronously, 
%% will return a bloom representing the contents of the file. 
%%
%% Casting a large object (the levelzero cache) to the SST file does not lead
%% to an immediate return.  With 32K keys in the TreeList it could take around
%% 35-40ms due to the overheads of copying.
%%
%% To avoid blocking the penciller, the SST file can request each item of the
%% cache one at a time.
%%
%% The Wait is set to false to use a cast when calling this in normal operation
%% where as the Wait of true is used at shutdown
roll_memory(NextManSQN, LedgerSQN, RootPath, none, CL, SSTOpts, false) ->
    L0Path = sst_rootpath(RootPath),
    L0FN = sst_filename(NextManSQN, 0, 0),
    leveled_log:log(p0019, [L0FN, LedgerSQN]),
    PCL = self(),
    FetchFun =
        fun(Slot, ReturnFun) -> pcl_fetchlevelzero(PCL, Slot, ReturnFun) end,
    {ok, Constructor, _} =
        leveled_sst:sst_newlevelzero(
            L0Path, L0FN, CL, FetchFun, PCL, LedgerSQN, SSTOpts),
    {Constructor, none};
roll_memory(NextManSQN, LedgerSQN, RootPath, L0Cache, CL, SSTOpts, true) ->
    L0Path = sst_rootpath(RootPath),
    L0FN = sst_filename(NextManSQN, 0, 0),
    FetchFun = fun(Slot) -> lists:nth(Slot, L0Cache) end,
    KVList = leveled_pmem:to_list(CL, FetchFun),
    {ok, Constructor, _, Bloom} =
        leveled_sst:sst_new(
            L0Path, L0FN, 0, KVList, LedgerSQN, SSTOpts),
    {Constructor, Bloom}.

-spec timed_fetch_mem(
    tuple(),
    {integer(), integer()}, 
    leveled_pmanifest:manifest(), list(), 
    leveled_pmem:index_array(),
    leveled_monitor:monitor()) -> leveled_codec:ledger_kv()|not_found.
%% @doc
%% Fetch the result from the penciller, starting by looking in the memory, 
%% and if it is not found looking down level by level through the LSM tree.
%%
%% This allows for the request to be timed, and the timing result to be added
%% to the aggregate timings - so that timinings per level can be logged and 
%% the cost of requests dropping levels can be monitored.
%%
%% the result tuple includes the level at which the result was found.
timed_fetch_mem(Key, Hash, Manifest, L0Cache, L0Index, Monitor) ->
    SW0 = leveled_monitor:maybe_time(Monitor),
    {R, Level} =
        fetch_mem(Key, Hash, Manifest, L0Cache, L0Index, fun timed_sst_get/4),
    {TS0, _SW1} = leveled_monitor:step_time(SW0),
    maybelog_fetch_timing(Monitor, Level, TS0, R == not_present),
    R.

-spec fetch_sqn(
    leveled_codec:ledger_key(),
    leveled_codec:segment_hash(),
    leveled_pmanifest:manifest(),
    list(),
    leveled_pmem:index_array()) ->
        not_present|leveled_codec:ledger_kv()|leveled_codec:sqn().
%% @doc
%% Fetch the result from the penciller, starting by looking in the memory, 
%% and if it is not found looking down level by level through the LSM tree.
fetch_sqn(Key, Hash, Manifest, L0Cache, L0Index) ->
    R = fetch_mem(Key, Hash, Manifest, L0Cache, L0Index, fun sst_getsqn/4),
    element(1, R).

fetch_mem(Key, Hash, Manifest, L0Cache, L0Index, FetchFun) ->
    PosList = 
        case L0Index of
            none ->
                lists:seq(1, length(L0Cache));
            _ ->
                leveled_pmem:check_index(Hash, L0Index)
        end,
    L0Check = leveled_pmem:check_levelzero(Key, Hash, PosList, L0Cache),
    case L0Check of
        {false, not_found} ->
            fetch(Key, Hash, Manifest, 0, FetchFun);
        {true, KV} ->
            {KV, memory}
    end.

-spec fetch(tuple(), {integer(), integer()}, 
                leveled_pmanifest:manifest(), integer(), 
                sst_fetchfun()) -> {tuple()|not_present, integer()|basement}.
%% @doc
%% Fetch from the persisted portion of the LSM tree, checking each level in 
%% turn until a match is found.
%% Levels can be skipped by checking the bloom for the relevant file at that
%% level.
fetch(_Key, _Hash, _Manifest, ?MAX_LEVELS + 1, _FetchFun) ->
    {not_present, basement};
fetch(Key, Hash, Manifest, Level, FetchFun) ->
    case leveled_pmanifest:key_lookup(Manifest, Level, Key) of
        false ->
            fetch(Key, Hash, Manifest, Level + 1, FetchFun);
        FP ->
            case leveled_pmanifest:check_bloom(Manifest, FP, Hash) of 
                true ->
                    case FetchFun(FP, Key, Hash, Level) of
                        not_present ->
                            fetch(Key, Hash, Manifest, Level + 1, FetchFun);
                        ObjectFound ->
                            {ObjectFound, Level}
                    end;
                false ->
                    fetch(Key, Hash, Manifest, Level + 1, FetchFun)
            end
    end.
    
timed_sst_get(PID, Key, Hash, Level) ->
    SW = os:timestamp(),
    R = leveled_sst:sst_get(PID, Key, Hash),
    T0 = timer:now_diff(os:timestamp(), SW),
    log_slowfetch(T0, R, PID, Level, ?SLOW_FETCH).

sst_getsqn(PID, Key, Hash, _Level) ->
    leveled_sst:sst_getsqn(PID, Key, Hash).

log_slowfetch(T0, R, PID, Level, FetchTolerance) ->
    case {T0, R} of
        {T, R} when T < FetchTolerance ->
            R;
        {T, not_present} ->
            leveled_log:log(pc016, [PID, T, Level, not_present]),
            not_present;
        {T, R} ->
            leveled_log:log(pc016, [PID, T, Level, found]),
            R
    end.

-spec compare_to_sqn(
    leveled_codec:ledger_kv()|leveled_codec:sqn()|not_present,
    integer()) -> sqn_check().
%% @doc
%% Check to see if the SQN in the penciller is after the SQN expected for an 
%% object (used to allow the journal to check compaction status from a cache
%% of the ledger - objects with a more recent sequence number can be compacted).
compare_to_sqn(not_present, _SQN) ->
    missing;
compare_to_sqn(ObjSQN, SQN) when is_integer(ObjSQN), ObjSQN > SQN ->
    replaced;
compare_to_sqn(ObjSQN, _SQN) when is_integer(ObjSQN) ->
    % Normally we would expect the SQN to be equal here, but
    % this also allows for the Journal to have a more advanced
    % value. We return true here as we wouldn't want to
    % compact thta more advanced value, but this may cause
    % confusion in snapshots.
    current;
compare_to_sqn(Obj, SQN) ->
    compare_to_sqn(leveled_codec:strip_to_seqonly(Obj), SQN).

-spec maybelog_fetch_timing(
    leveled_monitor:monitor(),
    memory|leveled_pmanifest:lsm_level(),
    leveled_monitor:timing(),
    boolean()) -> ok.
maybelog_fetch_timing(_Monitor, _Level, no_timing, _NF) ->
    ok;
maybelog_fetch_timing({Pid, _StatsFreq}, _Level, FetchTime, true) ->
    leveled_monitor:add_stat(Pid, {pcl_fetch_update, not_found, FetchTime});
maybelog_fetch_timing({Pid, _StatsFreq}, Level, FetchTime, _NF) ->
    leveled_monitor:add_stat(Pid, {pcl_fetch_update, Level, FetchTime}).

%%%============================================================================
%%% Key folder
%%%============================================================================

-type sst_iterator()
    :: #{
        leveled_pmanifest:lsm_level() =>
            list(leveled_sst:expandable_pointer()|leveled_codec:ledger_kv()),
        -1 =>
            list(leveled_codec:ledger_kv())}.
-type max_keys() :: unlimited|non_neg_integer().
-type iterator_level() :: -1|leveled_pmanifest:lsm_level().
-type search_info() ::
    {{leveled_codec:ledger_key(), leveled_codec:ledger_key()},
        {non_neg_integer(), pos_integer()|infinity},
        leveled_sst:segment_check_fun()}.

-define(NULL_KEY, {null, null}).

-spec keyfolder(
    sst_iterator(),
    {leveled_codec:ledger_key(), leveled_codec:ledger_key()},
    {pclacc_fun(), any(), pos_integer()},
    {leveled_sst:segment_check_fun(),
        {non_neg_integer(), pos_integer()|infinity},
        -1|non_neg_integer()}) -> {non_neg_integer(), term()}|term().
keyfolder(
        Iterator,
        {StartKey, EndKey},
        {AccFun, InitAcc, Now},
        {SegCheckFun, LastModRange, KeyLimit}) ->
    % The in-memory dump of keys in this range, may go beyond the end key - so
    % strip these back before starting the fold 
    StripIMMFun =
        fun(MemIter) ->
            lists:reverse(
                lists:dropwhile(
                    fun({K, _V}) -> leveled_codec:endkey_passed(EndKey, K) end,
                    lists:reverse(MemIter)))
        end,
    MaxKeys = 
        case KeyLimit of
            -1 -> unlimited;
            KeyLimit when is_integer(KeyLimit), KeyLimit >= 0 -> KeyLimit
        end,
    keyfolder(
        maps:update_with(-1, StripIMMFun, Iterator),
        InitAcc,
        MaxKeys,
        {?FOLD_SCANWIDTH, lists:sort(maps:keys(Iterator))},
        {{StartKey, EndKey}, LastModRange, SegCheckFun},
        {AccFun, Now}).

-spec keyfolder(
    sst_iterator()|no_more_keys,
    term(),
    max_keys(),
    {pos_integer(), list(iterator_level())},
    search_info(),
    {pclacc_fun(), integer()}) -> {non_neg_integer(), term()}|term().
%% @doc
%% The keyfolder takes an iterator - a map with an entry for each level, from
%% level -1 (the in-memory cache of keys) through to level 7 (the theoretical)
%% maximum level. 
%%
%% The find_nextkeys function is used to scan the iterators to find the next
%% set of W keys. These can then be accumulated.  If there is a MaxKeys set
%% (i.e. a maximum number of KV pairs to be accumulated), then this must be
%% tracked so the keyfolder never asks for more than the remainder from
%% find_nextkeys 
keyfolder(no_more_keys, Acc, MaxKeys, _LevelInfo, _SearchInfo, _AccDetails) ->
    case MaxKeys of
        unlimited -> Acc;
        MaxKeys -> {MaxKeys, Acc}
    end;
keyfolder(_Iter, Acc, 0, _LevelInfo, _SearchInfo, _AccDetails) ->
    {0, Acc};
keyfolder(
        Iter,
        Acc,
        MaxKeys,
        {W, Ls}=LevelInfo,
        {_KR, LastModRange, _SCF}=SearchInfo,
        {AccFun, Now}=AccDetails) ->
    {IterUpd, FoundKVs} =
        find_nextkeys(
            Iter,
            {Ls, ?NULL_KEY},
            [],
            Ls,
            {fetch_size(MaxKeys, W), scan_size(MaxKeys)},
            SearchInfo),
    {UpdAcc, KeyCount} =
        leveled_codec:maybe_accumulate(
            lists:reverse(FoundKVs), Acc, 0, {Now, LastModRange}, AccFun),
    MaxKeysLeft =
        case MaxKeys of
            unlimited -> unlimited;
            MaxKeys -> MaxKeys - KeyCount
        end,
    keyfolder(IterUpd, UpdAcc, MaxKeysLeft, LevelInfo, SearchInfo, AccDetails).

-spec fetch_size(max_keys(), pos_integer()) -> pos_integer().
fetch_size(unlimited, W) -> W;
fetch_size(MaxKeys, W) -> min(MaxKeys, W).

-spec scan_size(max_keys()) -> pos_integer().
scan_size(unlimited) ->
    ?ITERATOR_SCANWIDTH;
scan_size(MaxKeys) ->
    min(?ITERATOR_SCANWIDTH, max(?ITERATOR_MINSCANWIDTH, MaxKeys div 256)).

-spec find_nextkeys(
    sst_iterator(),
    {list(iterator_level()),
        {null|iterator_level(), null|leveled_codec:ledger_kv()}},
    list(leveled_codec:ledger_kv()),
    list(iterator_level()),
    {pos_integer(), pos_integer()},
    search_info()) ->
            {no_more_keys, list(leveled_codec:ledger_kv())}|
                {sst_iterator(), list(leveled_codec:ledger_kv())}.
%% @doc
%% Looks to find up to W keys, where for each key every level is checked,
%% comparing keys to find the best key for that loop
find_nextkeys(
        _Iter, {[], ?NULL_KEY}, FoundKVs, _Ls, _BatchInfo, _SearchInfo) ->
    % Each level checked and best key still NULL => no_more_keys
    {no_more_keys, FoundKVs};
find_nextkeys(
        Iter, {[], {BKL, BestKV}}, FoundKVs, _Ls, {W, _SW}, _SearchInfo)
            when length(FoundKVs) == W - 1 ->
    % All levels scanned, and there are now W keys (W - 1 previously found plus
    % the latest best key)
    {maps:update_with(BKL, fun tl/1, Iter), [BestKV|FoundKVs]};
find_nextkeys(
        Iter, {[], {BKL, BestKV}}, FoundKVs, Ls, BatchInfo, SearchInfo) ->
    % All levels scanned so this is the best key ... now loop to find more
    find_nextkeys(
        maps:update_with(BKL, fun tl/1, Iter),
        {Ls, ?NULL_KEY},
        [BestKV|FoundKVs],
        Ls, BatchInfo, SearchInfo);
find_nextkeys(
        Iter,
        {[LCnt|OtherLevels]=LoopLs, {BKL, BKV}=PrevBest},
        FoundKVs,
        Ls,
        {_W, ScanWidth}=BI,
        {{StartKey, EndKey}, {LowLastMod, _High}, SegChecker}=SI) ->
    case maps:get(LCnt, Iter) of
        [] ->
            find_nextkeys(
                Iter,
                {OtherLevels, PrevBest},
                FoundKVs,
                Ls -- [LCnt], BI, SI);
        [{next, Owner, _SK}|RestOfKeys] ->
            % Expansion required
            Pointer = {next, Owner, StartKey, EndKey},
            UpdList =
                leveled_sst:sst_expandpointer(
                    Pointer, RestOfKeys, ScanWidth, SegChecker, LowLastMod),
            % Need to loop around at this level (LCnt) as we have not yet
            % examined a real key at this level
            find_nextkeys(
                maps:update(LCnt, UpdList, Iter),
                {LoopLs, PrevBest},
                FoundKVs,
                Ls, BI, SI);
        [{pointer, SSTPid, Slot, PSK, PEK}|RestOfKeys] ->
            % Expansion required
            Pointer = {pointer, SSTPid, Slot, PSK, PEK},
            UpdList =
                leveled_sst:sst_expandpointer(
                    Pointer, RestOfKeys, ScanWidth, SegChecker, LowLastMod),
            % Need to loop around at this level (LCnt) as we have not yet
            % examined a real key at this level
            find_nextkeys(
                maps:update(LCnt, UpdList, Iter),
                {LoopLs, PrevBest},
                FoundKVs,
                Ls, BI, SI);
        [{Key, Val}|_RestOfKeys] when BKV == null ->
            find_nextkeys(
                Iter,
                {OtherLevels, {LCnt, {Key, Val}}},
                FoundKVs,
                Ls, BI, SI);
        [{Key, Val}|_RestOfKeys] when Key < element(1, BKV) ->
            find_nextkeys(
                Iter,
                {OtherLevels, {LCnt, {Key, Val}}},
                FoundKVs,
                Ls, BI, SI);
        [{Key, _Val}|_RestOfKeys] when Key > element(1, BKV) ->
            find_nextkeys(
                Iter,
                {OtherLevels, PrevBest},
                FoundKVs,
                Ls, BI, SI);
        [{Key, Val}|_RestOfKeys] ->
            case leveled_codec:key_dominates({Key, Val}, BKV) of
                true ->
                    find_nextkeys(
                        maps:update_with(BKL, fun tl/1, Iter),
                        {OtherLevels, {LCnt, {Key, Val}}},
                        FoundKVs,
                        Ls, BI, SI);
                false ->
                    find_nextkeys(
                        maps:update_with(LCnt, fun tl/1, Iter),
                        {OtherLevels, PrevBest},
                        FoundKVs,
                        Ls, BI, SI)
            end
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-spec pcl_fetch(
    pid(), leveled_codec:ledger_key())
    -> leveled_codec:ledger_kv()|not_present.
pcl_fetch(Pid, Key) ->
    Hash = leveled_codec:segment_hash(Key),
    if
        Hash /= no_lookup ->
            gen_server:call(Pid, {fetch, Key, Hash, true}, infinity)
    end.

keyfolder_test(IMMiter, SSTiter, StartKey, EndKey, {AccFun, Acc, Now}) ->
    keyfolder(
        maps:put(-1, IMMiter, SSTiter),
        {StartKey, EndKey},
        {AccFun, Acc, Now},
        {false, {0, infinity}, -1}).

convert_qmanifest_tomap(SSTiter) ->
    maps:from_list(SSTiter).

find_nextkey(QueryArray, StartKey, EndKey) ->
    {UpdArray, NextKeys} =
        find_nextkeys(
            QueryArray,
            {maps:keys(QueryArray), ?NULL_KEY},
            [],
            maps:keys(QueryArray),
            {1, 1},
            {{StartKey, EndKey}, {0, infinity}, false}),
    case UpdArray of
        no_more_keys ->
            no_more_keys;
        UpdArray ->
            [NextKey] = NextKeys,
            {UpdArray, NextKey}
    end.

generate_randomkeys({Count, StartSQN}) ->
    generate_randomkeys(Count, StartSQN, []).

generate_randomkeys(0, _SQN, Acc) ->
    lists:reverse(Acc);
generate_randomkeys(Count, SQN, Acc) ->
    K = {o,
            lists:concat(["Bucket", leveled_rand:uniform(1024)]),
            lists:concat(["Key", leveled_rand:uniform(1024)]),
            null},
    RandKey = {K,
                {SQN,
                {active, infinity},
                leveled_codec:segment_hash(K),
                null}},
    generate_randomkeys(Count - 1, SQN + 1, [RandKey|Acc]).

clean_testdir(RootPath) ->
    clean_subdir(sst_rootpath(RootPath)),
    clean_subdir(filename:join(RootPath, ?MANIFEST_FP)).

clean_subdir(DirPath) ->
    case filelib:is_dir(DirPath) of
        true ->
            {ok, Files} = file:list_dir(DirPath),
            lists:foreach(fun(FN) ->
                                File = filename:join(DirPath, FN),
                                ok = file:delete(File),
                                io:format("Success deleting ~s~n", [File])
                                end,
                            Files);
        false ->
            ok
    end.

maybe_pause_push(PCL, KL) ->
    T0 = [],
    I0 = leveled_pmem:new_index(),
    T1 = lists:foldl(fun({K, V}, {AccSL, AccIdx, MinSQN, MaxSQN}) ->
                            UpdSL = [{K, V}|AccSL],
                            SQN = leveled_codec:strip_to_seqonly({K, V}),
                            H = leveled_codec:segment_hash(K),
                            UpdIdx = leveled_pmem:prepare_for_index(AccIdx, H),
                            {UpdSL, UpdIdx, min(SQN, MinSQN), max(SQN, MaxSQN)}
                            end,
                        {T0, I0, infinity, 0},
                        KL),
    SL = element(1, T1),
    Tree = leveled_tree:from_orderedlist(lists:ukeysort(1, SL), ?CACHE_TYPE),
    T2 = setelement(1, T1, Tree),
    case pcl_pushmem(PCL, T2) of
        returned ->
            timer:sleep(50),
            maybe_pause_push(PCL, KL);
        ok ->
            ok
    end.

%% old test data doesn't have the magic hash
add_missing_hash({K, {SQN, ST, MD}}) ->
    {K, {SQN, ST, leveled_codec:segment_hash(K), MD}}.


archive_files_test() ->
    RootPath = "test/test_area/ledger",
    SSTPath = sst_rootpath(RootPath),
    ok = filelib:ensure_dir(SSTPath),
    ok = file:write_file(SSTPath ++ "/test1.sst", "hello_world"),
    ok = file:write_file(SSTPath ++ "/test2.sst", "hello_world"),
    ok = file:write_file(SSTPath ++ "/test3.bob", "hello_world"),
    UsedFiles = ["./test1.sst"],
    ok = archive_files(RootPath, UsedFiles),
    {ok, AllFiles} = file:list_dir(SSTPath),
    ?assertMatch(true, lists:member("test1.sst", AllFiles)),
    ?assertMatch(false, lists:member("test2.sst", AllFiles)),
    ?assertMatch(true, lists:member("test3.bob", AllFiles)),
    ?assertMatch(true, lists:member("test2.bak", AllFiles)),
    ok = clean_subdir(SSTPath).

shutdown_when_compact(Pid) ->
    FoldFun = 
        fun(_I, Ready) ->
            case Ready of 
                true -> 
                    true;
                false -> 
                    timer:sleep(200),
                    not pcl_checkforwork(Pid)
            end
        end,
    true = lists:foldl(FoldFun, false, lists:seq(1, 100)),
    io:format("No outstanding compaction work for ~w~n", [Pid]),
    pcl_close(Pid).

format_status_test() ->
    RootPath = "test/test_area/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = 
        pcl_start(#penciller_options{root_path=RootPath,
                                        max_inmemory_tablesize=1000,
                                        sst_options=#sst_options{}}),
    {status, PCL, {module, gen_server}, SItemL} = sys:get_status(PCL),
    S = lists:keyfind(state, 1, lists:nth(5, SItemL)),
    true = is_integer(array:size(element(2, S#state.manifest))),
    ST = format_status(terminate, [dict:new(), S]),
    ?assertMatch(redacted, ST#state.manifest),
    ?assertMatch(redacted, ST#state.levelzero_cache),
    ?assertMatch(redacted, ST#state.levelzero_index),
    ?assertMatch(redacted, ST#state.levelzero_astree),
    clean_testdir(RootPath).

close_no_crash_test_() ->
    {timeout, 60, fun close_no_crash_tester/0}.

close_no_crash_tester() ->
    RootPath = "test/test_area/ledger_close",
    clean_testdir(RootPath),
    {ok, PCL} = 
        pcl_start(
            #penciller_options{
                root_path=RootPath,
                max_inmemory_tablesize=1000,
                sst_options=#sst_options{}}),
    {ok, PclSnap} =
        pcl_snapstart(
            #penciller_options{
                start_snapshot = true,
                snapshot_query = undefined,
                bookies_mem = {empty_cache, empty_index, 1, 1},
                source_penciller = PCL,
                snapshot_longrunning = true,
                bookies_pid = self()
            }
        ),
    exit(PclSnap, kill),
    ok = pcl_close(PCL),
    clean_testdir(RootPath).


simple_server_test() ->
    RootPath = "test/test_area/ledger",
    clean_testdir(RootPath),
    {ok, PCL} = 
        pcl_start(#penciller_options{root_path=RootPath,
                                        max_inmemory_tablesize=1000,
                                        sst_options=#sst_options{}}),
    Key1_Pre = {{o,"Bucket0001", "Key0001", null},
                    {1, {active, infinity}, null}},
    Key1 = add_missing_hash(Key1_Pre),
    KL1 = generate_randomkeys({1000, 2}),
    Key2_Pre = {{o,"Bucket0002", "Key0002", null},
                {1002, {active, infinity}, null}},
    Key2 = add_missing_hash(Key2_Pre),
    KL2 = generate_randomkeys({900, 1003}),
    % Keep below the max table size by having 900 not 1000
    Key3_Pre = {{o,"Bucket0003", "Key0003", null},
                {2003, {active, infinity}, null}},
    Key3 = add_missing_hash(Key3_Pre),
    KL3 = generate_randomkeys({1000, 2004}), 
    Key4_Pre = {{o,"Bucket0004", "Key0004", null},
                {3004, {active, infinity}, null}},
    Key4 = add_missing_hash(Key4_Pre),
    KL4 = generate_randomkeys({1000, 3005}),
    ok = maybe_pause_push(PCL, [Key1]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, KL1),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ok = maybe_pause_push(PCL, [Key2]),
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    
    ok = maybe_pause_push(PCL, KL2),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ok = maybe_pause_push(PCL, [Key3]),
    
    ?assertMatch(Key1, pcl_fetch(PCL, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCL, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCL, {o,"Bucket0003", "Key0003", null})),
    
    true = pcl_checkbloomtest(PCL, {o,"Bucket0001", "Key0001", null}),
    true = pcl_checkbloomtest(PCL, {o,"Bucket0002", "Key0002", null}),
    true = pcl_checkbloomtest(PCL, {o,"Bucket0003", "Key0003", null}),
    false = pcl_checkbloomtest(PCL, {o,"Bucket9999", "Key9999", null}),
    
    ok = shutdown_when_compact(PCL),

    {ok, PCLr} = 
        pcl_start(#penciller_options{root_path=RootPath,
                                        max_inmemory_tablesize=1000,
                                        sst_options=#sst_options{}}),
    ?assertMatch(2003, pcl_getstartupsequencenumber(PCLr)),
    % ok = maybe_pause_push(PCLr, [Key2] ++ KL2 ++ [Key3]),
    true = pcl_checkbloomtest(PCLr, {o,"Bucket0001", "Key0001", null}),
    true = pcl_checkbloomtest(PCLr, {o,"Bucket0002", "Key0002", null}),
    true = pcl_checkbloomtest(PCLr, {o,"Bucket0003", "Key0003", null}),
    false = pcl_checkbloomtest(PCLr, {o,"Bucket9999", "Key9999", null}),
    
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    ok = maybe_pause_push(PCLr, KL3),
    ok = maybe_pause_push(PCLr, [Key4]),
    ok = maybe_pause_push(PCLr, KL4),
    ?assertMatch(Key1, pcl_fetch(PCLr, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PCLr, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PCLr, {o,"Bucket0003", "Key0003", null})),
    ?assertMatch(Key4, pcl_fetch(PCLr, {o,"Bucket0004", "Key0004", null})),
    
    {ok, PclSnap, null} = 
        leveled_bookie:snapshot_store(
            leveled_bookie:empty_ledgercache(),
            PCLr,
            null,
            {no_monitor, 0},
            ledger,
            undefined,
            false),           
    
    ?assertMatch(Key1, pcl_fetch(PclSnap, {o,"Bucket0001", "Key0001", null})),
    ?assertMatch(Key2, pcl_fetch(PclSnap, {o,"Bucket0002", "Key0002", null})),
    ?assertMatch(Key3, pcl_fetch(PclSnap, {o,"Bucket0003", "Key0003", null})),
    ?assertMatch(Key4, pcl_fetch(PclSnap, {o,"Bucket0004", "Key0004", null})),
    ?assertMatch(
        current, 
        pcl_checksequencenumber(
            PclSnap, {o, "Bucket0001", "Key0001", null}, 1)),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap, {o, "Bucket0002", "Key0002", null}, 1002)),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap, {o, "Bucket0003", "Key0003", null}, 2003)),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap, {o, "Bucket0004", "Key0004", null}, 3004)),

    % Add some more keys and confirm that check sequence number still
    % sees the old version in the previous snapshot, but will see the new 
    % version in a new snapshot
    
    Key1A_Pre = {{o,"Bucket0001", "Key0001", null},
                    {4005, {active, infinity}, null}},
    Key1A = add_missing_hash(Key1A_Pre),
    KL1A = generate_randomkeys({2000, 4006}),
    ok = maybe_pause_push(PCLr, [Key1A]),
    ok = maybe_pause_push(PCLr, KL1A),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap, {o, "Bucket0001", "Key0001", null}, 1)),
    ok = pcl_close(PclSnap),
     
    {ok, PclSnap2, null} = 
        leveled_bookie:snapshot_store(
            leveled_bookie:empty_ledgercache(),
            PCLr,
            null,
            {no_monitor, 0},
            ledger,
            undefined,
            false),
    
    ?assertMatch(
        replaced,
        pcl_checksequencenumber(
            PclSnap2, {o, "Bucket0001", "Key0001", null}, 1)),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap2, {o, "Bucket0001", "Key0001", null}, 4005)),
    ?assertMatch(
        current,
        pcl_checksequencenumber(
            PclSnap2, {o, "Bucket0002", "Key0002", null}, 1002)),
    ok = pcl_close(PclSnap2),
    ok = pcl_close(PCLr),
    clean_testdir(RootPath).


simple_findnextkey_test() ->
    QueryArrayAsList = [
    {2, [{{o, "Bucket1", "Key1", null}, {5, {active, infinity}, {0, 0}, null}},
        {{o, "Bucket1", "Key5", null}, {4, {active, infinity}, {0, 0}, null}}]},
    {3, [{{o, "Bucket1", "Key3", null}, {3, {active, infinity}, {0, 0}, null}}]},
    {5, [{{o, "Bucket1", "Key2", null}, {2, {active, infinity}, {0, 0}, null}}]}
    ],
    QueryArray = convert_qmanifest_tomap(QueryArrayAsList),
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key1", null}, 
                        {5, {active, infinity}, {0, 0}, null}},
                    KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key2", null}, 
                        {2, {active, infinity}, {0, 0}, null}},
                    KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key3", null}, 
                        {3, {active, infinity}, {0, 0}, null}},
                    KV3),
    {Array5, KV4} = find_nextkey(Array4,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key5", null}, 
                        {4, {active, infinity}, {0, 0}, null}},
                    KV4),
    ER = find_nextkey(Array5,
                        {o, "Bucket1", "Key0", null},
                        {o, "Bucket1", "Key5", null}),
    ?assertMatch(no_more_keys, ER).

sqnoverlap_findnextkey_test() ->
    QueryArrayAsList = [
    {2, [{{o, "Bucket1", "Key1", null}, {5, {active, infinity}, {0, 0}, null}},
        {{o, "Bucket1", "Key5", null}, {4, {active, infinity}, {0, 0}, null}}]},
    {3, [{{o, "Bucket1", "Key3", null}, {3, {active, infinity}, {0, 0}, null}}]},
    {5, [{{o, "Bucket1", "Key5", null}, {2, {active, infinity}, {0, 0}, null}}]}
    ],
    QueryArray = convert_qmanifest_tomap(QueryArrayAsList),
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key1", null}, 
                        {5, {active, infinity}, {0, 0}, null}},
                    KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key3", null}, 
                        {3, {active, infinity}, {0, 0}, null}},
                    KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key5", null}, 
                        {4, {active, infinity}, {0, 0}, null}},
                    KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0", null},
                        {o, "Bucket1", "Key5", null}),
    ?assertMatch(no_more_keys, ER).

sqnoverlap_otherway_findnextkey_test() ->
    QueryArrayAsList = [
    {2, [{{o, "Bucket1", "Key1", null}, {5, {active, infinity}, {0, 0}, null}},
        {{o, "Bucket1", "Key5", null}, {1, {active, infinity}, {0, 0}, null}}]},
    {3, [{{o, "Bucket1", "Key3", null}, {3, {active, infinity}, {0, 0}, null}}]},
    {5, [{{o, "Bucket1", "Key5", null}, {2, {active, infinity}, {0, 0}, null}}]}
    ],
    QueryArray = convert_qmanifest_tomap(QueryArrayAsList),
    {Array2, KV1} = find_nextkey(QueryArray,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key1", null},
                        {5, {active, infinity}, {0, 0}, null}},
                    KV1),
    {Array3, KV2} = find_nextkey(Array2,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key3", null},
                        {3, {active, infinity}, {0, 0}, null}},
                    KV2),
    {Array4, KV3} = find_nextkey(Array3,
                                    {o, "Bucket1", "Key0", null},
                                    {o, "Bucket1", "Key5", null}),
    ?assertMatch({{o, "Bucket1", "Key5", null},
                        {2, {active, infinity}, {0, 0}, null}},
                    KV3),
    ER = find_nextkey(Array4,
                        {o, "Bucket1", "Key0", null},
                        {o, "Bucket1", "Key5", null}),
    ?assertMatch(no_more_keys, ER).

foldwithimm_simple_test() ->
    Now = leveled_util:integer_now(),
    QueryArrayAsList = [
        {2, [{{o, "Bucket1", "Key1", null},
                    {5, {active, infinity}, 0, null}},
                {{o, "Bucket1", "Key5", null},
                    {1, {active, infinity}, 0, null}}]},
        {3, [{{o, "Bucket1", "Key3", null},
                {3, {active, infinity}, 0, null}}]},
        {5, [{{o, "Bucket1", "Key5", null},
                {2, {active, infinity}, 0, null}}]}
    ],
    QueryArray = convert_qmanifest_tomap(QueryArrayAsList),
    KL1A = [{{o, "Bucket1", "Key6", null}, {7, {active, infinity}, 0, null}},
            {{o, "Bucket1", "Key1", null}, {8, {active, infinity}, 0, null}},
            {{o, "Bucket1", "Key8", null}, {9, {active, infinity}, 0, null}}],
    IMM2 = leveled_tree:from_orderedlist(lists:ukeysort(1, KL1A), ?CACHE_TYPE),
    IMMiter = leveled_tree:match_range({o, "Bucket1", "Key1", null},
                                        {o, null, null, null},
                                        IMM2),
    AccFun = fun(K, V, Acc) -> SQN = leveled_codec:strip_to_seqonly({K, V}),
                                Acc ++ [{K, SQN}] end,
    Acc = keyfolder_test(IMMiter,
                    QueryArray,
                    {o, "Bucket1", "Key1", null}, {o, "Bucket1", "Key6", null},
                    {AccFun, [], Now}),
    ?assertMatch([{{o, "Bucket1", "Key1", null}, 8},
                    {{o, "Bucket1", "Key3", null}, 3},
                    {{o, "Bucket1", "Key5", null}, 2},
                    {{o, "Bucket1", "Key6", null}, 7}], Acc),
    
    IMMiterA = [{{o, "Bucket1", "Key1", null},
                    {8, {active, infinity}, 0, null}}],
    AccA = keyfolder_test(IMMiterA,
                        QueryArray,
                        {o, "Bucket1", "Key1", null}, 
                        {o, "Bucket1", "Key6", null},
                        {AccFun, [], Now}),
    ?assertMatch([{{o, "Bucket1", "Key1", null}, 8},
                    {{o, "Bucket1", "Key3", null}, 3},
                    {{o, "Bucket1", "Key5", null}, 2}], AccA),
    
    AddKV = {{o, "Bucket1", "Key4", null}, {10, {active, infinity}, 0, null}},
    KL1B = [AddKV|KL1A],
    IMM3 = leveled_tree:from_orderedlist(lists:ukeysort(1, KL1B), ?CACHE_TYPE),
    IMMiterB = leveled_tree:match_range({o, "Bucket1", "Key1", null},
                                        {o, null, null, null},
                                        IMM3),
    io:format("Compare IMM3 with QueryArrary~n"),
    AccB = keyfolder_test(IMMiterB,
                    QueryArray,
                    {o, "Bucket1", "Key1", null}, {o, "Bucket1", "Key6", null},
                    {AccFun, [], Now}),
    ?assertMatch([{{o, "Bucket1", "Key1", null}, 8},
                    {{o, "Bucket1", "Key3", null}, 3},
                    {{o, "Bucket1", "Key4", null}, 10},
                    {{o, "Bucket1", "Key5", null}, 2},
                    {{o, "Bucket1", "Key6", null}, 7}], AccB).

create_file_test() ->
    {RP, Filename} = {"test/test_area/", "new_file.sst"},
    ok = file:write_file(filename:join(RP, Filename), term_to_binary("hello")),
    KVL = lists:usort(generate_randomkeys({50000, 0})),
    Tree = leveled_tree:from_orderedlist(KVL, ?CACHE_TYPE),
    
    {ok, SP, noreply} = 
        leveled_sst:sst_newlevelzero(RP,
                                        Filename,
                                        1,
                                        [Tree],
                                        undefined,
                                        50000,
                                        #sst_options{press_method = native}),
    {ok, SrcFN, StartKey, EndKey} = leveled_sst:sst_checkready(SP),
    io:format("StartKey ~w EndKey ~w~n", [StartKey, EndKey]),
    ?assertMatch({o, _, _, _}, StartKey),
    ?assertMatch({o, _, _, _}, EndKey),
    ?assertMatch("./new_file.sst", SrcFN),
    ok = leveled_sst:sst_clear(SP),
    {ok, Bin} = file:read_file("test/test_area/new_file.sst.discarded"),
    ?assertMatch("hello", binary_to_term(Bin)).

slow_fetch_test() ->
    ?assertMatch(not_present, log_slowfetch(2, not_present, "fake", 0, 1)),
    ?assertMatch("value", log_slowfetch(2, "value", "fake", 0, 1)).

coverage_cheat_test() ->
    {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).

handle_down_test() ->
    RootPath = "test/test_area/ledger",
    clean_testdir(RootPath),
    {ok, PCLr} = 
        pcl_start(#penciller_options{root_path=RootPath,
                                        max_inmemory_tablesize=1000,
                                        sst_options=#sst_options{}}),
    FakeBookie = spawn(fun loop/0),

    Mon = erlang:monitor(process, FakeBookie),

    FakeBookie ! {snap, PCLr, self()},

    {ok, PclSnap, null} =
        receive
            {FakeBookie, {ok, Snap, null}} ->
                {ok, Snap, null}
        end,
    
    CheckSnapDiesFun =
        fun(_X, IsDead) ->
            case IsDead of
                true ->
                    true;
                false ->
                    case erlang:process_info(PclSnap) of
                        undefined ->
                            true;
                        _ ->
                            timer:sleep(100),
                            false
                    end
            end
        end,
    ?assertNot(lists:foldl(CheckSnapDiesFun, false, [1, 2])),

    FakeBookie ! stop,

    receive
        {'DOWN', Mon, process, FakeBookie, normal} ->
            %% Now we know that pclr should have received this too!
            %% (better than timer:sleep/1)
            ok
    end,

    ?assert(lists:foldl(CheckSnapDiesFun, false, lists:seq(1, 10))),

    pcl_close(PCLr),
    clean_testdir(RootPath).


%% the fake bookie. Some calls to leveled_bookie (like the two below)
%% do not go via the gen_server (but it looks like they expect to be
%% called by the gen_server, internally!) they use "self()" to
%% populate the bookie's pid in the pclr. This process wrapping the
%% calls ensures that the TEST controls the bookie's Pid. The
%% FakeBookie.
loop() ->
    receive
        {snap, PCLr, TestPid} ->
            {ok, Snap, null} =
                leveled_bookie:snapshot_store(
                    leveled_bookie:empty_ledgercache(),
                    PCLr,
                    null, 
                    {no_monitor, 0},
                    ledger,
                    undefined,
                    false),
            TestPid ! {self(), {ok, Snap, null}},
            loop();
        stop ->
            ok
    end.

-endif.