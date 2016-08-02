%% Controlling asynchronour work in leveleddb to manage compaction within a
%% level and cleaning out of old files across a level


-module(leveled_clerk).

-export([merge_file/3, perform_merge/3]).      

-include_lib("eunit/include/eunit.hrl").


merge_file(_FileToMerge, _ManifestMgr, _Level) ->
    %% CandidateList = leveled_manifest:get_manifest_atlevel(ManifestMgr, Level),
    %% [Adds, Removes] = perform_merge(FileToMerge, CandidateList, Level),
    %%leveled_manifest:update_manifest_atlevel(ManifestMgr, Level, Adds, Removes),
    ok.
    

%% Assumption is that there is a single SFT from a higher level that needs
%% to be merged into multiple SFTs at a lower level.  This should create an
%% entirely new set of SFTs, and the calling process can then update the
%% manifest.
%%
%% Once the FileToMerge has been emptied, the remainder of the candidate list
%% needs to be placed in a remainder SFT that may be of a sub-optimal (small)
%% size.  This stops the need to perpetually roll over the whole level if the
%% level consists of already full files.  Some smartness may be required when
%% selecting the candidate list so that small files just outside the candidate
%% list be included to avoid a proliferation of small files.
%%
%% FileToMerge should be a tuple of {FileName, Pid} where the Pid is the Pid of
%% the gen_server leveled_sft process representing the file.
%%
%% CandidateList should be a list of {StartKey, EndKey, Pid} tuples
%% representing different gen_server leveled_sft processes, sorted by StartKey.
%%
%% The level is the level which the new files should be created at.

perform_merge(FileToMerge, CandidateList, Level) ->
    {Filename, UpperSFTPid} = FileToMerge,
    MergeID = generate_merge_id(Filename, Level),
    io:format("Merge to be commenced for FileToMerge=~s with MergeID=~s~n",
                    [Filename, MergeID]),
    PointerList = lists:map(fun(P) -> {next, P, all} end, CandidateList),
    do_merge([{next, UpperSFTPid, all}],
                    PointerList, Level, MergeID, 0, []).

do_merge([], [], Level, MergeID, FileCounter, OutList) ->
    io:format("Merge completed with MergeID=~s Level=~w and FileCounter=~w~n",
                [MergeID, Level, FileCounter]),
    OutList;
do_merge(KL1, KL2, Level, MergeID, FileCounter, OutList) ->
    FileName = lists:flatten(io_lib:format("../test/~s_~w.sft", [MergeID, FileCounter])),
    io:format("File to be created as part of MergeID=~s Filename=~s~n", [MergeID, FileName]),
    case leveled_sft:sft_new(FileName, KL1, KL2, Level) of
        {ok, _Pid, {error, Reason}} ->
            io:format("Exiting due to error~w~n", [Reason]);
        {ok, Pid, Reply} ->
            {{KL1Rem, KL2Rem}, SmallestKey, HighestKey} = Reply,
            do_merge(KL1Rem, KL2Rem, Level, MergeID, FileCounter + 1,
                        lists:append(OutList, [{SmallestKey, HighestKey, Pid}]))
    end.


generate_merge_id(Filename, Level) ->
    <<A:32, C:16, D:16, E:48>> = crypto:rand_bytes(14),
    FileID = erlang:phash2(Filename, 256),
    B = FileID * 256 + Level,
    Str = io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b",
                        [A, B, C band 16#0fff, D band 16#3fff bor 16#8000, E]),
    list_to_binary(Str).




%%%============================================================================
%%% Test
%%%============================================================================


generate_randomkeys(Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Count, [], BucketRangeLow, BucketRangeHigh).

generate_randomkeys(0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Count, Acc, BucketLow, BRange) ->
    BNumber = string:right(integer_to_list(BucketLow + random:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    RandKey = {{o,
                "Bucket" ++ BNumber,
                "Key" ++ KNumber},
                Count + 1,
                {active, infinity}, null},
    generate_randomkeys(Count - 1, [RandKey|Acc], BucketLow, BRange).

choose_pid_toquery([{StartKey, EndKey, Pid}|_T], Key) when Key >= StartKey,
                                                            EndKey >= Key ->
    Pid;
choose_pid_toquery([_H|T], Key) ->
    choose_pid_toquery(T, Key).


find_randomkeys(_FList, 0, _Source) ->
    ok;
find_randomkeys(FList, Count, Source) ->
    K1 = leveled_sft:strip_to_keyonly(lists:nth(random:uniform(length(Source)),
                                                                Source)),
    P1 = choose_pid_toquery(FList, K1),
    FoundKV = leveled_sft:sft_get(P1, K1),
    case FoundKV of
        not_present ->
            io:format("Failed to find ~w in ~w~n", [K1, P1]),
            ?assertMatch(true, false);
        _ ->
            Found = leveled_sft:strip_to_keyonly(FoundKV),
            io:format("success finding ~w in ~w~n", [K1, P1]),
            ?assertMatch(K1, Found)
    end,
    find_randomkeys(FList, Count - 1, Source).


merge_file_test() ->
    KL1_L1 = lists:sort(generate_randomkeys(16000, 0, 1000)),
    {ok, PidL1_1, _} = leveled_sft:sft_new("../test/KL1_L1.sft",
                                            KL1_L1, [], 1),
    KL1_L2 = lists:sort(generate_randomkeys(16000, 0, 250)),
    {ok, PidL2_1, _} = leveled_sft:sft_new("../test/KL1_L2.sft",
                                            KL1_L2, [], 2),
    KL2_L2 = lists:sort(generate_randomkeys(16000, 250, 250)),
    {ok, PidL2_2, _} = leveled_sft:sft_new("../test/KL2_L2.sft",
                                            KL2_L2, [], 2),
    KL3_L2 = lists:sort(generate_randomkeys(16000, 500, 250)),
    {ok, PidL2_3, _} = leveled_sft:sft_new("../test/KL3_L2.sft",
                                            KL3_L2, [], 2),
    KL4_L2 = lists:sort(generate_randomkeys(16000, 750, 250)),
    {ok, PidL2_4, _} = leveled_sft:sft_new("../test/KL4_L2.sft",
                                            KL4_L2, [], 2),
    Result = perform_merge({"../test/KL1_L1.sft", PidL1_1},
                            [PidL2_1, PidL2_2, PidL2_3, PidL2_4],
                            2),
    lists:foreach(fun({{o, B1, K1}, {o, B2, K2}, R}) ->
                        io:format("Result of ~s ~s and ~s ~s with Pid ~w~n",
                            [B1, K1, B2, K2, R]) end,
                        Result),
    io:format("Finding keys in KL1_L1~n"),
    ok = find_randomkeys(Result, 50, KL1_L1),
    io:format("Finding keys in KL1_L2~n"),
    ok = find_randomkeys(Result, 50, KL1_L2),
    io:format("Finding keys in KL2_L2~n"),
    ok = find_randomkeys(Result, 50, KL2_L2),
    io:format("Finding keys in KL3_L2~n"),
    ok = find_randomkeys(Result, 50, KL3_L2),
    io:format("Finding keys in KL4_L2~n"),
    ok = find_randomkeys(Result, 50, KL4_L2),
    leveled_sft:sft_clear(PidL1_1),
    leveled_sft:sft_clear(PidL2_1),
    leveled_sft:sft_clear(PidL2_2),
    leveled_sft:sft_clear(PidL2_3),
    leveled_sft:sft_clear(PidL2_4),
    lists:foreach(fun({_StK, _EndK, Pid}) -> leveled_sft:sft_clear(Pid) end, Result).
