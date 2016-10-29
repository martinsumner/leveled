-module(leveled_pmem).

-behaviour(gen_server).

-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        roll_singletree/4,
        roll_arraytree/4,
        roll_arraylist/4,
        terminate/2,
        code_change/3]).      

-include_lib("eunit/include/eunit.hrl").

-define(ARRAY_WIDTH, 32).
-define(SLOT_WIDTH, 16386).

-record(state, {}).

%%%============================================================================
%%% API
%%%============================================================================


roll_singletree(LevelZero, LevelMinus1, LedgerSQN, PCL) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    gen_server:call(Pid, {single_tree, LevelZero, LevelMinus1, LedgerSQN, PCL}).

roll_arraytree(LevelZero, LevelMinus1, LedgerSQN, PCL) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    gen_server:call(Pid, {array_tree, LevelZero, LevelMinus1, LedgerSQN, PCL}).

roll_arraylist(LevelZero, LevelMinus1, LedgerSQN, PCL) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    gen_server:call(Pid, {array_list, LevelZero, LevelMinus1, LedgerSQN, PCL}).

roll_arrayfilt(LevelZero, LevelMinus1, LedgerSQN, PCL) ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    gen_server:call(Pid, {array_filter, LevelZero, LevelMinus1, LedgerSQN, PCL}).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({single_tree, LevelZero, LevelMinus1, LedgerSQN, _PCL},
                                                            _From, State) ->
    SW = os:timestamp(),
    {NewL0, Size, MaxSQN} = leveled_penciller:roll_new_tree(LevelZero,
                                                            LevelMinus1,
                                                            LedgerSQN),
    T = timer:now_diff(os:timestamp(), SW),
    io:format("Rolled tree to size ~w in ~w microseconds using single_tree~n",
                [Size, T]),
    {stop, normal, {NewL0, Size, MaxSQN, T}, State};
handle_call({array_tree, LevelZero, LevelMinus1, LedgerSQN, _PCL},
                                                            _From, State) ->
    SW = os:timestamp(),
    {MinSQN, MaxSQN, _Size, SplitTrees} = assess_sqn(LevelMinus1, to_array),
    R = lists:foldl(fun(X, {Arr, ArrSize}) ->
                        LM1 = array:get(X, SplitTrees),
                        T0 = array:get(X, LevelZero),
                        T1 = lists:foldl(fun({K, V}, TrAcc) ->
                                                gb_trees:enter(K, V, TrAcc)
                                                end,
                                            T0,
                                            LM1),
                        {array:set(X, T1, Arr), ArrSize + gb_trees:size(T1)}
                        end,
                    {array:new(?ARRAY_WIDTH, {default, gb_trees:empty()}), 0},
                    lists:seq(0, ?ARRAY_WIDTH - 1)),
    {NextL0, NewSize} = R,
    T = timer:now_diff(os:timestamp(), SW),
    io:format("Rolled tree to size ~w in ~w microseconds using array_tree~n",
                [NewSize, T]),
    if
        MinSQN >= LedgerSQN ->
            {stop, normal, {NextL0, NewSize, MaxSQN, T}, State}
    end;
handle_call({array_list, LevelZero, LevelMinus1, LedgerSQN, _PCL},
                                                            _From, State) ->
    SW = os:timestamp(),
    {MinSQN, MaxSQN, _Size, SplitTrees} = assess_sqn(LevelMinus1, to_array),
    R = lists:foldl(fun(X, {Arr, ArrSize}) ->
                        LM1 = array:get(X, SplitTrees),
                        T0 = array:get(X, LevelZero),
                        T1 = lists:foldl(fun({K, V}, TrAcc) ->
                                                [{K, V}|TrAcc]
                                                end,
                                            T0,
                                            LM1),
                        {array:set(X, T1, Arr), ArrSize + length(T1)}
                        end,
                    {array:new(?ARRAY_WIDTH, {default, []}), 0},
                    lists:seq(0, ?ARRAY_WIDTH - 1)),
    {NextL0, NewSize} = R,
    T = timer:now_diff(os:timestamp(), SW),
    io:format("Rolled tree to size ~w in ~w microseconds using array_list~n",
                [NewSize, T]),
    if
        MinSQN >= LedgerSQN ->
            {stop, normal, {NextL0, NewSize, MaxSQN, T}, State}
    end;
handle_call({array_filter, LevelZero, LevelMinus1, LedgerSQN, _PCL},
                                                            _From, State) ->
    SW = os:timestamp(),
    {MinSQN, MaxSQN, LM1Size, HashList} = assess_sqn(LevelMinus1, to_hashes),
    {L0Lookup, L0TreeList, L0Size} = LevelZero,
    UpdL0TreeList = [{LedgerSQN, LevelMinus1}|L0TreeList],
    UpdL0Lookup = lists:foldl(fun(X, LookupArray) ->
                                    L = array:get(X, LookupArray),
                                    array:set(X, [LedgerSQN|L], LookupArray)
                                    end,
                                L0Lookup,
                                HashList),
    NewSize = LM1Size + L0Size,
    T = timer:now_diff(os:timestamp(), SW),
    io:format("Rolled tree to size ~w in ~w microseconds using array_filter~n",
                [NewSize, T]),
    if
        MinSQN >= LedgerSQN ->
            {stop,
                normal,
                {{UpdL0Lookup, UpdL0TreeList, NewSize}, NewSize, MaxSQN, T},
                State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {stop, normal, ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


hash_to_index(Key) ->
    erlang:phash2(Key) band (?ARRAY_WIDTH - 1).

hash_to_slot(Key) ->
    erlang:phash2(Key) band (?SLOT_WIDTH - 1).

roll_into_list(Tree) ->
    gb_trees:to_list(Tree).

assess_sqn(Tree, to_array) ->
    L = roll_into_list(Tree),
    TmpA = array:new(?ARRAY_WIDTH, {default, []}),
    FoldFun = fun({K, V}, {AccMinSQN, AccMaxSQN, AccSize, Array}) ->
                    SQN = leveled_codec:strip_to_seqonly({K, V}),
                    Index = hash_to_index(K),
                    List0 = array:get(Index, Array),
                    List1 = lists:append(List0, [{K, V}]),
                    {min(SQN, AccMinSQN),
                        max(SQN, AccMaxSQN),
                        AccSize + 1,
                        array:set(Index, List1, Array)}
                    end,
    lists:foldl(FoldFun, {infinity, 0, 0, TmpA}, L);
assess_sqn(Tree, to_hashes) ->
    L = roll_into_list(Tree),
    FoldFun = fun({K, V}, {AccMinSQN, AccMaxSQN, AccSize, HashList}) ->
                    SQN = leveled_codec:strip_to_seqonly({K, V}),
                    Hash = hash_to_slot(K),
                    {min(SQN, AccMinSQN),
                        max(SQN, AccMaxSQN),
                        AccSize + 1,
                        [Hash|HashList]}
                    end,
    lists:foldl(FoldFun, {infinity, 0, 0, []}, L).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_randomkeys(Seqn,
                        Count,
                        gb_trees:empty(),
                        BucketRangeLow,
                        BucketRangeHigh).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BNumber = string:right(integer_to_list(BucketLow + random:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        gb_trees:enter(K, V, Acc),
                        BucketLow,
                        BRange).


speed_test() ->
    R = lists:foldl(fun(_X, {LedgerSQN,
                                {L0st, TTst},
                                {L0at, TTat},
                                {L0al, TTal},
                                {L0af, TTaf}}) ->
                            LM1 = generate_randomkeys(LedgerSQN + 1, 2000, 1, 500),
                            {NextL0st, S, MaxSQN, Tst} = roll_singletree(L0st,
                                                                            LM1,
                                                                            LedgerSQN,
                                                                            self()),
                            {NextL0at, S, MaxSQN, Tat} = roll_arraytree(L0at,
                                                                            LM1,
                                                                            LedgerSQN,
                                                                            self()),
                            {NextL0al, _S, MaxSQN, Tal} = roll_arraylist(L0al,
                                                                            LM1,
                                                                            LedgerSQN,
                                                                            self()),
                            {NextL0af, _S, MaxSQN, Taf} = roll_arrayfilt(L0af,
                                                                            LM1,
                                                                            LedgerSQN,
                                                                            self()),
                            {MaxSQN,
                                {NextL0st, TTst + Tst},
                                {NextL0at, TTat + Tat},
                                {NextL0al, TTal + Tal},
                                {NextL0af, TTaf + Taf}}
                            end,
                        {0,
                            {gb_trees:empty(), 0},
                            {array:new(?ARRAY_WIDTH, [{default, gb_trees:empty()}, fixed]), 0},
                            {array:new(?ARRAY_WIDTH, [{default, []}, fixed]), 0},
                            {{array:new(?SLOT_WIDTH, [{default, []}, fixed]), [], 0}, 0}
                            },
                        lists:seq(1, 16)),
    {_, {_, TimeST}, {_, TimeAT}, {_, TimeLT}, {_, TimeAF}} = R,
    io:format("Total time for single_tree ~w microseconds ~n", [TimeST]),
    io:format("Total time for array_tree ~w microseconds ~n", [TimeAT]),
    io:format("Total time for array_list ~w microseconds ~n", [TimeLT]),
    io:format("Total time for array_filter ~w microseconds ~n", [TimeAF]),
    ?assertMatch(true, false).
                    



-endif.