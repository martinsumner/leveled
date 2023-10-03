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

-module(leveled_simpleeqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(DATA_DIR, "./leveled_eqc").

-record(state, {leveled = undefined ::  undefined | pid(),
                model :: orddict:orddict(),  %% The DB state on disk
                %% gets set if leveled is started, and not destroyed
                %% in the test.
                leveled_needs_destroy = false :: boolean(),
                previous_keys = [] :: list(binary()),   %% Used to increase probability to pick same key
                deleted_keys = [] :: list(binary()),
                start_opts = []
               }).

-define(NUMTESTS, 10000).
-define(TIME_BUDGET, 300).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-type state() :: #state{}.

eqc_test_() ->
    {timeout,
        ?TIME_BUDGET + 10,
        ?_assertEqual(
            true,
            eqc:quickcheck(
                eqc:testing_time(?TIME_BUDGET, ?QC_OUT(prop_db()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_db())).

check() ->
    eqc:check(prop_db()).

initial_state() ->
    #state{model = orddict:new()}.

%% --- Operation: init_backend ---
%% @doc init_backend_pre/1 - Precondition for generation
-spec init_backend_pre(S :: eqc_statem:symbolic_state()) -> boolean().
init_backend_pre(S) ->
    not is_leveled_open(S).

%% @doc init_backend_args - Argument generator
-spec init_backend_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
init_backend_args(_S) ->
    [gen_opts()].

%% @doc init_backend - The actual operation
%% Start the database and read data from disk
init_backend(Options) ->
    {ok, Bookie} = leveled_bookie:book_start(Options),
    Bookie.

%% @doc init_backend_next - Next state function
-spec init_backend_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
init_backend_next(S, LevelEdPid, [Options]) ->
    S#state{leveled=LevelEdPid, leveled_needs_destroy=true,
            start_opts = Options}.

%% @doc init_backend_post - Postcondition for init_backend
-spec init_backend_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
init_backend_post(_S, [_Options], LevelEdPid) ->
    is_pid(LevelEdPid).

%% --- Operation: stop ---
%% @doc stop_pre/1 - Precondition for generation
-spec stop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
stop_pre(S) ->
    is_leveled_open(S).

%% @doc stop_args - Argument generator
-spec stop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
stop_args(#state{leveled=Pid}) ->
    [Pid].

%% @doc stop - The actual operation
%% Stop the server, but the values are still on disk
stop(Pid) ->
    ok = leveled_bookie:book_close(Pid).

%% @doc stop_next - Next state function
-spec stop_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
stop_next(S, _Value, [_Pid]) ->
    S#state{leveled=undefined}.

%% @doc stop_post - Postcondition for stop
-spec stop_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
stop_post(_S, [Pid], _Res) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            true
    after 5000 ->
            {still_a_pid, Pid}
    end.


%% --- Operation: put ---
%% @doc put_pre/1 - Precondition for generation
-spec put_pre(S :: eqc_statem:symbolic_state()) -> boolean().
put_pre(S) ->
    is_leveled_open(S).

%% @doc put_args - Argument generator
-spec put_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
put_args(#state{leveled=Pid, previous_keys=PK}) ->
    [gen_key(PK), gen_val(), Pid].

%% @doc put - The actual operation
put(Key, Value, Pid) ->
    ok = leveled_bookie:book_put(Pid, Key, Key, Value, []).

%% @doc put_next - Next state function
-spec put_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
put_next(S, _Value, [Key, Value, _Pid]) ->
    #state{model=Model, previous_keys=PK} = S,
    Model2 = orddict:store(Key, Value, Model),
    S#state{model=Model2, previous_keys=[Key | PK]}.

%% @doc put_features - Collects a list of features of this call with these arguments.
-spec put_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
put_features(#state{previous_keys=PK}, [Key, _Value, _Pid], _Res) ->
    case lists:member(Key, PK) of
        true ->
            [{put, update}];
        false ->
            [{put, insert}]
    end.

%% --- Operation: get ---
%% @doc get_pre/1 - Precondition for generation
-spec get_pre(S :: eqc_statem:symbolic_state()) -> boolean().
get_pre(S) ->
    is_leveled_open(S).

%% @doc get_args - Argument generator
-spec get_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
get_args(#state{leveled=Pid, previous_keys=PK}) ->
    [Pid, gen_key(PK)].

%% @doc get - The actual operation
get(Pid, Key) ->
    leveled_bookie:book_get(Pid, Key, Key).

%% @doc get_post - Postcondition for get
-spec get_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
get_post(S, [_Pid, Key], Res) ->
    #state{model=Model} = S,
    case orddict:find(Key, Model) of
        {ok, V} ->
            Res == {ok, V};
        error ->
            Res == not_found
    end.

%% @doc get_features - Collects a list of features of this call with these arguments.
-spec get_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
get_features(S, [_Pid, Key], Res) ->
    case Res of
        not_found ->
            [{get, not_found, deleted} || lists:member(Key, S#state.deleted_keys)] ++ 
          [{get, not_found, not_inserted} || not lists:member(Key, S#state.previous_keys)];
        {ok, B} when is_binary(B) ->
            [{get, found}]
    end.

%% --- Operation: delete ---
%% @doc delete_pre/1 - Precondition for generation
-spec delete_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delete_pre(S) ->
    is_leveled_open(S).

%% @doc delete_args - Argument generator
-spec delete_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
delete_args(#state{leveled=Pid, previous_keys=PK}) ->
    [Pid, gen_key(PK)].

%% @doc delete - The actual operation
delete(Pid, Key) ->
    ok = leveled_bookie:book_delete(Pid, Key, Key, []).

%% @doc delete_next - Next state function
-spec delete_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
delete_next(S, _Value, [_Pid, Key]) ->
    #state{model=Model, deleted_keys=DK} = S,
    Model2 = orddict:erase(Key, Model),
    S#state{model=Model2, deleted_keys = case orddict:is_key(Key, Model) of
                                             true -> [Key | DK];
                                             false -> DK
                                         end}.

%% @doc delete_features - Collects a list of features of this call with these arguments.
-spec delete_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
delete_features(S, [_Pid, Key], _Res) ->
    #state{previous_keys=PK} = S,
    case lists:member(Key, PK) of
        true ->
            [{delete, written}];
        false ->
            [{delete, not_written}]
    end.

%% --- Operation: is_empty ---
%% @doc is_empty_pre/1 - Precondition for generation
-spec is_empty_pre(S :: eqc_statem:symbolic_state()) -> boolean().
is_empty_pre(S) ->
    is_leveled_open(S).

%% @doc is_empty_args - Argument generator
-spec is_empty_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
is_empty_args(#state{leveled=Pid}) ->
    [Pid].

%% @doc is_empty - The actual operation
is_empty(Pid) ->
    leveled_bookie:book_isempty(Pid, ?STD_TAG).

%% @doc is_empty_post - Postcondition for is_empty
-spec is_empty_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_post(#state{model=Model}, [_Pid], Res) ->
    Size = orddict:size(Model),
    case Res of
      true -> eq(0, Size);
      false when Size == 0 -> expected_empty;
      false when Size > 0  -> true
    end.

%% @doc is_empty_features - Collects a list of features of this call with these arguments.
-spec is_empty_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_features(_S, [_Pid], Res) ->
    [{empty, Res}].

%% --- Operation: drop ---
%% @doc drop_pre/1 - Precondition for generation
-spec drop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
drop_pre(S) ->
    is_leveled_open(S).

%% @doc drop_args - Argument generator
-spec drop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
drop_args(#state{leveled=Pid}) ->
    [Pid, gen_opts()].

%% @doc drop - The actual operation
%% Remove fles from disk (directory structure may remain) and start a new clean database
drop(Pid, Opts) ->
    ok = leveled_bookie:book_destroy(Pid),
    init_backend(Opts).

%% @doc drop_next - Next state function
-spec drop_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
drop_next(_S, NewPid, [_Pid, Opts]) ->
    init_backend_next(#state{model = orddict:new()}, NewPid, [Opts]).

%% @doc drop_post - Postcondition for drop
-spec drop_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
drop_post(_S, [Pid, _Opts], NewPid) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            is_empty(NewPid)
    after 5000 ->
            {still_a_pid, Pid}
    end.

drop_features(S, [_Pid, _Opts], _Res) ->
  Size = orddict:size(S#state.model),
  [{drop, empty} || Size == 0 ] ++ 
    [{drop, Size div 10} || Size > 0 ].


weight(#state{previous_keys=[]}, Command) when Command == get;
                                               Command == delete ->
    1;
weight(_S, C) when C == get;
                   C == put ->
    10;
weight(_S, stop) ->
    1;
weight(_, _) ->
    1.

%% @doc check that the implementation of leveled is equivalent to a
%% sorted dict at least
-spec prop_db() -> eqc:property().
prop_db() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                delete_level_data(),

                {H, S, Res} = run_commands(?MODULE, Cmds),
                CallFeatures = call_features(H),
                StartOptions =  [{root_path, "./leveled_data"}, {log_level, error}],
                AllVals = get_all_vals(S#state.leveled, S#state.leveled_needs_destroy, StartOptions),

                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          aggregate(with_title('Features'), CallFeatures,
                                                    features(CallFeatures,
                                                             conjunction([{result, Res == ok},
                                                                          {vals, vals_equal(AllVals, S#state.model)}
                                                                         ])))))

            end).

gen_opts() ->
    [{root_path, "./leveled_data"}, {log_level, error}].

gen_key() ->
    binary(16).

gen_val() ->
    binary(32).

gen_key([]) ->
    gen_key();
gen_key(Previous) ->
    frequency([{1, gen_key()},
               {2, elements(Previous)}]).


%% Helper for all those preconditions that just check that leveled Pid
%% is populated in state.
-spec is_leveled_open(state()) -> boolean().
is_leveled_open(#state{leveled=undefined}) ->
    false;
is_leveled_open(_) ->
    true.

get_all_vals(undefined, false, _) ->
    [];
get_all_vals(undefined, true, Opts) ->
    %% start a leveled (may have been stopped in the test)
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    get_all_vals(Bookie);
get_all_vals(Pid, true, _) ->
    %% is stopped, but not yet destroyed
    get_all_vals(Pid).

get_all_vals(Pid) ->
    %% fold over all the values in leveled
    Acc = [],
    FoldFun = fun(_B, K, V, A) -> [{K, V} | A] end,
    AllKeyQuery = {foldobjects_allkeys, o, {FoldFun, Acc}, false},
    {async, Folder} = leveled_bookie:book_returnfolder(Pid, AllKeyQuery),
    AccFinal = Folder(),
    ok = leveled_bookie:book_destroy(Pid),
    lists:reverse(AccFinal).

vals_equal(Leveled, Model) ->
    %% We assume here that Leveled is an orddict, since Model is.
    ?WHENFAIL(eqc:format("level ~p =/=\nmodel ~p\n", [Leveled, Model]), Leveled == Model).

delete_level_data() ->
    ?_assertCmd("rm -rf " ++ " ./leveled_data").

-endif.