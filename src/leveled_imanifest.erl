%% -------- Inker Manifest ---------
%% 


-module(leveled_imanifest).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
        generate_entry/1,
        add_entry/2,
        append_lastkey/3,
        remove_entry/2,
        find_entry/2
        
        ]).         


%%%============================================================================
%%% API
%%%============================================================================

generate_entry(Journal) ->
    {ok, NewFN} = leveled_cdb:cdb_complete(Journal),
    {ok, PidR} = leveled_cdb:cdb_open_reader(NewFN),
    case leveled_cdb:cdb_firstkey(PidR) of
        {StartSQN, _Type, _PK} ->
            LastKey = leveled_cdb:cdb_lastkey(PidR),
            [{StartSQN, NewFN, PidR, LastKey}];
        empty ->
            leveled_log:log("IC013", [NewFN]),
            []
    end.
                        
add_entry(Manifest, Entry) ->
    {SQN, FN, PidR, LastKey} = Entry,
    StrippedName = filename:rootname(FN),
    lists:reverse(lists:sort([{SQN, StrippedName, PidR, LastKey}|Manifest])).

append_lastkey(Manifest, Pid, LastKey) ->
    [{SQN, Filename, PidR, LK}|ManifestTail] = Manifest,
    case {PidR, LK} of 
        {Pid, empty} ->
            [{SQN, Filename, PidR, LastKey}|ManifestTail];
        _ ->
            Manifest
    end.

remove_entry(Manifest, Entry) ->
    {SQN, FN, _PidR, _LastKey} = Entry,
    leveled_log:log("I0013", [FN]),
    lists:keydelete(SQN, 1, Manifest).

find_entry(SQN, [{LowSQN, _FN, Pid, _LK}|_Tail]) when SQN >= LowSQN ->
    Pid;
find_entry(SQN, [_Head|Tail]) ->
    find_entry(SQN, Tail).



%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


-endif.