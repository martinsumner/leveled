-module(lookup_test).

-export([go_dict/1, 
    go_ets/1, 
    go_gbtree/1, 
    go_arrayofdict/1, 
    go_arrayofgbtree/1, 
    go_arrayofdict_withcache/1,
    create_blocks/3,
    size_testblocks/1,
    test_testblocks/2]).

-define(CACHE_SIZE, 512).

hash(Key) ->
  H = 5381,
  hash1(H,Key) band 16#FFFFFFFF.

hash1(H,[]) ->H;
hash1(H,[B|Rest]) ->
  H1 = H * 33,
  H2 = H1 bxor B,
  hash1(H2,Rest).

% Get the least significant 8 bits from the hash.
hash_to_index(Hash) ->
  Hash band 255.


%%
%% Timings (microseconds):
%%
%% go_dict(200000) :   1569894
%% go_dict(1000000) : 17191365
%% go_dict(5000000) :  forever

go_dict(N) ->
    go_dict(dict:new(), N, N).

go_dict(_, 0, _) -> 
    {erlang:memory(), statistics(garbage_collection)};
go_dict(D, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    dict:find(LookupHash, D),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    case dict:find(Hash, D) of 
        error ->
            go_dict(dict:store(Hash, [N], D), N-1, M);
        {ok, List} ->
            go_dict(dict:store(Hash, [N|List], D), N-1, M)
    end.



%%
%% Timings (microseconds):
%%
%% go_ets(200000) :    609119
%% go_ets(1000000) :  3520757
%% go_ets(5000000) : 19974562

go_ets(N) ->
    go_ets(ets:new(ets_test, [private, bag]), N, N).

go_ets(_, 0, _) ->
    {erlang:memory(), statistics(garbage_collection)};
go_ets(Ets, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    ets:lookup(Ets, LookupHash),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    ets:insert(Ets, {Hash, N}),
    go_ets(Ets, N - 1, M).

%%
%% Timings (microseconds):
%%
%% go_gbtree(200000) :   1393936
%% go_gbtree(1000000) :  8430997
%% go_gbtree(5000000) : 45630810

go_gbtree(N) ->
    go_gbtree(gb_trees:empty(), N, N).

go_gbtree(_, 0, _) ->
    {erlang:memory(), statistics(garbage_collection)};
go_gbtree(Tree, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    gb_trees:lookup(LookupHash, Tree),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    case gb_trees:lookup(Hash, Tree) of 
        none ->
            go_gbtree(gb_trees:insert(Hash, [N], Tree), N - 1, M);
        {value, List} ->
            go_gbtree(gb_trees:update(Hash, [N|List], Tree), N - 1, M)
    end.


%%
%% Timings (microseconds):
%%
%% go_arrayofidict(200000) :   1266931
%% go_arrayofidict(1000000) :  7387219
%% go_arrayofidict(5000000) : 49511484

go_arrayofdict(N) ->
    go_arrayofdict(array:new(256, {default, dict:new()}), N, N).

go_arrayofdict(_, 0, _) ->
    % dict:to_list(array:get(0, Array)),
    % dict:to_list(array:get(1, Array)),
    % dict:to_list(array:get(2, Array)),
    % dict:to_list(array:get(3, Array)),
    % dict:to_list(array:get(4, Array)),
    % dict:to_list(array:get(5, Array)),
    % dict:to_list(array:get(6, Array)),
    % dict:to_list(array:get(7, Array)),
    % dict:to_list(array:get(8, Array)),
    % dict:to_list(array:get(9, Array)),
    {erlang:memory(), statistics(garbage_collection)};
go_arrayofdict(Array, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    LookupIndex = hash_to_index(LookupHash),
    dict:find(LookupHash, array:get(LookupIndex, Array)),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    D = array:get(Index, Array),
    case dict:find(Hash, D) of 
        error ->
            go_arrayofdict(array:set(Index, 
                dict:store(Hash, [N], D), Array), N-1, M);
        {ok, List} ->
            go_arrayofdict(array:set(Index, 
                dict:store(Hash, [N|List], D), Array), N-1, M)
    end.

%%
%% Timings (microseconds):
%%
%% go_arrayofgbtree(200000) :   1176224
%% go_arrayofgbtree(1000000) :  7480653
%% go_arrayofgbtree(5000000) : 41266701

go_arrayofgbtree(N) ->
    go_arrayofgbtree(array:new(256, {default, gb_trees:empty()}), N, N).

go_arrayofgbtree(_, 0, _) ->
    % gb_trees:to_list(array:get(0, Array)),
    % gb_trees:to_list(array:get(1, Array)),
    % gb_trees:to_list(array:get(2, Array)),
    % gb_trees:to_list(array:get(3, Array)),
    % gb_trees:to_list(array:get(4, Array)),
    % gb_trees:to_list(array:get(5, Array)),
    % gb_trees:to_list(array:get(6, Array)),
    % gb_trees:to_list(array:get(7, Array)),
    % gb_trees:to_list(array:get(8, Array)),
    % gb_trees:to_list(array:get(9, Array)),
    {erlang:memory(), statistics(garbage_collection)};
go_arrayofgbtree(Array, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    LookupIndex = hash_to_index(LookupHash),
    gb_trees:lookup(LookupHash, array:get(LookupIndex, Array)),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    Tree = array:get(Index, Array),
    case gb_trees:lookup(Hash, Tree) of 
        none ->
            go_arrayofgbtree(array:set(Index, 
                gb_trees:insert(Hash, [N], Tree), Array), N - 1, M);
        {value, List} ->
            go_arrayofgbtree(array:set(Index, 
                gb_trees:update(Hash, [N|List], Tree), Array), N - 1, M)
    end.


%%
%% Timings (microseconds):
%%
%% go_arrayofdict_withcache(200000) :   1432951
%% go_arrayofdict_withcache(1000000) :  9140169
%% go_arrayofdict_withcache(5000000) : 59435511

go_arrayofdict_withcache(N) ->
    go_arrayofdict_withcache({array:new(256, {default, dict:new()}), 
        array:new(256, {default, dict:new()})}, N, N).

go_arrayofdict_withcache(_, 0, _) ->
    {erlang:memory(), statistics(garbage_collection)};
go_arrayofdict_withcache({MArray, CArray}, N, M) ->
    % Lookup a random key - which may not be present
    LookupKey = lists:concat(["key-", leveled_rand:uniform(M)]),
    LookupHash = hash(LookupKey),
    LookupIndex = hash_to_index(LookupHash),
    dict:find(LookupHash, array:get(LookupIndex, CArray)),
    dict:find(LookupHash, array:get(LookupIndex, MArray)),

    % Add a new key - which may be present so value to be appended
    Key = lists:concat(["key-", N]),
    Hash = hash(Key),
    Index = hash_to_index(Hash),
    Cache = array:get(Index, CArray),
    case dict:find(Hash, Cache) of 
        error ->
            UpdCache = dict:store(Hash, [N], Cache);
        {ok, _} ->
            UpdCache = dict:append(Hash, N, Cache)
    end,
    case dict:size(UpdCache) of 
        ?CACHE_SIZE ->
            UpdCArray = array:set(Index, dict:new(), CArray),
            UpdMArray = array:set(Index, dict:merge(fun merge_values/3, UpdCache, array:get(Index, MArray)), MArray),
            go_arrayofdict_withcache({UpdMArray, UpdCArray}, N - 1, M);
        _ ->
            UpdCArray = array:set(Index, UpdCache, CArray),
            go_arrayofdict_withcache({MArray, UpdCArray}, N - 1, M)
    end.



merge_values(_, Value1, Value2) ->
    lists:append(Value1, Value2).


%% Some functions for testing options compressing term_to_binary

create_block(N, BlockType) ->
    case BlockType of
        keylist ->
            create_block(N, BlockType, []);
        keygbtree ->
            create_block(N, BlockType, gb_trees:empty())
    end.

create_block(0, _, KeyStruct) ->
    KeyStruct;
create_block(N, BlockType, KeyStruct) ->
    Bucket = <<"pdsRecord">>,
    case N of
        20 ->
            Key = lists:concat(["key-20-special"]);
        _ ->
            Key = lists:concat(["key-", N, "-", leveled_rand:uniform(1000)])
    end,
    SequenceNumber = leveled_rand:uniform(1000000000),
    Indexes = [{<<"DateOfBirth_int">>, leveled_rand:uniform(10000)}, {<<"index1_bin">>, lists:concat([leveled_rand:uniform(1000), "SomeCommonText"])}, {<<"index2_bin">>, <<"RepetitionRepetitionRepetition">>}],
    case BlockType of
        keylist ->
            Term = {o, Bucket, Key, {Indexes, SequenceNumber}},
            create_block(N-1, BlockType, [Term|KeyStruct]);
        keygbtree ->
            create_block(N-1, BlockType, gb_trees:insert({o, Bucket, Key}, {Indexes, SequenceNumber}, KeyStruct))
    end.


create_blocks(N, Compression, BlockType) ->
    create_blocks(N, Compression, BlockType, 10000, []).

create_blocks(_, _, _, 0, BlockList) ->
    BlockList;
create_blocks(N, Compression, BlockType, TestLoops, BlockList) ->
    NewBlock = term_to_binary(create_block(N, BlockType), [{compressed, Compression}]),
    create_blocks(N, Compression, BlockType, TestLoops - 1, [NewBlock|BlockList]).
    
size_testblocks(BlockList) ->
    size_testblocks(BlockList,0).

size_testblocks([], Acc) ->
    Acc;
size_testblocks([H|T], Acc) ->
    size_testblocks(T, Acc + byte_size(H)).

test_testblocks([], _) ->
    true;
test_testblocks([H|T], BlockType) ->
    Block = binary_to_term(H),
    case findkey("key-20-special", Block, BlockType) of
        true ->
            test_testblocks(T, BlockType);
        not_found ->
            false
    end.

findkey(_, [], keylist) ->
    not_found;
findkey(Key, [H|T], keylist) ->
    case H of
        {o, <<"pdsRecord">>, Key, _} ->
            true;
        _ ->
        findkey(Key,T, keylist)     
    end;
findkey(Key, Tree, keygbtree) ->
    case gb_trees:lookup({o, <<"pdsRecord">>, Key}, Tree) of
        none ->
            not_found;
        _ ->
            true
    end.
    
