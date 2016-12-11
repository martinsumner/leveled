%% -------- TINY BLOOM ---------
%%
%% For sheltering relatively expensive lookups with a probabilistic check
%%
%% Uses multiple 256 byte blooms.  Can sensibly hold up to 1000 keys per array.
%% Even at 1000 keys should still offer only a 20% false positive
%%
%% Restricted to no more than 256 arrays - so can't handle more than 250K keys
%% in total
%%
%% Implemented this way to make it easy to control false positive (just by
%% setting the width).  Also only requires binary manipulations of a single
%% hash

-module(leveled_tinybloom).

-include("include/leveled.hrl").

-export([
        enter/2,
        check/2,
        empty/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

%%%============================================================================
%%% Bloom API
%%%============================================================================


empty(Width) when Width =< 256 ->
    FoldFun = fun(X, Acc) -> dict:store(X, <<0:4096>>, Acc) end,
    lists:foldl(FoldFun, dict:new(), lists:seq(0, Width - 1)).

enter({hash, no_lookup}, Bloom) ->
    Bloom;
enter({hash, Hash}, Bloom) ->
    {H0, Bit1, Bit2} = split_hash(Hash),
    Slot = H0 rem dict:size(Bloom),
    BitArray0 = dict:fetch(Slot, Bloom),
    BitArray1 = lists:foldl(fun add_to_array/2,
                                BitArray0,
                                lists:usort([Bit1, Bit2])),
    dict:store(Slot, BitArray1, Bloom);
enter(Key, Bloom) ->
    Hash = leveled_codec:magic_hash(Key),
    enter({hash, Hash}, Bloom).

check({hash, _Hash}, undefined) ->
    true;
check({hash, Hash}, Bloom) ->
    {H0, Bit1, Bit2} = split_hash(Hash),
    Slot = H0 rem dict:size(Bloom),
    BitArray = dict:fetch(Slot, Bloom),
    case getbit(Bit1, BitArray) of
        <<0:1>> ->
            false;
        <<1:1>> ->
            case getbit(Bit2, BitArray) of
                <<0:1>> ->
                    false;
                <<1:1>> ->
                    true
            end
    end;
check(Key, Bloom) ->
    Hash = leveled_codec:magic_hash(Key),
    check({hash, Hash}, Bloom).

%%%============================================================================
%%% Internal Functions
%%%============================================================================

split_hash(Hash) ->
    H0 = Hash band 255,
    H1 = (Hash bsr 8) band 4095,
    H2 = Hash bsr 20,
    {H0, H1, H2}.

add_to_array(Bit, BitArray) ->
    RestLen = 4096 - Bit - 1,
    <<Head:Bit/bitstring,
        _B:1/bitstring,
        Rest:RestLen/bitstring>> = BitArray,
    <<Head/bitstring, 1:1, Rest/bitstring>>.

getbit(Bit, BitArray) ->
    RestLen = 4096 - Bit - 1,
    <<_Head:Bit/bitstring,
        B:1/bitstring,
        _Rest:RestLen/bitstring>> = BitArray,
    B.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

simple_test() ->
    N = 4000,
    W = 4,
    KLin = lists:map(fun(X) -> "Key_" ++
                                integer_to_list(X) ++
                                integer_to_list(random:uniform(100)) ++
                                binary_to_list(crypto:rand_bytes(2))
                                end,
                        lists:seq(1, N)),
    KLout = lists:map(fun(X) ->
                            "NotKey_" ++
                            integer_to_list(X) ++
                            integer_to_list(random:uniform(100)) ++
                            binary_to_list(crypto:rand_bytes(2))
                            end,
                        lists:seq(1, N)),
    SW0_PH = os:timestamp(),
    lists:foreach(fun(X) -> erlang:phash2(X) end, KLin),
    io:format(user,
                "~nNative hash function hashes ~w keys in ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW0_PH)]),
    SW0_MH = os:timestamp(),
    lists:foreach(fun(X) -> leveled_codec:magic_hash(X) end, KLin),
    io:format(user,
                "~nMagic hash function hashes ~w keys in ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW0_MH)]),
    
    SW1 = os:timestamp(),
    Bloom = lists:foldr(fun enter/2, empty(W), KLin),
    io:format(user,
                "~nAdding ~w keys to bloom took ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW1)]),
    
    SW2 = os:timestamp(),
    lists:foreach(fun(X) -> ?assertMatch(true, check(X, Bloom)) end, KLin),
    io:format(user,
                "~nChecking ~w keys in bloom took ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW2)]),
    
    SW3 = os:timestamp(),
    FP = lists:foldr(fun(X, Acc) -> case check(X, Bloom) of
                                        true -> Acc + 1;
                                        false -> Acc
                                    end end,
                        0,
                        KLout),
    io:format(user,
                "~nChecking ~w keys out of bloom took ~w microseconds " ++
                    "with ~w false positive rate~n",
                [N, timer:now_diff(os:timestamp(), SW3), FP / N]),
    ?assertMatch(true, FP < (N div 4)).
    

-endif.