%% -------- TINY BLOOM ---------
%%
%% For sheltering relatively expensive lookups with a probabilistic check
%%
%% Uses multiple 512 byte blooms.  Can sensibly hold up to 1000 keys per array.
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
        empty/1,
        tiny_enter/2,
        tiny_check/2,
        tiny_empty/0
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
    FoldFun =
        fun(K, Arr) -> add_to_array(K, Arr, 4096) end,
    BitArray1 = lists:foldl(FoldFun,
                                BitArray0,
                                lists:usort([Bit1, Bit2])),
    dict:store(Slot, BitArray1, Bloom);
enter(Key, Bloom) ->
    Hash = leveled_codec:magic_hash(Key),
    enter({hash, Hash}, Bloom).

check({hash, Hash}, Bloom) ->
    {H0, Bit1, Bit2} = split_hash(Hash),
    Slot = H0 rem dict:size(Bloom),
    BitArray = dict:fetch(Slot, Bloom),
    case getbit(Bit1, BitArray, 4096) of
        <<0:1>> ->
            false;
        <<1:1>> ->
            case getbit(Bit2, BitArray, 4096) of
                <<0:1>> ->
                    false;
                <<1:1>> ->
                    true
            end
    end;
check(Key, Bloom) ->
    Hash = leveled_codec:magic_hash(Key),
    check({hash, Hash}, Bloom).

tiny_empty() ->
    <<0:4096>>.

tiny_enter({hash, no_lookup}, Bloom) ->
    Bloom;
tiny_enter({hash, Hash}, Bloom) ->
    {Q, Bit0, Bit1, Bit2} = split_hash_for_tinybloom(Hash),
    AddFun = fun(Bit, Arr0) -> add_to_array(Bit, Arr0, 1024) end,
    case Q of
        0 ->
            <<Bin1:1024/bitstring, Bin2:3072/bitstring>> = Bloom,
            NewBin = lists:foldl(AddFun, Bin1, [Bit0, Bit1, Bit2]),
            <<NewBin/bitstring, Bin2/bitstring>>;
        1 ->
            <<Bin1:1024/bitstring,
                Bin2:1024/bitstring,
                Bin3:2048/bitstring>> = Bloom,
            NewBin = lists:foldl(AddFun, Bin2, [Bit0, Bit1, Bit2]),
            <<Bin1/bitstring, NewBin/bitstring, Bin3/bitstring>>;
        2 ->
            <<Bin1:2048/bitstring,
                Bin2:1024/bitstring,
                Bin3:1024/bitstring>> = Bloom,
            NewBin = lists:foldl(AddFun, Bin2, [Bit0, Bit1, Bit2]),
            <<Bin1/bitstring, NewBin/bitstring, Bin3/bitstring>>;
        3 ->
            <<Bin1:3072/bitstring, Bin2:1024/bitstring>> = Bloom,
            NewBin = lists:foldl(AddFun, Bin2, [Bit0, Bit1, Bit2]),
            <<Bin1/bitstring, NewBin/bitstring>>
    end.

tiny_check({hash, Hash}, FullBloom) ->
    {Q, Bit0, Bit1, Bit2} = split_hash_for_tinybloom(Hash),
    Bloom =
        case Q of
            0 ->
                <<Bin1:1024/bitstring, _Bin2:3072/bitstring>> = FullBloom,
                Bin1;
            1 ->
                <<_Bin1:1024/bitstring,
                    Bin2:1024/bitstring,
                    _Bin3:2048/bitstring>> = FullBloom,
                Bin2;
            2 ->
                <<_Bin1:2048/bitstring,
                    Bin2:1024/bitstring,
                    _Bin3:1024/bitstring>> = FullBloom,
                Bin2;
            3 ->
                <<_Bin1:3072/bitstring, Bin2:1024/bitstring>> = FullBloom,
                Bin2
        end,
    case getbit(Bit0, Bloom, 1024) of
        <<0:1>> ->
            false;
        <<1:1>> ->
            case getbit(Bit1, Bloom, 1024) of
                <<0:1>> ->
                    false;
                <<1:1>> ->
                    case getbit(Bit2, Bloom, 1024) of
                        <<0:1>> ->
                            false;
                        <<1:1>> ->
                            true
                    end
            end
    end.


%%%============================================================================
%%% Internal Functions
%%%============================================================================

split_hash(Hash) ->
    H0 = Hash band 255,
    H1 = (Hash bsr 8) band 4095,
    H2 = Hash bsr 20,
    {H0, H1, H2}.

split_hash_for_tinybloom(Hash) ->
    % Tiny bloom can make k=3 from one hash
    Q = Hash band 3,
    H0 = (Hash bsr 2) band 1023,
    H1 = (Hash bsr 12) band 1023,
    H2 = (Hash bsr 22) band 1023,
    {Q, H0, H1, H2}.

add_to_array(Bit, BitArray, ArrayLength) ->
    RestLen = ArrayLength - Bit - 1,
    <<Head:Bit/bitstring,
        _B:1/bitstring,
        Rest:RestLen/bitstring>> = BitArray,
    <<Head/bitstring, 1:1, Rest/bitstring>>.

getbit(Bit, BitArray, ArrayLength) ->
    RestLen = ArrayLength - Bit - 1,
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

tiny_test() ->
    N = 512,
    K = 32, % more checks out then in K * checks
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
                        lists:seq(1, N * K)),
    
    HashIn = lists:map(fun(X) ->
                            {hash, leveled_codec:magic_hash(X)} end,
                            KLin),
    HashOut = lists:map(fun(X) ->
                            {hash, leveled_codec:magic_hash(X)} end,
                            KLout),
       
    SW1 = os:timestamp(),
    Bloom = lists:foldr(fun tiny_enter/2, tiny_empty(), HashIn),
    io:format(user,
                "~nAdding ~w hashes to tiny bloom took ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW1)]),
    
    SW2 = os:timestamp(),
    lists:foreach(fun(X) ->
                    ?assertMatch(true, tiny_check(X, Bloom)) end, HashIn),
    io:format(user,
                "~nChecking ~w hashes in tiny bloom took ~w microseconds~n",
                [N, timer:now_diff(os:timestamp(), SW2)]),
    
    SW3 = os:timestamp(),
    FP = lists:foldr(fun(X, Acc) -> case tiny_check(X, Bloom) of
                                        true -> Acc + 1;
                                        false -> Acc
                                    end end,
                        0,
                        HashOut),
    io:format(user,
                "~nChecking ~w hashes out of tiny bloom took ~w microseconds "
                    ++ "with ~w false positive rate~n",
                [N * K, timer:now_diff(os:timestamp(), SW3), FP / (N * K)]),
    ?assertMatch(true, FP < ((N * K) div 8)).


-endif.