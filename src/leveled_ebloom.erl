%% -------- TinyBloom ---------
%%
%% A fixed size bloom that supports 32K keys only, made to try and minimise
%% the cost of producing the bloom
%%


-module(leveled_ebloom).

-include("include/leveled.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
            create_bloom/1,
            create_bloom/2,
            check_hash/2,
            check_hash/3
            ]).

-define(INTEGER_SIZE_MEDIUM, 4096).
-define(INTEGER_SIZE_SMALL, 256).

-type bloom() :: binary().
-type size() :: small|medium.

-export_type([bloom/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec create_bloom(list(leveled_codec:segment_hash())) -> bloom().
%% @doc
%% Create a binary bloom filter from alist of hashes
create_bloom(HashList) ->
    create_bloom(HashList, medium).

-spec create_bloom(list(pos_integer()), size()) -> bloom().
create_bloom(HashList, Size) ->
    IntSize =
        case Size of
            small -> ?INTEGER_SIZE_SMALL;
            medium -> ?INTEGER_SIZE_MEDIUM
        end,
    Slices = 
        case length(HashList) of
            0 ->
                [];
            L when L > IntSize * 4 ->
                add_hashlist(HashList,
                                32, Size,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0);
            L when L > IntSize ->
                add_hashlist(HashList,
                                16, Size,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0);
            L when L > IntSize div 2 ->
                add_hashlist(HashList, 4, Size, 0, 0, 0, 0);
            _ ->
                add_hashlist(HashList, 2, Size, 0, 0)
        end,
    lists:foldl(fun(S, Acc) -> <<Acc/binary, S:IntSize/integer>> end,
                <<>>,
                Slices).


-spec check_hash(leveled_codec:segment_hash(), bloom()) -> boolean().
%% @doc
%% Check for the presence of a given hash within a bloom
check_hash(SegHashTuple, BloomBin) ->
    check_hash(SegHashTuple, BloomBin, medium).

-spec check_hash(leveled_codec:segment_hash(), bloom(), size()) -> boolean().
check_hash(_Hash, <<>>, _Size) ->
    false;
check_hash({_SegHash, Hash}, BloomBin, Size) ->
    {IntSize, BloomBytes} = 
        case Size of
            small -> {?INTEGER_SIZE_SMALL, ?INTEGER_SIZE_SMALL div 8};
            medium -> {?INTEGER_SIZE_MEDIUM, ?INTEGER_SIZE_MEDIUM div 8}
        end,
    SlotSplit = byte_size(BloomBin) div BloomBytes,
    {Slot, Hashes} = split_hash(Hash, SlotSplit, Size),
    Mask = get_mask(Hashes),
    Pos = Slot * BloomBytes,
    <<_H:Pos/binary, CheckInt:IntSize/integer, _T/binary>> = BloomBin,
    case CheckInt band Mask of
        Mask ->
            true;
        _ ->
            false
    end.
    
%%%============================================================================
%%% Internal Functions
%%%============================================================================

split_hash(Hash, SlotSplit, medium) ->
    Slot = (Hash band 255) rem SlotSplit,
    H0 = (Hash bsr 8) band (?INTEGER_SIZE_MEDIUM - 1),
    H1 = (Hash bsr 20) band (?INTEGER_SIZE_MEDIUM - 1),
    {Slot, [H0, H1]};
split_hash(Hash, SlotSplit, small) ->
    Slot = (Hash band 255) rem SlotSplit,
    H0 = (Hash bsr 8) band (?INTEGER_SIZE_SMALL - 1),
    H1 = (Hash bsr 16) band (?INTEGER_SIZE_SMALL - 1),
    H2 = (Hash bsr 24) band (?INTEGER_SIZE_SMALL - 1),
    {Slot, [H0, H1, H2]}.

get_mask([H0, H1]) ->
    (1 bsl H0) bor (1 bsl H1);
get_mask([H0, H1, H2]) ->
    (1 bsl H0) bor (1 bsl H1) bor (1 bsl H2).


%% This looks ugly and clunky, but in tests it was quicker than modifying an
%% Erlang term like an array as it is passed around the loop

add_hashlist([], _Slot, _Size, S0, S1) ->
    [S0, S1];
add_hashlist([{_SegHash, TopHash}|T], SlotSplit, Size, S0, S1) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit, Size),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T, SlotSplit, Size, S0 bor Mask, S1);
        1 ->
            add_hashlist(T, SlotSplit, Size, S0, S1 bor Mask)
    end.

add_hashlist([], _Slot, _Size, S0, S1, S2, S3) ->
     [S0, S1, S2, S3];
add_hashlist([{_SegHash, TopHash}|T], SlotSplit, Size, S0, S1, S2, S3) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit, Size),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T, SlotSplit, Size, S0 bor Mask, S1, S2, S3);
        1 ->
            add_hashlist(T, SlotSplit, Size, S0, S1 bor Mask, S2, S3);
        2 ->
            add_hashlist(T, SlotSplit, Size, S0, S1, S2 bor Mask, S3);
        3 ->
            add_hashlist(T, SlotSplit, Size, S0, S1, S2, S3 bor Mask)
    end.

add_hashlist([], _Slot, _Size, S0, S1, S2, S3, S4, S5, S6, S7,
                S8, S9, S10, S11, S12, S13, S14, S15) ->
    [S0, S1, S2, S3, S4, S5, S6, S7, S8,
        S9, S10, S11, S12, S13, S14, S15];
add_hashlist([{_SegHash, TopHash}|T],
                SlotSplit, Size,
                S0, S1, S2, S3, S4, S5, S6, S7,
                S8, S9, S10, S11, S12, S13, S14, S15) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit, Size),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0 bor Mask, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        1 ->
            add_hashlist(T, 
                            SlotSplit, Size,
                            S0, S1 bor Mask, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        2 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2 bor Mask, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        3 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3 bor Mask, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        4 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4 bor Mask, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        5 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5 bor Mask, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        6 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6 bor Mask, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        7 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7 bor Mask,
                            S8, S9, S10, S11, S12, S13, S14, S15);
        8 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8 bor Mask, S9, S10, S11, S12, S13, S14, S15);
        9 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9 bor Mask, S10, S11, S12, S13, S14, S15);
        10 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10 bor Mask, S11, S12, S13, S14, S15);
        11 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11 bor Mask, S12, S13, S14, S15);
        12 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12 bor Mask, S13, S14, S15);
        13 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13 bor Mask, S14, S15);
        14 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14 bor Mask, S15);
        15 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15 bor Mask)
    end.


add_hashlist([], _Slot, _Size, S0, S1, S2, S3, S4, S5, S6, S7,
                S8, S9, S10, S11, S12, S13, S14, S15,
                S16, S17, S18, S19, S20, S21, S22, S23,
                S24, S25, S26, S27, S28, S29, S30, S31) ->
    [S0, S1, S2, S3, S4, S5, S6, S7,
        S8, S9, S10, S11, S12, S13, S14, S15,
        S16, S17, S18, S19, S20, S21, S22, S23,
        S24, S25, S26, S27, S28, S29, S30, S31];
add_hashlist([{_SegHash, TopHash}|T],
                SlotSplit, Size,
                S0, S1, S2, S3, S4, S5, S6, S7,
                S8, S9, S10, S11, S12, S13, S14, S15,
                S16, S17, S18, S19, S20, S21, S22, S23,
                S24, S25, S26, S27, S28, S29, S30, S31) ->
    {Slot, Hashes} = split_hash(TopHash, SlotSplit, Size),
    Mask = get_mask(Hashes),
    case Slot of
        0 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0 bor Mask, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        1 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1 bor Mask, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        2 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2 bor Mask, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        3 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3 bor Mask, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        4 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4 bor Mask, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        5 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5 bor Mask, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        6 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6 bor Mask, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        7 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7 bor Mask,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        8 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8 bor Mask, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        9 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9 bor Mask, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        10 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10 bor Mask, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        11 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11 bor Mask, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        12 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12 bor Mask, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        13 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13 bor Mask, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        14 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14 bor Mask, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        15 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15 bor Mask,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        16 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16 bor Mask, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        17 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17 bor Mask, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        18 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18 bor Mask, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        19 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19 bor Mask, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        20 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20 bor Mask, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        21 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21 bor Mask, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        22 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22 bor Mask, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        23 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23 bor Mask,
                            S24, S25, S26, S27, S28, S29, S30, S31);
        24 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24 bor Mask, S25, S26, S27, S28, S29, S30, S31);
        25 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25 bor Mask, S26, S27, S28, S29, S30, S31);
        26 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26 bor Mask, S27, S28, S29, S30, S31);
        27 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27 bor Mask, S28, S29, S30, S31);
        28 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28 bor Mask, S29, S30, S31);
        29 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29 bor Mask, S30, S31);
        30 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30 bor Mask, S31);
        31 ->
            add_hashlist(T,
                            SlotSplit, Size,
                            S0, S1, S2, S3, S4, S5, S6, S7,
                            S8, S9, S10, S11, S12, S13, S14, S15,
                            S16, S17, S18, S19, S20, S21, S22, S23,
                            S24, S25, S26, S27, S28, S29, S30, S31 bor Mask)
        
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_orderedkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_orderedkeys(Seqn,
                            Count,
                            [],
                            BucketRangeLow,
                            BucketRangeHigh).

generate_orderedkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_orderedkeys(Seqn, Count, Acc, BucketLow, BucketHigh) ->
    BNumber = Seqn div (BucketHigh - BucketLow),
    BucketExt =
        io_lib:format("K~4..0B", [BucketLow + BNumber]),
    KeyExt =
        io_lib:format("K~8..0B", [Seqn * 100 + leveled_rand:uniform(100)]),
    
    LK = leveled_codec:to_ledgerkey("Bucket" ++ BucketExt, "Key" ++ KeyExt, o),
    Chunk = leveled_rand:rand_bytes(16),
    {_B, _K, MV, _H, _LMs} =
        leveled_codec:generate_ledgerkv(LK, Seqn, Chunk, 64, infinity),
    generate_orderedkeys(Seqn + 1,
                        Count - 1,
                        [{LK, MV}|Acc],
                        BucketLow,
                        BucketHigh).


get_hashlist(N) ->
    KVL = generate_orderedkeys(1, N, 1, 20),
    HashFun =
        fun({K, _V}) ->
            leveled_codec:segment_hash(K)
        end,
    lists:map(HashFun, KVL).

check_all_hashes(BloomBin, HashList, Size) ->
    CheckFun =
        fun(Hash) ->
            ?assertMatch(true, check_hash(Hash, BloomBin, Size))
        end,
    lists:foreach(CheckFun, HashList).
        
check_neg_hashes(BloomBin, HashList, Counters, Size) ->
    CheckFun =
        fun(Hash, {AccT, AccF}) ->
            case check_hash(Hash, BloomBin, Size) of
                true ->
                    {AccT + 1, AccF};
                false ->
                    {AccT, AccF + 1}
            end
        end,
    lists:foldl(CheckFun, Counters, HashList).


empty_bloom_test() ->
    BloomBin0 = create_bloom([]),
    ?assertMatch({0, 4},
                    check_neg_hashes(BloomBin0,
                                        [0, 10, 100, 100000],
                                        {0, 0},
                                        medium)).

bloom_test_() ->
    {timeout, 60, fun bloom_test_ranges/0}.

bloom_test_ranges() ->
    test_bloom(40000, 2, medium),
    test_bloom(128 * 256, 10, medium),
    test_bloom(20000, 2, medium),
    test_bloom(10000, 2, medium),
    test_bloom(5000, 2, medium),
    test_bloom(2000, 2, medium),
    test_bloom(1000, 2, small),
    test_bloom(500, 2, small),
    test_bloom(300, 2,small),
    test_bloom(200, 2,small),
    test_bloom(100, 2,small).

test_bloom(N, Runs, Size) ->
    ListOfHashLists = 
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),
    SpliListFun =
        fun(HashList) -> 
            HitOrMissFun = 
                fun (Entry, {HitL, MissL}) ->
                    case leveled_rand:uniform() < 0.5 of
                        true -> 
                            {[Entry|HitL], MissL};
                        false ->
                            {HitL, [Entry|MissL]}
                    end 
                end,
            lists:foldl(HitOrMissFun, {[], []}, HashList)
        end,
    SplitListOfHashLists = lists:map(SpliListFun, ListOfHashLists),

    SWa = os:timestamp(),
    ListOfBlooms =
        lists:map(fun({HL, _ML}) -> create_bloom(HL, Size) end, 
                    SplitListOfHashLists),
    TSa = timer:now_diff(os:timestamp(), SWa)/Runs,
    
    SWb = os:timestamp(),
    lists:foreach(fun(Nth) ->
                        {HL, _ML} = lists:nth(Nth, SplitListOfHashLists),
                        BB = lists:nth(Nth, ListOfBlooms),
                        check_all_hashes(BB, HL, Size)
                     end,
                     lists:seq(1, Runs)),
    TSb = timer:now_diff(os:timestamp(), SWb)/Runs,
    
    SWc = os:timestamp(),
    {Pos, Neg} = 
        lists:foldl(fun(Nth, Acc) ->
                            {_HL, ML} = lists:nth(Nth, SplitListOfHashLists),
                            BB = lists:nth(Nth, ListOfBlooms),
                            check_neg_hashes(BB, ML, Acc, Size)
                        end,
                        {0, 0},
                        lists:seq(1, Runs)),
    FPR = Pos / (Pos + Neg),
    TSc = timer:now_diff(os:timestamp(), SWc)/Runs,
    
    io:format(user,
                "Test with size ~w has bloom size ~w microsecond timings: -"
                    ++ " build ~w check ~w neg_check ~w and fpr ~w~n",
                [N, Size, TSa, TSb, TSc, FPR]).


-endif.
