%% -------- SST Block Functions ---------
%%
%% Functions to serialise and then fetch from those serialised blocks, i.e.
%% - serialise_block/3
%% - get_all/2 deserialise returning all
%% - get_topandtail/2 return only the first and last elements, as well as a
%% function to return the remainder, so that deserialisation of the remainder
%% may be avoided on inspection of top and tail
%% - get_nth/3 deserialise enough of the block to return just the nth item
%%
%% The fetch functions may be optimised for the block type to minimise the
%% work required to fetch the required amount of deserialised data.
%%
%% Standard block sizes are
%% -define(LOOK_BLOCKSIZE, {24, 32}).
%% -define(NOLOOK_BLOCKSIZE, {56, 32}).
%%
%% Requirement to serialise LOOK_BLOCKS to optimise for picking the nth value
%% Requirement to serialise NOLOOK_BLOCKS to optimise for picking the first and
%% last values

-module(leveled_sstblock).

-include("leveled.hrl").

-define(MAX_SUBBLOCK_SIZE, 1 bsl 16).
-define(BLOCK_TYPE0, 0).
% Block is just a list of terms
-define(BLOCK_TYPE1, 1).
% Lookup block divided into 4 blocks of 6
% 24 KV blocks only
-define(BLOCK_TYPE2, 2).
% Lookup block divided into 4 blocks of 8
% 32 KV blocks only
-define(BLOCK_TYPE3, 3).
% Nolookup block with first/last terms at head
-define(BLOCK_TYPE4, 4).
% Nolookup block with first/last terms at head, and block split into L/M/R
% 56 KV blocks only
-define(COMPRESSION_FACTOR, 1).
% When using native compression - how hard should the compression code
% try to reduce the size of the compressed output. 1 Is to imply minimal
% effort, 6 is default in OTP:
% https://www.erlang.org/doc/man/erlang.html#term_to_binary-2
-define(BINARY_SETTINGS, [{compressed, ?COMPRESSION_FACTOR}]).

-type block_type() ::
    ?BLOCK_TYPE0 | ?BLOCK_TYPE1 | ?BLOCK_TYPE2 | ?BLOCK_TYPE3 | ?BLOCK_TYPE4.
-type range_filter() ::
    all | {leveled_codec:ledger_key(), leveled_codec:ledger_key()}.
-type top_and_tail() ::
    {
        leveled_codec:ledger_key() | not_present,
        leveled_codec:ledger_key() | not_present,
        fun((range_filter()) -> list(leveled_codec:ledger_kv()))
    }.

-export(
    [
        serialise_block/3,
        get_all/2,
        get_topandtail/2,
        get_nth/3
    ]
).

%%%============================================================================
%%% API
%%%============================================================================

%% erlfmt:ignore-begin
-spec serialise_block(
    lookup|no_lookup,
    {leveled_sst:block_version(), leveled_sst:press_method()},
    list(leveled_codec:ledger_kv())) ->
        binary().
serialise_block(
    lookup,
    {1, PressMethod},
    [   A1, A2, A3, A4, A5, A6,
        B1, B2, B3, B4, B5, B6,
        C1, C2, C3, C4, C5, C6,
        D1, D2, D3, D4, D5, D6
    ] = TL
) when PressMethod == lz4; PressMethod == zstd ->
    ABn = term_to_binary([A1, A2, A3, A4, A5, A6]),
    BBn = term_to_binary([B1, B2, B3, B4, B5, B6]),
    CBn = term_to_binary([C1, C2, C3, C4, C5, C6]),
    DBn = term_to_binary([D1, D2, D3, D4, D5, D6]),
    case {byte_size(ABn), byte_size(BBn), byte_size(CBn), byte_size(DBn)} of
        {ASz, BSz, CSz, DSz}
            when
                ASz < ?MAX_SUBBLOCK_SIZE,
                BSz < ?MAX_SUBBLOCK_SIZE,
                CSz < ?MAX_SUBBLOCK_SIZE,
                DSz < ?MAX_SUBBLOCK_SIZE ->
            BlockBin =
                <<
                    ASz:16/integer,
                    BSz:16/integer,
                    CSz:16/integer,
                    DSz:16/integer,
                    ABn/binary,
                    BBn/binary,
                    CBn/binary,
                    DBn/binary
                    >>,
            crc_validate_bin(
                <<
                    (compress_block(BlockBin, PressMethod))/binary,
                    (?BLOCK_TYPE1):8/integer
                >>
            );
        _ ->
            serialise_block_aslist(PressMethod, TL)
    end;
serialise_block(
    lookup,
    {1, PressMethod},
    [
        A1, A2, A3, A4, A5, A6, A7, A8,
        B1, B2, B3, B4, B5, B6, B7, B8,
        C1, C2, C3, C4, C5, C6, C7, C8,
        D1, D2, D3, D4, D5, D6, D7, D8
    ] = TL
) when PressMethod == lz4; PressMethod == zstd ->
    ABn = term_to_binary([A1, A2, A3, A4, A5, A6, A7, A8]),
    BBn = term_to_binary([B1, B2, B3, B4, B5, B6, B7, B8]),
    CBn = term_to_binary([C1, C2, C3, C4, C5, C6, C7, C8]),
    DBn = term_to_binary([D1, D2, D3, D4, D5, D6, D7, D8]),
    case {byte_size(ABn), byte_size(BBn), byte_size(CBn), byte_size(DBn)} of
        {ASz, BSz, CSz, DSz}
            when
                ASz < ?MAX_SUBBLOCK_SIZE,
                BSz < ?MAX_SUBBLOCK_SIZE,
                CSz < ?MAX_SUBBLOCK_SIZE,
                DSz < ?MAX_SUBBLOCK_SIZE ->
            BlockBin =
                <<
                    ASz:16/integer,
                    BSz:16/integer,
                    CSz:16/integer,
                    DSz:16/integer,
                    ABn/binary,
                    BBn/binary,
                    CBn/binary,
                    DBn/binary
                    >>,
            crc_validate_bin(
                <<
                    (compress_block(BlockBin, PressMethod))/binary,
                    (?BLOCK_TYPE2):8/integer
                >>
            );
        _ ->
            serialise_block_aslist(PressMethod, TL)
    end;
serialise_block(
    no_lookup,
    {1, PressMethod},
    [
        L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12,
        L13, L14, L15, L16, L17, L18, L19, L20, L21, L22, L23, L24,
        M1, M2, M3, M4, M5, M6, M7, M8,
        R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12,
        R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24
    ] = TermList
)
        when
            PressMethod == zstd; PressMethod == lz4 ->
    LBn =
        term_to_binary(
            [
                L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12,
                L13, L14, L15, L16, L17, L18, L19, L20, L21, L22, L23, L24
            ]
        ),
    MBn = term_to_binary([M1, M2, M3, M4, M5, M6, M7, M8]),
    RBn =
        term_to_binary(
            [
                R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12,
                R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24
            ]
        ),
    TTBn = term_to_binary({element(1, L1), element(1, R24)}),
    case {byte_size(LBn), byte_size(MBn), byte_size(RBn), byte_size(TTBn)} of
        {LSz, MSz, RSz, TTSz}
            when
                LSz < ?MAX_SUBBLOCK_SIZE,
                MSz < ?MAX_SUBBLOCK_SIZE,
                RSz < ?MAX_SUBBLOCK_SIZE,
                TTSz < ?MAX_SUBBLOCK_SIZE
            ->
                CompressedBin =
                    compress_block(
                        <<
                            LSz:16/integer,
                            MSz:16/integer,
                            RSz:16/integer,
                            LBn/binary,
                            MBn/binary,
                            RBn/binary
                        >>,
                        PressMethod
                    ),

                crc_validate_bin(
                    <<
                        TTSz:16/integer,
                        TTBn/binary,
                        CompressedBin/binary,
                        (?BLOCK_TYPE4):8/integer
                    >>
                );
        _ ->
            serialise_block_aslist(PressMethod, TermList)
    end;
serialise_block(no_lookup, {1, PressMethod}, TermList)
        when
            length(TermList) > 2, 
            PressMethod == zstd; PressMethod == lz4 ->
    TopTail =
        term_to_binary(
            {
                element(1, hd(TermList)),
                element(1, lists:last(TermList))
            }
        ),
    AllBin = compress_block(term_to_binary(TermList), PressMethod),
    case byte_size(TopTail) of
        TTSz when TTSz < ?MAX_SUBBLOCK_SIZE ->
            crc_validate_bin(
                <<
                    TTSz:16/integer,
                    TopTail/binary,
                    AllBin/binary,
                    (?BLOCK_TYPE3):8/integer
                >>
            );
        _ ->
            serialise_block_aslist(PressMethod, TermList)
    end;
serialise_block(_, {1, PressMethod}, TermList) ->
    serialise_block_aslist(PressMethod, TermList);
serialise_block(_, {0, PressMethod}, TermList) ->
    serialise_block(TermList, PressMethod).
%% erlfmt:ignore-end

-spec get_all(
    binary(), leveled_sst:block_method()
) ->
    list(leveled_codec:ledger_kv()).
get_all(Block, {1, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            get_all_block(CheckedBlock, PressMethod)
        end,
    check_block(Block, [], ExtractFun);
get_all(Block, {0, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            deserialise_checkedblock(CheckedBlock, PressMethod)
        end,
    check_block(Block, [], ExtractFun).

-spec get_topandtail(
    binary(), leveled_sst:block_method()
) -> top_and_tail().
get_topandtail(Block, {0, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            TL = deserialise_checkedblock(CheckedBlock, PressMethod),
            {
                element(1, hd(TL)),
                element(1, lists:last(TL)),
                fun(_) -> TL end
            }
        end,
    check_block(
        Block,
        {not_present, not_present, fun(_) -> [] end},
        ExtractFun
    );
get_topandtail(Block, {1, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            get_topandtail_block(CheckedBlock, PressMethod)
        end,
    check_block(
        Block,
        {not_present, not_present, fun(_) -> [] end},
        ExtractFun
    ).

-spec get_nth(
    pos_integer(), binary(), leveled_sst:block_method()
) ->
    leveled_codec:ledger_kv() | not_present.
get_nth(N, Block, {1, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            get_nth_item(N, CheckedBlock, PressMethod)
        end,
    check_block(Block, not_present, ExtractFun);
get_nth(N, Block, {0, PressMethod}) ->
    ExtractFun =
        fun(CheckedBlock) ->
            lists:nth(
                N,
                deserialise_checkedblock(CheckedBlock, PressMethod)
            )
        end,
    check_block(Block, not_present, ExtractFun).

%%%============================================================================
%%% General internal functions - v1
%%%============================================================================

-spec crc_validate_bin(binary()) -> binary().
crc_validate_bin(Bin) ->
    CRC32 = leveled_sst:hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>.

-spec serialise_block_aslist(
    leveled_sst:press_method(), list(leveled_codec:ledger_kv())
) ->
    binary().
serialise_block_aslist(PM, TermList) when PM == lz4; PM == zstd ->
    CompressedBin =
        <<
            (compress_block(term_to_binary(TermList), PM))/binary,
            (?BLOCK_TYPE0):8/integer
        >>,
    crc_validate_bin(CompressedBin);
serialise_block_aslist(native, TermList) ->
    CompressedBin =
        <<
            (term_to_binary(TermList, ?BINARY_SETTINGS))/binary,
            ?BLOCK_TYPE0:8/integer
        >>,
    crc_validate_bin(CompressedBin);
serialise_block_aslist(none, TermList) ->
    UncompressedBin =
        <<(term_to_binary(TermList))/binary, ?BLOCK_TYPE0:8/integer>>,
    crc_validate_bin(UncompressedBin).

-spec compress_block(binary(), lz4 | zstd) -> binary().
compress_block(BlockBin, lz4) ->
    {ok, Bin} = lz4:pack(BlockBin),
    Bin;
compress_block(BlockBin, zstd) ->
    zstd:compress(BlockBin).

-spec decompress_block(binary(), lz4 | zstd) -> binary().
decompress_block(BlockBin, lz4) ->
    {ok, Bin} = lz4:unpack(BlockBin),
    Bin;
decompress_block(BlockBin, zstd) ->
    case zstd:decompress(BlockBin) of
        DeflateBin when is_binary(DeflateBin) ->
            DeflateBin
    end.

-spec check_block
    (binary(), list(), fun((binary()) -> list(leveled_codec:ledger_kv()))) ->
        list(leveled_codec:ledger_kv());
    (binary(), not_present, fun((binary()) -> leveled_codec:ledger_kv())) ->
        leveled_codec:ledger_kv() | not_present;
    (binary(), top_and_tail(), fun((binary()) -> top_and_tail())) ->
        top_and_tail().
check_block(Block, Default, ExtractFun) when byte_size(Block) > 4 ->
    BinS = byte_size(Block) - 4,
    <<TermBin:BinS/binary, CRC32:32/integer>> = Block,
    try
        CRC32 = leveled_sst:hmac(TermBin),
        ExtractFun(TermBin)
    catch
        _Exception:Reason ->
            ?STD_LOG(sst15, [Reason]),
            Default
    end;
check_block(_Block, Default, _ExtractFun) ->
    Default.

%%%============================================================================
%%% Block-type specific cases - v1
%%%============================================================================

%% erlfmt:ignore-begin
-spec get_topandtail_block(
    binary(), leveled_sst:press_method()) -> top_and_tail().
get_topandtail_block(CheckedBlock, PressMethod) ->
    CheckedSize = byte_size(CheckedBlock),
    <<TypedBlock:(CheckedSize - 1)/binary, Type:8/integer>> = CheckedBlock,
    get_topandtail_block(Type, TypedBlock, PressMethod).

-spec get_topandtail_block(
    block_type(), binary(), leveled_sst:press_method()) ->
        top_and_tail().
get_topandtail_block(Type, TypedBlock, PM) when Type == ?BLOCK_TYPE3 ->
    <<TTSz:16/integer, TopTail:TTSz/binary, _/binary>> = TypedBlock,
    {Top, Tail} = binary_to_term(TopTail),
    {
        Top,
        Tail,
        fun(_) -> get_all_block(?BLOCK_TYPE3, TypedBlock, PM) end
    };
get_topandtail_block(Type, TypedBlock, PM)
        when
            Type == ?BLOCK_TYPE4 andalso
            (PM == lz4 orelse PM == zstd) ->
    <<
        TTSz:16/integer,
        TopTail:TTSz/binary,
        CompressedBin/binary
    >> = TypedBlock,
    {Top, Tail} = binary_to_term(TopTail),
    FetchFun =
        fun(Range) ->
            <<
                LSz:16/integer,
                MSz:16/integer,
                RSz:16/integer,
                LBn:LSz/binary,
                MBn:MSz/binary,
                RBn:RSz/binary
            >> = decompress_block(CompressedBin, PM),
            [M1, M2, M3, M4, M5, M6, M7, M8] = binary_to_term(MBn),
            BlockNeeds =
                case Range of
                    all ->
                        all_blocks;
                    {SK, EK} ->
                        leveled_sst:filterby_midblock(
                            {
                                element(1, M1),
                                element(1, M8)
                            },
                            {SK, EK}
                        )
                end,
            case BlockNeeds of
                lt_mid ->
                    binary_to_term(LBn);
                le_mid ->
                    [
                        L1, L2, L3, L4, L5, L6,
                        L7, L8, L9, L10, L11, L12,
                        L13, L14, L15, L16, L17, L18,
                        L19, L20, L21, L22, L23, L24
                    ] = binary_to_term(LBn),
                    [
                        L1, L2, L3, L4, L5, L6,
                        L7, L8, L9, L10, L11, L12,
                        L13, L14, L15, L16, L17, L18,
                        L19, L20, L21, L22, L23, L24,
                        M1, M2, M3, M4, M5, M6, M7, M8 
                    ];
                mid_only ->
                    [M1, M2, M3, M4, M5, M6, M7, M8];
                ge_mid ->
                    [
                        R1, R2, R3, R4, R5, R6,
                        R7, R8, R9, R10, R11, R12,
                        R13, R14, R15, R16, R17, R18,
                        R19, R20, R21, R22, R23, R24
                    ] = binary_to_term(RBn),
                    [
                        M1, M2, M3, M4, M5, M6, M7, M8,
                        R1, R2, R3, R4, R5, R6,
                        R7, R8, R9, R10, R11, R12,
                        R13, R14, R15, R16, R17, R18,
                        R19, R20, R21, R22, R23, R24
                    ];
                gt_mid ->
                    binary_to_term(RBn);
                _ ->
                    [
                        L1, L2, L3, L4, L5, L6,
                        L7, L8, L9, L10, L11, L12,
                        L13, L14, L15, L16, L17, L18,
                        L19, L20, L21, L22, L23, L24
                    ] = binary_to_term(LBn),
                    [
                        R1, R2, R3, R4, R5, R6,
                        R7, R8, R9, R10, R11, R12,
                        R13, R14, R15, R16, R17, R18,
                        R19, R20, R21, R22, R23, R24
                    ] = binary_to_term(RBn),
                    [
                        L1, L2, L3, L4, L5, L6,
                        L7, L8, L9, L10, L11, L12,
                        L13, L14, L15, L16, L17, L18,
                        L19, L20, L21, L22, L23, L24,
                        M1, M2, M3, M4, M5, M6, M7, M8,
                        R1, R2, R3, R4, R5, R6,
                        R7, R8, R9, R10, R11, R12,
                        R13, R14, R15, R16, R17, R18,
                        R19, R20, R21, R22, R23, R24
                    ]
            end
        end,
    {Top, Tail, FetchFun};
get_topandtail_block(Type, TypedBlock, PM) ->
    TL = get_all_block(Type, TypedBlock, PM),
    {element(1, hd(TL)), element(1, lists:last(TL)), fun(_) -> TL end}.

-spec get_nth_item(
    pos_integer(), binary(), leveled_sst:press_method()) ->
        leveled_codec:ledger_kv().
get_nth_item(N, CheckedBlock, PressMethod) ->
    CheckedSize = byte_size(CheckedBlock),
    <<TypedBlock:(CheckedSize - 1)/binary, Type:8/integer>> = CheckedBlock,
    get_nth_item(Type, N, TypedBlock, PressMethod).

-spec get_nth_item(
    block_type(), pos_integer(), binary(), leveled_sst:press_method()) ->
        leveled_codec:ledger_kv().
get_nth_item(Type, N, TypedBlock, PM)
        when Type == ?BLOCK_TYPE0, (PM == zstd orelse PM == lz4) ->
    lists:nth(N, deserialise_checkedblock(TypedBlock, PM));
get_nth_item(Type, N, TypedBlock, _PM) when Type == ?BLOCK_TYPE0 ->
    lists:nth(N, binary_to_term(TypedBlock));
get_nth_item(Type, N, TypedBlock, PressMethod)
        when 
            (Type == ?BLOCK_TYPE1 orelse Type == ?BLOCK_TYPE2 ) andalso
            (PressMethod == lz4 orelse PressMethod == zstd) ->
    Width = case Type of ?BLOCK_TYPE1 -> 6; ?BLOCK_TYPE2 -> 8 end,
    <<
        ASz:16/integer,
        BSz:16/integer,
        CSz:16/integer,
        DSz:16/integer,
        ABn:ASz/binary,
        BBn:BSz/binary,
        CBn:CSz/binary,
        DBn:DSz/binary
    >> = decompress_block(TypedBlock, PressMethod),
    case N of 
        N when N =< Width ->
            lists:nth(N, binary_to_term(ABn));
        N when N =< (2 * Width) ->
            lists:nth(N - Width, binary_to_term(BBn));
        N when N =< (3 * Width) ->
            lists:nth(N - (2 * Width), binary_to_term(CBn));
        N ->
            lists:nth(N - (3 * Width), binary_to_term(DBn))
    end.

-spec get_all_block(
    binary(), leveled_sst:press_method()) ->
        list(leveled_codec:ledger_kv()).
get_all_block(CheckedBlock, PressMethod) ->
    CheckedSize = byte_size(CheckedBlock),
    <<TypedBlock:(CheckedSize - 1)/binary, Type:8/integer>> = CheckedBlock,
    get_all_block(Type, TypedBlock, PressMethod).

-spec get_all_block(
        block_type(), binary(), leveled_sst:press_method()) ->
            list(leveled_codec:ledger_kv()).
get_all_block(Type, TypedBlock, PM)
        when Type == ?BLOCK_TYPE0, (PM == zstd orelse PM == lz4) ->
    deserialise_checkedblock(TypedBlock, PM);
get_all_block(Type, TypedBlock, _PM) when Type == ?BLOCK_TYPE0 ->
    binary_to_term(TypedBlock);
get_all_block(Type, TypedBlock, PM) when Type == ?BLOCK_TYPE3 ->
    <<
        TTSz:16/integer,
        _TopTail:TTSz/binary,
        AllBin/binary
    >> = TypedBlock,
    get_all_block(?BLOCK_TYPE0, AllBin, PM);
get_all_block(Type, TypedBlock, PM)
        when
            Type == ?BLOCK_TYPE4 andalso
            (PM == lz4 orelse PM == zstd) ->
    <<
        TTSz:16/integer,
        _TopTail:TTSz/binary,
        CompressedBin/binary
    >> = TypedBlock,
    <<
        LSz:16/integer,
        MSz:16/integer,
        RSz:16/integer,
        LBn:LSz/binary,
        MBn:MSz/binary,
        RBn:RSz/binary
    >> = decompress_block(CompressedBin, PM),
    [
        L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12,
        L13, L14, L15, L16, L17, L18, L19, L20, L21, L22, L23, L24
    ] = binary_to_term(LBn),
    [
        M1, M2, M3, M4, M5, M6, M7, M8
    ] = binary_to_term(MBn),
    [
        R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12,
        R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24
    ] = binary_to_term(RBn),
    [
        L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12,
        L13, L14, L15, L16, L17, L18, L19, L20, L21, L22, L23, L24,
        M1, M2, M3, M4, M5, M6, M7, M8,
        R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12,
        R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24
    ];
get_all_block(Type, TypedBlock, PM)
        when
            Type == ?BLOCK_TYPE1,
            (PM == lz4 orelse PM == zstd) ->
    <<
        ASz:16/integer,
        BSz:16/integer,
        CSz:16/integer,
        DSz:16/integer,
        ABn:ASz/binary,
        BBn:BSz/binary,
        CBn:CSz/binary,
        DBn:DSz/binary
    >> = decompress_block(TypedBlock, PM),
    [A1, A2, A3, A4, A5, A6] = binary_to_term(ABn),
    [B1, B2, B3, B4, B5, B6] = binary_to_term(BBn),
    [C1, C2, C3, C4, C5, C6] = binary_to_term(CBn),
    [D1, D2, D3, D4, D5, D6] = binary_to_term(DBn),
    [
        A1, A2, A3, A4, A5, A6,
        B1, B2, B3, B4, B5, B6,
        C1, C2, C3, C4, C5, C6,
        D1, D2, D3, D4, D5, D6
    ];
get_all_block(Type, TypedBlock, PM)
        when
            Type == ?BLOCK_TYPE2,
            (PM == lz4 orelse PM == zstd) ->
    <<
        ASz:16/integer,
        BSz:16/integer,
        CSz:16/integer,
        DSz:16/integer,
        ABn:ASz/binary,
        BBn:BSz/binary,
        CBn:CSz/binary,
        DBn:DSz/binary
    >> = decompress_block(TypedBlock, PM),
    [A1, A2, A3, A4, A5, A6, A7, A8] = binary_to_term(ABn),
    [B1, B2, B3, B4, B5, B6, B7, B8] = binary_to_term(BBn),
    [C1, C2, C3, C4, C5, C6, C7, C8] = binary_to_term(CBn),
    [D1, D2, D3, D4, D5, D6, D7, D8] = binary_to_term(DBn),
    [
        A1, A2, A3, A4, A5, A6, A7, A8,
        B1, B2, B3, B4, B5, B6, B7, B8,
        C1, C2, C3, C4, C5, C6, C7, C8,
        D1, D2, D3, D4, D5, D6, D7, D8
    ].
%% erlfmt:ignore-end

%%%============================================================================
%%% Internal functions - v0
%%%============================================================================

deserialise_checkedblock(Bin, lz4) when is_binary(Bin) ->
    case lz4:unpack(Bin) of
        {ok, Bin0} when is_binary(Bin0) ->
            binary_to_term(Bin0)
    end;
deserialise_checkedblock(Bin, zstd) when is_binary(Bin) ->
    case zstd:decompress(Bin) of
        Bin0 when is_binary(Bin0) ->
            binary_to_term(Bin0)
    end;
deserialise_checkedblock(Bin, _Other) when is_binary(Bin) ->
    % native or none can be treated the same
    binary_to_term(Bin).

-spec serialise_block(any(), leveled_sst:press_method()) -> binary().
%% @doc
%% Convert term to binary
%% Function split out to make it easier to experiment with different
%% compression methods.  Also, perhaps standardise applictaion of CRC
%% checks
serialise_block(Term, lz4) ->
    {ok, Bin} = lz4:pack(term_to_binary(Term)),
    CRC32 = leveled_sst:hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>;
serialise_block(Term, native) ->
    Bin = term_to_binary(Term, ?BINARY_SETTINGS),
    CRC32 = leveled_sst:hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>;
serialise_block(Term, zstd) ->
    Bin = zstd:compress(term_to_binary(Term)),
    CRC32 = leveled_sst:hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>;
serialise_block(Term, none) ->
    Bin = term_to_binary(Term),
    CRC32 = leveled_sst:hmac(Bin),
    <<Bin/binary, CRC32:32/integer>>.

%%%============================================================================
%%% eunit tests
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

v1_block_test() ->
    v1_block_tester(lookup, {1, zstd}, 24),
    v1_block_tester(lookup, {1, zstd}, 25),
    v1_block_tester(lookup, {1, native}, 32),
    v1_block_tester(lookup, {1, native}, 31),
    v1_block_tester(no_lookup, {1, zstd}, 24).

v1_bigblock_test() ->
    BigBlob = crypto:strong_rand_bytes(16384),
    {MegaSec, Sec, MicroSec} = os:timestamp(),
    MetaBin =
        <<
            MegaSec:32/integer,
            Sec:32/integer,
            MicroSec:32/integer,
            BigBlob/binary
        >>,
    MetaLen = byte_size(MetaBin),
    SibMetaBin =
        <<
            1:32/integer,
            0:32/integer,
            MetaLen:32/integer,
            MetaBin/binary
        >>,
    v1_block_tester(lookup, {1, zstd}, 24, SibMetaBin),
    v1_block_tester(lookup, {1, zstd}, 32, SibMetaBin),
    v1_block_tester(lookup, {1, zstd}, 25, SibMetaBin),
    v1_block_tester(lookup, {1, native}, 32, SibMetaBin),
    v1_block_tester(lookup, {1, native}, 31, SibMetaBin),
    v1_block_tester(no_lookup, {1, zstd}, 24, SibMetaBin).

v1_nolookup_bigtail_test() ->
    BigBlob = crypto:strong_rand_bytes(1024),
    BigBucket = base64:encode(crypto:strong_rand_bytes(65536)),
    {MegaSec, Sec, MicroSec} = os:timestamp(),
    MetaBin =
        <<
            MegaSec:32/integer,
            Sec:32/integer,
            MicroSec:32/integer,
            BigBlob/binary
        >>,
    MetaLen = byte_size(MetaBin),
    SibMetaBin =
        <<
            1:32/integer,
            0:32/integer,
            MetaLen:32/integer,
            MetaBin/binary
        >>,
    v1_block_tester(no_lookup, {1, zstd}, 24, SibMetaBin, BigBucket),
    v1_block_tester(no_lookup, {1, zstd}, 56, SibMetaBin, BigBucket).

v1_block_tester(Lookup, BlockMethod, BlockSize) ->
    v1_block_tester(
        Lookup,
        BlockMethod,
        BlockSize,
        <<1:32/integer, 0:32/integer, 0:32/integer>>
    ).

v1_block_tester(Lookup, BlockMethod, BlockSize, SibMetaBin) ->
    v1_block_tester(Lookup, BlockMethod, BlockSize, SibMetaBin, <<"Bucket">>).

v1_block_tester(Lookup, BlockMethod, BlockSize, SibMetaBin, B) ->
    V =
        leveled_head:riak_metadata_to_binary(
            term_to_binary([{"actor1", 1}]),
            SibMetaBin
        ),
    GenKeyFun =
        fun(X) ->
            LK =
                {?RIAK_TAG, B, list_to_binary("Key" ++ integer_to_list(X)),
                    null},
            LKV =
                leveled_codec:generate_ledgerkv(
                    LK, X, V, byte_size(V), infinity
                ),
            {_Bucket, _Key, MetaValue, _Hashes, _LastMods} = LKV,
            {LK, MetaValue}
        end,
    KVL = lists:map(GenKeyFun, lists:seq(1, BlockSize)),
    Block = serialise_block(Lookup, BlockMethod, KVL),
    case Lookup of
        lookup ->
            LKV1 = get_nth(1, Block, BlockMethod),
            ?assertMatch(LKV1, hd(KVL)),
            LKV6 = get_nth(6, Block, BlockMethod),
            ?assertMatch(LKV6, lists:nth(6, KVL)),
            LKV7 = get_nth(7, Block, BlockMethod),
            ?assertMatch(LKV7, lists:nth(7, KVL)),
            LKV24 = get_nth(24, Block, BlockMethod),
            ?assertMatch(LKV24, lists:nth(24, KVL));
        no_lookup ->
            ok
    end,
    {Top, Tail, AllFun} = get_topandtail(Block, BlockMethod),
    ?assertMatch(Top, element(1, hd(KVL))),
    ?assertMatch(Tail, element(1, lists:last(KVL))),
    ?assertMatch(KVL, AllFun(all)),
    ?assertMatch(KVL, get_all(Block, BlockMethod)).

-endif.
