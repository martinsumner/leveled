-module(lz4f_SUITE).
-compile(export_all).

-import(ct_helper, [config/2]).
-import(ct_helper, [doc/1]).

%% ct.

all() ->
    [{group, all}].

groups() ->
    [{all, [parallel], ct_helper:all(?MODULE)}].

init_per_suite(Config) ->
    [{test_file, config(data_dir, Config) ++ "/pdf_reference_1-7.pdf"}|Config].

end_per_suite(_) ->
    ok.

%% Tests.

compress_frame1(Config) ->
    doc("Use lz4f:compress_frame/1 and then decompress back."),
    {ok, File} = file:read_file(config(test_file, Config)),
    Compressed = lz4f:compress_frame(File),
    File = iolist_to_binary(lz4f:decompress(Compressed)),
    ok.

compress_update(Config) ->
    doc("Stream compress with lz4f:compress_update/2 and then decompress back."),
    {ok, File} = file:read_file(config(test_file, Config)),
    Chunks = do_slice(File),
    Ctx = lz4f:create_compression_context(),
    Begin = lz4f:compress_begin(Ctx),
    CompressedChunks = [lz4f:compress_update(Ctx, C) || C <- Chunks],
    End = lz4f:compress_end(Ctx),
    Compressed = iolist_to_binary([Begin, CompressedChunks, End]),
    File = iolist_to_binary(lz4f:decompress(Compressed)),
    ok.

compress_flush(Config) ->
    doc("Stream compress with some lz4f:flush/1 and then decompress back."),
    {ok, File} = file:read_file(config(test_file, Config)),
    Chunks = do_insert_flush(do_slice(File)),
    Ctx = lz4f:create_compression_context(),
    Begin = lz4f:compress_begin(Ctx),
    CompressedChunks = [case C of
        flush -> lz4f:flush(Ctx);
        _ -> lz4f:compress_update(Ctx, C)
    end || C <- Chunks],
    End = lz4f:compress_end(Ctx),
    Compressed = iolist_to_binary([Begin, CompressedChunks, End]),
    File = iolist_to_binary(lz4f:decompress(Compressed)),
    ok.

decompress(Config) ->
    doc("Compress and then stream decompress with lz4f:decompress/2."),
    {ok, File} = file:read_file(config(test_file, Config)),
    Compressed = lz4f:compress_frame(File),
    Chunks = do_slice(Compressed),
    Ctx = lz4f:create_decompression_context(),
    DecompressedChunks = [lz4f:decompress(Ctx, C) || C <- Chunks],
    File = iolist_to_binary(DecompressedChunks),
    ok.

%% Internal.

do_insert_flush(L) ->
    do_insert_flush(L, 0).

do_insert_flush([], _) ->
    [];
do_insert_flush(L, 5) ->
    [flush|do_insert_flush(L, 0)];
do_insert_flush([H|T], N) ->
    [H|do_insert_flush(T, N + 1)].

do_slice(<<Bin:1000000/binary, R/bits>>) ->
    [Bin|do_slice(R)];
do_slice(Bin) ->
    [Bin].
