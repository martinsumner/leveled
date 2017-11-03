%% Copyright (c) 2017-Present Pivotal Software, Inc.  All rights reserved.
%%
%% This package, the LZ4 binding for Erlang, is double-licensed under the Mozilla
%% Public License 1.1 ("MPL") and the Apache License version 2
%% ("ASL"). For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
%% please see LICENSE-APACHE2.
%%
%% This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
%% either express or implied. See the LICENSE file for specific language governing
%% rights and limitations of this software.
%%
%% If you have any questions regarding licensing, please contact us at
%% info@rabbitmq.com.

-module(lz4f).

%% Simple compression.
-export([compress_frame/1]).
-export([compress_frame/2]).

%% Advanced compression.
-export([create_compression_context/0]).
-export([compress_begin/1]).
-export([compress_begin/2]).
-export([compress_update/2]).
-export([flush/1]).
-export([compress_end/1]).

%% Decompression.
-export([create_decompression_context/0]).
-export([get_frame_info/2]).
-export([decompress/1]).
-export([decompress/2]).

-type block_size_id() :: default | max64KB | max256KB | max1MB | max4MB.
-type block_mode() :: linked | independent.
-type frame_type() :: frame | skippable_frame.

-type frame_info() :: #{
    block_size_id => block_size_id(),
    block_mode => block_mode(),
    content_checksum => boolean(),
    frame_type => frame_type(),
    content_size => non_neg_integer()
}.

-type opts() :: #{
    frame_info => frame_info(),
    compression_level => 0..16,
    auto_flush => boolean()
}.

-opaque cctx() :: <<>>. %% Resource.
-export_type([cctx/0]).

-opaque dctx() :: <<>>. %% Resource.
-export_type([dctx/0]).

-spec compress_frame(binary()) -> binary().
compress_frame(Data) ->
    lz4_nif:lz4f_compress_frame(Data, #{}).

-spec compress_frame(binary(), opts()) -> binary().
compress_frame(Data, Opts) ->
    lz4_nif:lz4f_compress_frame(Data, Opts).

-spec create_compression_context() -> cctx().
create_compression_context() ->
    lz4_nif:lz4f_create_compression_context().

-spec compress_begin(cctx()) -> binary().
compress_begin(Cctx) ->
    compress_begin(Cctx, #{}).

-spec compress_begin(cctx(), opts()) -> binary().
compress_begin(Cctx, Opts) ->
    lz4_nif:lz4f_compress_begin(Cctx, Opts).

-spec compress_update(cctx(), binary()) -> binary().
compress_update(Cctx, Data) ->
    lz4_nif:lz4f_compress_update(Cctx, Data).

-spec flush(cctx()) -> binary().
flush(Cctx) ->
    lz4_nif:lz4f_flush(Cctx).

-spec compress_end(cctx()) -> binary().
compress_end(Cctx) ->
    lz4_nif:lz4f_compress_end(Cctx).

-spec create_decompression_context() -> dctx().
create_decompression_context() ->
    lz4_nif:lz4f_create_decompression_context().

-spec get_frame_info(dctx(), binary()) -> {ok, frame_info(), non_neg_integer()}.
get_frame_info(Dctx, Data) ->
    lz4_nif:lz4f_get_frame_info(Dctx, Data).

-spec decompress(binary()) -> iolist().
decompress(Data) ->
    decompress(create_decompression_context(), Data).

-spec decompress(dctx(), binary()) -> iolist().
decompress(Dctx, Data) ->
    lz4_nif:lz4f_decompress(Dctx, Data).

%% @todo LZ4F_resetDecompressionContext
