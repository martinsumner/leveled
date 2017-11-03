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

-module(lz4_nif).

%% lz4f.
-export([lz4f_compress_frame/2]).
-export([lz4f_create_compression_context/0]).
-export([lz4f_compress_begin/2]).
-export([lz4f_compress_update/2]).
-export([lz4f_flush/1]).
-export([lz4f_compress_end/1]).
-export([lz4f_create_decompression_context/0]).
-export([lz4f_get_frame_info/2]).
-export([lz4f_decompress/2]).

-on_load(on_load/0).
on_load() ->
    case code:priv_dir(leveled) of
        {error, _} ->
            {error, {load_failed, "Could not determine the leveled priv/ directory."}};
        Path ->
            erlang:load_nif(filename:join(Path, atom_to_list(?MODULE)), 0)
    end.

%% lz4f.

lz4f_compress_frame(_, _) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_create_compression_context() ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_compress_begin(_, _) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_compress_update(_, _) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_flush(_) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_compress_end(_) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_create_decompression_context() ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_get_frame_info(_, _) ->
    erlang:nif_error({not_loaded, ?MODULE}).

lz4f_decompress(_, _) ->
    erlang:nif_error({not_loaded, ?MODULE}).
