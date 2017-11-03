// Copyright (c) 2017-Present Pivotal Software, Inc.  All rights reserved.
//
// This package, the LZ4 binding for Erlang, is double-licensed under the Mozilla
// Public License 1.1 ("MPL") and the Apache License version 2
// ("ASL"). For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

#include "lz4_erlang.h"

NIF_ATOMS(NIF_ATOM_DECL)
NIF_RESOURCES(NIF_RES_DECL)

int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info);
void unload(ErlNifEnv* env, void* priv_data);

int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    NIF_ATOMS(NIF_ATOM_INIT)
    NIF_RESOURCES(NIF_RES_INIT)

    return 0;
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    *priv_data = *old_priv_data;

    return 0;
}

void unload(ErlNifEnv* env, void* priv_data)
{
}

static ErlNifFunc nif_funcs[] = {
    NIF_FUNCTIONS(NIF_FUNCTION_ARRAY)
};

ERL_NIF_INIT(lz4_nif, nif_funcs, load, NULL, upgrade, unload)
