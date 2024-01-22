#include "erl_nif.h"
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>

#define PRIu16 "hu"

static ERL_NIF_TERM leveled_atom_error;
static ERL_NIF_TERM leveled_atom_enomem;

bool
check_int(uint16_t checker, unsigned char *bin, size_t size, uint16_t max)
{
    int i;
    uint16_t member;

    if( checker > max )
        return false;

    for (i = 0; i < size / 2; i++) {
        member = ((bin[i * 2] << 8) + bin[(i * 2) + 1]);
        if( checker == member )
            return true;
        else
            if( checker < member )
                return false;
    }

    return false;
        
}

unsigned char *
find_pos(
    unsigned char *posbin, unsigned char *checkbin,
    size_t possize, size_t checksize, uint16_t max,
    unsigned char *positions)
{
    int i;
    uint8_t pos;
    uint8_t found;
    uint16_t hash;

    pos = 1;
    found = 0;

    for (i = 0; i < possize; i++) {
        if( posbin[i] > 127 ) {
            hash = (posbin[i] << 8) + posbin[i + 1];
            if( check_int(hash, checkbin, checksize, max) ) {
                * (positions + found) = pos;
                ++pos;
                ++found;
                ++i;
            }
            else {
                ++pos;
                ++i;
            }
            }
        else
            pos = pos + posbin[i] + 1;
    }

    return positions;
}

static uint32_t
magic_hash(unsigned char *bin, size_t size)
{
    uint32_t hash = 5381;
    int i;

    for (i = 0; i < size; i++)
        hash = ((hash << 5) + hash) ^ bin[i];

    return hash;
}

static ERL_NIF_TERM
nif_hash(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary in;

    if (!enif_inspect_binary(env, argv[0], &in)) {
    return enif_make_badarg(env);
  }

    return enif_make_uint(env, magic_hash(in.data, in.size));
    
}

static ERL_NIF_TERM
nif_checker(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary in;
    unsigned int checker;
    unsigned int max;

    if (!enif_inspect_binary(env, argv[0], &in)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[1], &checker)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[2], &max)) {
        return enif_make_badarg(env);
    }
    if( checker < 0 || checker > UINT16_MAX) {
        return enif_make_badarg(env);
    }
    if( in.size % 2 == 1) {
        return enif_make_badarg(env);
    }

    if( check_int(checker, in.data, in.size, max) == 1 )
        return enif_make_atom(env, "true");
    else
        return enif_make_atom(env, "false");

}

static ERL_NIF_TERM
nif_findpos(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary posbin, checkbin;
    ERL_NIF_TERM ret;
    unsigned int max;
    unsigned char *positions;

    if (!enif_inspect_binary(env, argv[0], &posbin)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &checkbin)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[2], &max)) {
        return enif_make_badarg(env);
    }
    if( checkbin.size % 2 == 1) {
        return enif_make_badarg(env);
    }

    positions = calloc(128, 1);
    if( positions == NULL ) {
        return enif_make_tuple2(env, leveled_atom_error, leveled_atom_enomem);
    }
    positions =
        find_pos(
            posbin.data, checkbin.data, posbin.size, checkbin.size, max,
            positions);
    ret = enif_make_string(env, positions, ERL_NIF_LATIN1);
    free(positions);
    return ret;
}

static ErlNifFunc nif_funcs[] =
{
    {"magic_chash", 1, nif_hash},
    {"int_checker", 3, nif_checker},
    {"pos_finder", 3, nif_findpos}
};

ERL_NIF_INIT(leveled_nif, nif_funcs, NULL, NULL, NULL, NULL);