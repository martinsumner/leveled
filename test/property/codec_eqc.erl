
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% leveled_codec Binary Encoding Specification
%%
%% All integers are big-endian unsigned unless otherwise noted.
%% All fields are contiguous with no padding or alignment.
%% "EXT" denotes Erlang External Term Format (OTP term_to_binary).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% ==============================================================
%% 1.  Ledger Value
%%
%% Historically stored as Erlang tuples (versions 1 and 2).
%% Version 3 uses the pure-binary format below.
%% ==============================================================

%% ledger-value       = ledger-value-v1    ; legacy: 4-tuple  {sqn,status,hash,md}
%%                    / ledger-value-v2    ; legacy: 5-tuple  {sqn,status,hash,md,lmd}
%%                    / ledger-value-v3    ; current binary format
%%
%% ledger-value-v3    = LV3-VERSION
%%                      LV3-STATUS
%%                      LV3-SEG-HASH
%%                      LV3-LMD
%%                      LV3-SQN
%%                      LV3-UMD
%%
%% LV3-VERSION        = %x03              ; literal version tag
%%
%% LV3-STATUS         = status-active-inf
%%                    / status-tomb
%%                    / status-active-ttl
%%
%% status-active-inf  = %x00              ; high-nibble=0 (active), low-nibble=0 (no TTL length)
%% status-tomb        = %x10              ; high-nibble=1 (tomb),   low-nibble=0
%%
%% TTL: high-nibble=2, low-nibble=L, followed by L bytes of timestamp
%% status-active-ttl  = ttl-header *OCTET ; *OCTET length is low-nibble of ttl-header
%% ttl-header         = %x21-2F           ; byte = (0x2 << 4) | L, L in 1..15
%%                                        ; L = byte_size(binary:encode_unsigned(TTL))
%%                                        ; TTL expressed as seconds since Unix epoch
%%
%% LV3-SEG-HASH       = seg-hash-present
%%                    / seg-hash-absent
%%
%% seg-hash-present   = %x00 seg-hash-lo extra-hash
%% seg-hash-lo        = 2OCTET            ; 16-bit segment hash
%% extra-hash         = 4OCTET            ; 32-bit extra hash
%%
%% seg-hash-absent    = %x01              ; no-lookup marker (index entries)
%%
%% LV3-LMD            = lmd-absent
%%                    / lmd-present
%%
%% lmd-absent         = %x00              ; undefined / not recorded
%% lmd-present        = lmd-length lmd-value
%% lmd-length         = OCTET             ; L in 1..4 (epoch seconds fit in 4 bytes
%%                                        ; until year 2106; encoded as minimum bytes)
%% lmd-value          = 1*4OCTET          ; big-endian unsigned integer, L bytes
%%
%% LV3-SQN            = sqn-length sqn-value
%% sqn-length         = OCTET             ; L = byte_size(binary:encode_unsigned(SQN))
%% sqn-value          = 1*OCTET           ; big-endian unsigned integer, L bytes
%%
%% %% LV3-UMD            = umd-absent
%%                    / umd-present
%% 
%% umd-absent         = %x00
%% umd-present        = %x01 umd-length umd-bytes
%% umd-length         = 3OCTET            ; 24-bit big-endian byte count N, N < 16777216
%% umd-bytes          = 1*OCTET           ; N bytes of Erlang EXT (term_to_binary/1)
%%                                        ; WARNING: no size guard in create_v3_value
%%                                        ; if byte_size(EXT) >= 2^24 the encoder crashes

-module(codec_eqc).

-ifdef(EQC).

-compile([export_all, nowarn_export_all]).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_prop1_test_() ->
  {timeout,
      ?EQC_TIME_BUDGET + 10,
      ?_assertEqual(
          true,
          eqc:quickcheck(
              eqc:testing_time(?EQC_TIME_BUDGET, ?QC_OUT(prop_value_versions()))))}.

%% generators

pos() ->
     choose(1, 16#ffff).

ledger_status() ->
    oneof([tomb, {active, nat()}, {active, infinity}]).

ledger_seg_hash() ->
    oneof([no_lookup, {choose(0, 16#ffff), choose(0, 16#ffff)}]).

ledger_metadata() ->
    %% for any() take just bool() for the moment
    oneof([{int(), int()}, null, bool()]).

ledger_last_moddate() ->
    oneof([undefined, ?LET(N, choose(-16#ffff, 16#ffff), N + 1786520336)]).


%% From type definition in leveled_codec.erl:
%% -type ledger_value_v2() ::  {sqn(), ledger_status(), segment_hash(), metadata(), last_moddate()}.
prop_value_versions() ->
  ?FORALL(
      {Sqn, Status, SegHash, MD, Lmd},
      {pos(), ledger_status(), ledger_seg_hash(), ledger_metadata(), ledger_last_moddate()},
      begin
        VV1 = {Sqn, Status, SegHash, MD},
        VV2 = {Sqn, Status, SegHash, MD, Lmd},
        VV3 = leveled_codec:create_v3_value(Sqn, Status, SegHash, MD, Lmd),
        conjunction([
            {sqn1, equals(leveled_codec:ledgermd_sqn(VV1), Sqn)}, 
            {sqn2, equals(leveled_codec:ledgermd_sqn(VV2), Sqn)},
            {sqn3, equals(leveled_codec:ledgermd_sqn(VV3), Sqn)},
            {seg_hash1, equals(leveled_codec:ledgermd_seg(VV1), SegHash)},
            {seg_hash2, equals(leveled_codec:ledgermd_seg(VV2), SegHash)},
            {seg_hash3, equals(leveled_codec:ledgermd_seg(VV3), SegHash)},
            {seg_hashlmd2, equals(leveled_codec:ledgermd_seglmd(VV2), {SegHash, Lmd})},
            {seg_hashlmd3, equals(leveled_codec:ledgermd_seglmd(VV3), {SegHash, Lmd})},
            {status_and_sqn1, equals(leveled_codec:ledgermd_statussqn(VV1), {Status, Sqn})},
            {status_and_sqn2, equals(leveled_codec:ledgermd_statussqn(VV2), {Status, Sqn})},
            {status_and_sqn3, equals(leveled_codec:ledgermd_statussqn(VV3), {Status, Sqn})},
            {status_lmd2, equals(leveled_codec:ledgermd_statuslmd(VV2), {Status, Lmd})},
            {status_lmd3, equals(leveled_codec:ledgermd_statuslmd(VV3), {Status, Lmd})},
            {status_sqn_umd1, equals(leveled_codec:ledgermd_statussqnumd(VV1), {Status, Sqn, MD})},
            {status_sqn_umd2, equals(leveled_codec:ledgermd_statussqnumd(VV2), {Status, Sqn, MD})},
            {status_sqn_umd3, equals(leveled_codec:ledgermd_statussqnumd(VV3), {Status, Sqn, MD})},
            {sqn_umd1, equals(leveled_codec:ledgermd_sqnumd(VV1), {Sqn, MD})},
            {sqn_umd2, equals(leveled_codec:ledgermd_sqnumd(VV2), {Sqn, MD})},
            {sqn_umd3, equals(leveled_codec:ledgermd_sqnumd(VV3), {Sqn, MD})},
            {umd1, equals(leveled_codec:ledgermd_umd(VV1), MD)},
            {umd2, equals(leveled_codec:ledgermd_umd(VV2), MD)},
            {umd3, equals(leveled_codec:ledgermd_umd(VV3), MD)},          
            {status1, equals(leveled_codec:ledgermd_status(VV1), Status)},
            {status2, equals(leveled_codec:ledgermd_status(VV2), Status)},
            {status3, equals(leveled_codec:ledgermd_status(VV3), Status)}
        ])
      end).

-endif.
