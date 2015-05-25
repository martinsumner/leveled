%%
%% This module provides functions for managing bst files - a modified version
%% of sst files, to be used in leveleddb.
%% bst files are borken into the following sections:
%% - Header (fixed width 32 bytes - containing pointers and metadata)
%% - Blocks (variable length)
%% - Slots (variable length)
%% - Footer (variable length - contains slot index and helper metadata)
%%
%% The 32-byte header is made up of
%% - 1 byte version (major 5 bits, minor 3 bits) - default 0.1
%% - 1 byte state bits (1 bit to indicate mutability, 1 for use of compression)
%% - 4 bytes footer position
%% - 4 bytes slot list length
%% - 4 bytes helper length
%% - 14 bytes spare for future options
%% - 4 bytes CRC (header)  
%%
%% The Blocks is a series of blocks of: 
%% - 4 byte block length
%% - variable-length compressed list of 32 keys & values
%% - 4 byte CRC for block
%% There will be up to 4000 blocks in a single bst file
%%
%% The slots is a series of references
%% - 4 byte bloom-filter length
%% - 4 byte key-helper length 
%% - a variable-length compressed bloom filter for all keys in slot (approx 1KB)
%% - 32 ordered variable-length key helpers pointing to first key in each 
%% block (in slot) of the form Key Length, Key, Block Position
%% - 4 byte CRC for the slot
%%
%% The slot index in the footer is made up of 128 keys and pointers at the 
%% the start of each slot
%% - 128 Key Length (4 byte), Key, Position (4 byte) indexes
%% - 4 bytes CRC for the index
%%
%% The format of the file is intended to support quick lookups, whilst 
%% allowing for a new file to be written incrementally (so that all keys and
%% values need not be retained in memory) - perhaps n blocks at a time


-module(leveled_bst).

-export([start_file/1, convert_header/1]).

-include_lib("eunit/include/eunit.hrl").

-define(WORD_SIZE, 4).
-define(CURRENT_VERSION, {0,1}).
-define(SLOT_COUNT, 128).
-define(BLOCK_SIZE, 32).
-define(SLOT_SIZE, 32).

-record(metadata, {version = ?CURRENT_VERSION :: tuple(),
					   mutable = false :: true | false,
					   compressed = true :: tre | false,
					   slot_list :: list(),
					   cache :: tuple(),
					   smallest_key :: tuple(),
					   largest_key :: tuple(),
					   smallest_sqn :: integer(),
					   largest_sqn :: integer()
					  }).

%% Start a bare file with an initial header and no further details
%% Return the {Handle, metadata record}
start_file(FileName) when is_list(FileName) ->
	{ok, Handle} = file:open(FileName, [binary, raw, read, write]),
	start_file(Handle);
start_file(Handle) -> 
	Header = create_header(initial),
	{ok, _} = file:position(Handle, bof),
	file:write(Handle, Header),
	{Version, {M, C}, _, _} = convert_header(Header),
	FileMD = #metadata{version=Version, mutable=M, compressed=C},
	SlotArray = array:new(?SLOT_COUNT),
	{Handle, FileMD, SlotArray}.


create_header(initial) ->
	{Major, Minor} = ?CURRENT_VERSION, 
	Version = <<Major:5, Minor:3>>,
	State = <<0:6, 1:1, 1:1>>, % Mutable and compressed
	Lengths = <<0:32, 0:32, 0:32>>,
	Options = <<0:112>>,
	H1 = <<Version/binary, State/binary, Lengths/binary, Options/binary>>,
	CRC32 = erlang:crc32(H1),
	<<H1/binary, CRC32:32/integer>>.


convert_header(Header) ->
	<<H1:28/binary, CRC32:32/integer>> = Header,
	case erlang:crc32(H1) of 
		CRC32 ->
			<<Major:5/integer, Minor:3/integer, _/binary>> = H1,
			case {Major, Minor} of 
				{0, 1} -> 
					convert_header_v01(H1);
				_ ->
					unknown_version
			end;
		_ ->
			crc_mismatch
	end.

convert_header_v01(Header) ->
	<<_:8, 0:6, Mutable:1, Comp:1, 
	FooterP:32/integer, SlotLng:32/integer, HlpLng:32/integer, 
	_/binary>> = Header,
	case Mutable of 
		1 -> M = true;
		0 -> M = false
	end,
	case Comp of 
		1 -> C = true;
		0 -> C = false
	end,
	{{0, 1}, {M, C}, {FooterP, SlotLng, HlpLng}, none}.




%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  

empty_header_test() ->
	Header = create_header(initial),
	?assertMatch(32, byte_size(Header)),
	<<Major:5, Minor:3, _/binary>> = Header,
	?assertMatch({0, 1}, {Major, Minor}),
	{Version, State, Lengths, Options} = convert_header(Header),
	?assertMatch({0, 1}, Version),
	?assertMatch({true, true}, State),
	?assertMatch({0, 0, 0}, Lengths),
	?assertMatch(none, Options).

bad_header_test() ->
	Header = create_header(initial),
	<<_:1/binary, Rest/binary >> = Header,
	HdrDetails1 = convert_header(<<0:5/integer, 2:3/integer, Rest/binary>>),
	?assertMatch(crc_mismatch, HdrDetails1),
	<<_:1/binary, RestToCRC:27/binary, _:32/integer>> = Header,
	NewHdr1 = <<0:5/integer, 2:3/integer, RestToCRC/binary>>,
	CRC32 = erlang:crc32(NewHdr1),
	NewHdr2 = <<NewHdr1/binary, CRC32:32/integer>>,
	?assertMatch(unknown_version, convert_header(NewHdr2)).

record_onstartfile_test() ->
	{_, FileMD, _} = start_file("onstartfile.bst"),
	?assertMatch({0, 1}, FileMD#metadata.version).




