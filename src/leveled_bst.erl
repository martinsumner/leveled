%%
%% This module provides functions for managing bst files - a modified version
%% of sst files, to be used in leveleddb.
%% bst files are broken into the following sections:
%% - Header (fixed width 32 bytes - containing pointers and metadata)
%% - Summaries (variable length)
%% - Blocks (variable length)
%% - Slots (variable length)
%% - Footer (variable length - contains slot index and helper metadata)
%%
%% The 64-byte header is made up of
%% - 1 byte version (major 5 bits, minor 3 bits) - default 0.1
%% - 1 byte state bits (1 bit to indicate mutability, 1 for use of compression)
%% - 4 bytes summary length
%% - 4 bytes blocks length
%% - 4 bytes slots length
%% - 4 bytes footer position
%% - 4 bytes slot list length
%% - 4 bytes helper length
%% - 34 bytes spare for future options
%% - 4 bytes CRC (header)  
%%
%% A key in the file is a tuple of {Key, Value/Metadata, Sequence #, State}
%% - Keys are themselves tuples, and all entries must be added to the bst 
%% in key order
%% - Metadata can be null or some fast-access information that may be required
%% in preference to the full value (e.g. vector clocks, hashes).  This could 
%% be a value instead of Metadata should the file be used in an alternate 
%% - Sequence numbers is the integer representing the order which the item
%% was added to the overall database
%% - State can be tomb (for tombstone), active or {timestamp, TS}
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
%% - a variable-length compressed bloom filter for all keys in slot (approx 3KB)
%% - 64 ordered variable-length key helpers pointing to first key in each 
%% block (in slot) of the form Key Length, Key, Block Position
%% - 4 byte CRC for the slot
%% - ulitmately a slot covers 64 x 32 = 2048 keys
%%
%% The slot index in the footer is made up of up to 64 ordered keys and 
%% pointers, with the key being a key at the start of each slot
%% - 1 byte value showing number of keys in slot index
%% - 64 x Key Length (4 byte), Key, Position (4 byte) indexes
%% - 4 bytes CRC for the index
%%
%% The format of the file is intended to support quick lookups, whilst 
%% allowing for a new file to be written incrementally (so that all keys and
%% values need not be retained in memory) - perhaps n blocks at a time


-module(leveled_bst).

-export([start_file/1, convert_header/1, append_slot/4]).

-include_lib("eunit/include/eunit.hrl").

-define(WORD_SIZE, 4).
-define(DWORD_SIZE, 8).
-define(CURRENT_VERSION, {0,1}).
-define(SLOT_COUNT, 64).
-define(BLOCK_SIZE, 32).
-define(SLOT_SIZE, 64).
-define(FOOTERPOS_HEADERPOS, 2).


-record(metadata, {version = ?CURRENT_VERSION :: tuple(),
					   mutable = false :: true | false,
					   compressed = true :: true | false,
					   slot_index,
					   open_slot :: integer(),
					   cache :: tuple(),
					   smallest_key :: tuple(),
					   largest_key :: tuple(),
					   smallest_sqn :: integer(),
					   largest_sqn :: integer()
					  }).

-record(object, {key :: tuple(),
					value,
					sequence_numb :: integer(),
					state}).

%% Start a bare file with an initial header and no further details
%% Return the {Handle, metadata record}
start_file(FileName) when is_list(FileName) ->
	{ok, Handle} = file:open(FileName, [binary, raw, read, write]),
	start_file(Handle);
start_file(Handle) -> 
	Header = create_header(initial),
	{ok, _} = file:position(Handle, bof),
	ok = file:write(Handle, Header),
	{Version, {M, C}, _, _} = convert_header(Header),
	FileMD = #metadata{version = Version, mutable = M, compressed = C, 
	slot_index = array:new(?SLOT_COUNT), open_slot = 0},
	{Handle, FileMD}.


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


%% Append a slot of blocks to the end file, and update the slot index in the
%% file metadata

append_slot(Handle, SortedKVList, SlotCount, FileMD) ->
	{ok, SlotPos} = file:position(Handle, eof),
	{KeyList, BlockIndexBin, BlockBin} = add_blocks(SortedKVList),
	ok = file:write(Handle, BlockBin),
	[TopObject|_] = SortedKVList,
	BloomBin = leveled_rice:create_bloom(KeyList),
	SlotIndex = array:set(SlotCount, 
		{TopObject#object.key, BloomBin, BlockIndexBin, SlotPos}, 
		FileMD#metadata.slot_index),
	{Handle, FileMD#metadata{slot_index=SlotIndex}}.

append_slot_index(Handle, _FileMD=#metadata{slot_index=SlotIndex}) ->
	{ok, FooterPos} = file:position(Handle, eof),
	SlotBin1 = <<?SLOT_COUNT:8/integer>>,
	SlotBin2 = array:foldl(fun slot_folder_write/3, SlotBin1, SlotIndex),
	CRC = erlang:crc32(SlotBin2),
	SlotBin3 = <<CRC:32/integer, SlotBin2/binary>>,
	ok = file:write(Handle, SlotBin3),
	SlotLength = byte_size(SlotBin3),
	Header = <<FooterPos:32/integer, SlotLength:32/integer>>,
	ok = file:pwrite(Handle, ?FOOTERPOS_HEADERPOS, Header).

slot_folder_write(_Index, undefined, Bin) ->
	Bin;
slot_folder_write(_Index, {ObjectKey, _, _, SlotPos}, Bin) ->
	KeyBin = serialise_key(ObjectKey),
	KeyLen = byte_size(KeyBin),
	<<Bin/binary, KeyLen:32/integer, KeyBin/binary, SlotPos:32/integer>>.

slot_folder_read(<<>>, SlotIndex, SlotCount) ->
	io:format("Slot index read with count=~w slots~n", [SlotCount]),
	SlotIndex;
slot_folder_read(SlotIndexBin, SlotIndex, SlotCount) ->
	<<KeyLen:32/integer, Tail1/binary>> = SlotIndexBin,
	<<KeyBin:KeyLen/binary, SlotPos:32/integer, Tail2/binary>> = Tail1,
	slot_folder_read(Tail2, 
		array:set(SlotCount, {load_key(KeyBin), null, null, SlotPos}, SlotIndex),
		SlotCount + 1).

read_slot_index(SlotIndexBin) ->
	<<CRC:32/integer, SlotIndexBin2/binary>> = SlotIndexBin,
	case erlang:crc32(SlotIndexBin2) of 
		CRC ->
			<<SlotCount:8/integer, SlotIndexBin3/binary>> = SlotIndexBin2,
			CleanSlotIndex = array:new(SlotCount),
			SlotIndex = slot_folder_read(SlotIndexBin3, CleanSlotIndex, 0),
			{ok, SlotIndex};
		_ ->
			{error, crc_wonky}
	end. 

find_slot_index(Handle) ->
	{ok, SlotIndexPtr} = file:pread(Handle, ?FOOTERPOS_HEADERPOS, ?DWORD_SIZE),
	<<FooterPos:32/integer, SlotIndexLength:32/integer>> = SlotIndexPtr,
	{ok, SlotIndexBin} = file:pread(Handle, FooterPos, SlotIndexLength),
	SlotIndexBin.


read_blockindex(Handle, Position) ->
	{ok, _FilePos} = file:position(Handle, Position),
	{ok, <<BlockLength:32/integer>>} = file:read(Handle, 4),
	io:format("block length is ~w~n", [BlockLength]),
	{ok, BlockBin} = file:read(Handle, BlockLength),
	CheckLessBlockLength = BlockLength - 4,
	<<Block:CheckLessBlockLength/binary, CRC:32/integer>> = BlockBin,
	case erlang:crc32(Block) of 
		CRC ->
			<<BloomFilterL:32/integer, KeyHelperL:32/integer, 
			Tail/binary>> = Block,
			<<BloomFilter:BloomFilterL/binary, 
			KeyHelper:KeyHelperL/binary>> = Tail,
			{ok, BloomFilter, KeyHelper};
		_ ->
			{error, crc_wonky}
	end.


add_blocks(SortedKVList) ->
	add_blocks(SortedKVList, [], [], [], 0).

add_blocks([], KeyList, BlockIndex, BlockBinList, _) ->
	{KeyList, serialise_blockindex(BlockIndex), list_to_binary(BlockBinList)};
add_blocks(SortedKVList, KeyList, BlockIndex, BlockBinList, Position) ->
	case length(SortedKVList) of 
		KeyCount when KeyCount >= ?BLOCK_SIZE ->
			{TopBlock, Rest} = lists:split(?BLOCK_SIZE, SortedKVList);
		KeyCount ->
			{TopBlock, Rest} = lists:split(KeyCount, SortedKVList)
	end,
	[TopKey|_] = TopBlock,
	TopBin = serialise_block(TopBlock),
	add_blocks(Rest, add_to_keylist(KeyList, TopBlock), 
		[{TopKey, Position}|BlockIndex], 
		[TopBin|BlockBinList], Position + byte_size(TopBin)). 

add_to_keylist(KeyList, []) ->
	KeyList;
add_to_keylist(KeyList, [TopKV|Rest]) ->
	add_to_keylist([map_keyforbloom(TopKV)|KeyList], Rest).

map_keyforbloom(_Object=#object{key=Key}) ->
	Key.


serialise_blockindex(BlockIndex) ->
	serialise_blockindex(BlockIndex, <<>>).

serialise_blockindex([], BlockBin) ->
	BlockBin;
serialise_blockindex([TopIndex|Rest], BlockBin) ->
	{Key, BlockPos} = TopIndex,
	KeyBin = serialise_key(Key),
	KeyLength = byte_size(KeyBin),
	serialise_blockindex(Rest, 
		<<KeyLength:32/integer, KeyBin/binary, BlockPos:32/integer, 
		BlockBin/binary>>).

serialise_block(Block) ->
	term_to_binary(Block).

serialise_key(Key) ->
	term_to_binary(Key).

load_key(KeyBin) ->
	binary_to_term(KeyBin).


%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  

create_sample_kv(Prefix, Counter) ->
	Key = {o, "Bucket1", lists:concat([Prefix, Counter])},
	Object = #object{key=Key, value=null, 
	sequence_numb=random:uniform(1000000), state=active},
	Object.

create_ordered_kvlist(KeyList, 0) ->
	KeyList;
create_ordered_kvlist(KeyList, Length) ->
	KV = create_sample_kv("Key", Length),
	create_ordered_kvlist([KV|KeyList], Length - 1).


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
	{_, FileMD} = start_file("onstartfile.bst"),
	?assertMatch({0, 1}, FileMD#metadata.version),
	ok = file:delete("onstartfile.bst").

append_initialblock_test() ->
	{Handle, FileMD} = start_file("onstartfile.bst"),
	KVList = create_ordered_kvlist([], 2048),
	Key1 = {o, "Bucket1", "Key1"},
	[TopObj|_] = KVList,
	?assertMatch(Key1, TopObj#object.key),
	{_, UpdFileMD} = append_slot(Handle, KVList, 0, FileMD),
	{TopKey1, BloomBin, _, _} = array:get(0, UpdFileMD#metadata.slot_index),
	io:format("top key of ~w~n", [TopKey1]),
	?assertMatch(Key1, TopKey1),
	?assertMatch(true, leveled_rice:check_key(Key1, BloomBin)),
	?assertMatch(false, leveled_rice:check_key("OtherKey", BloomBin)),
	ok = file:delete("onstartfile.bst").

append_initialslotindex_test() ->
	{Handle, FileMD} = start_file("onstartfile.bst"),
	KVList = create_ordered_kvlist([], 2048),
	{_, UpdFileMD} = append_slot(Handle, KVList, 0, FileMD),
	append_slot_index(Handle, UpdFileMD),
	SlotIndexBin = find_slot_index(Handle),
	{ok, SlotIndex} = read_slot_index(SlotIndexBin),
	io:format("slot index is ~w ~n", [SlotIndex]),
	TopItem = array:get(0, SlotIndex),
	io:format("top item in slot index is ~w~n", [TopItem]),
	{ok, BloomFilter, KeyHelper} = read_blockindex(Handle, 32),
	?assertMatch(true, false),
	ok = file:delete("onstartfile.bst").



