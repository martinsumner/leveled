%% Used for creating fixed-size self-regulating encoded bloom filters
%%
%% Normally a bloom filter in order to achieve optimium size increases the
%% number of hashes as the desired false positive rate increases.  There is 
%% a processing overhead for checking this bloom, both because of the number
%% of hash calculations required, and also because of the need to CRC check
%% the bloom to ensure a false negative result is not returned due to 
%% corruption.
%%
%% A more space efficient bloom can be achieved through the compression of 
%% bloom filters with less hashes (and in an optimal case a single hash).  
%% This can be achieved using rice encoding.
%%
%% Rice-encoding and single hash blooms are used here in order to provide an
%% optimally space efficient solution, but also as the processing required to
%% support uncompression can be concurrently performing a checksum role.
%%
%% For this to work, the bloom is divided into 64 parts and a 32-bit hash is 
%% required.  Each hash is placed into one of 64 blooms based on the six least
%% significant bits of the hash, and the fmost significant 26-bits are used 
%% to indicate the bit to be added to the bloom.
%%
%% The bloom is then created by calculating the differences between the ordered
%% elements of the hash list and representing the difference using an exponent 
%% and a 13-bit remainder i.e.
%% 8000  ->   0  11111 01000000
%% 10000 ->  10  00000 00010000
%% 20000 -> 110  01110 00100000
%%
%% Each bloom should have approximately 64 differences.  
%%
%% Fronting the bloom is a bloom index, formed first by 16 pairs of 3-byte 
%% max hash, 2-byte length (bits) - with then each of the encoded bitstrings 
%% appended.  The max hash is the  total of all the differences (which should 
%% be the highest hash in the bloom).
%%
%% To check a key against the bloom, hash it, take the four least signifcant 
%% bits and read the start pointer, max hash end pointer from the expected 
%% positions in the bloom index.  Then roll through from the start pointer to 
%% the end pointer, accumulating each difference. There is a possible match if 
%% either the accumulator hits the expected hash or the max hash doesn't match 
%% the final accumulator (to cover if the bloom has been corrupted by a bit 
%% flip somwhere). A miss is more than twice as expensive (on average) than a
%% potential match - but still only requires around 64 integer additions
%% and the processing of <100 bytes of data.
%%
%% For 2048 keys, this takes up <4KB.  The false positive rate is 0.000122
%% This compares favourably for the equivalent size optimal bloom which 
%% would require 11 hashes and have a false positive rate of 0.000459.
%% Checking with a positive match should take on average about 6 microseconds, 
%% and a negative match should take around 11 microseconds.  
%%
%% See ../test/rice_test.erl for proving timings and fpr.



-module(leveled_rice).

-export([create_bloom/1, 
	check_key/2,
	check_keys/2]).

-include_lib("eunit/include/eunit.hrl").

-define(SLOT_COUNT, 64).
-define(MAX_HASH, 16777216).
-define(DIVISOR_BITS, 13).
-define(DIVISOR, 8092).

%% Create a bitstring representing the bloom filter from a key list

create_bloom(KeyList) ->
	create_bloom(KeyList, ?SLOT_COUNT, ?MAX_HASH).

create_bloom(KeyList, SlotCount, MaxHash) ->
	HashLists = array:new(SlotCount, [{default, []}]),
	OrdHashLists = create_hashlist(KeyList, HashLists, SlotCount, MaxHash),
	serialise_bloom(OrdHashLists).


%% Checking for a key

check_keys([], _) ->
	true;
check_keys([Key|Rest], BitStr) ->
	case check_key(Key, BitStr) of 
		false ->
			false;
		true ->
			check_keys(Rest, BitStr)
	end.

check_key(Key, BitStr) ->
	check_key(Key, BitStr, ?SLOT_COUNT, ?MAX_HASH, ?DIVISOR_BITS, ?DIVISOR).

check_key(Key, BitStr, SlotCount, MaxHash, Factor, Divisor) ->
	{Slot, Hash} = get_slothash(Key, MaxHash, SlotCount),
	{StartPos, Length, TopHash} = find_position(Slot, BitStr, 0, 40 * SlotCount),
	case BitStr of 
		<<_:StartPos/bitstring, Bloom:Length/bitstring, _/bitstring>> ->
			check_hash(Hash, Bloom, Factor, Divisor, 0, TopHash);
		_ ->
			io:format("Possible corruption of bloom index ~n"),
			true
	end.

find_position(Slot, BloomIndex, Counter, StartPosition) ->
	<<TopHash:24/integer, Length:16/integer, Rest/bitstring>> = BloomIndex,
	case Slot of 
		Counter -> 
			{StartPosition, Length, TopHash};
		_ ->
			find_position(Slot, Rest, Counter + 1, StartPosition + Length)
	end.


% Checking for a hash within a bloom

check_hash(_, <<>>, _, _, Acc, MaxHash) ->
	case Acc of 
		MaxHash -> 
			false;
		_ -> 
			io:format("Failure of CRC check on bloom filter~n"),
			true
	end;
check_hash(HashToCheck, BitStr, Factor, Divisor, Acc, TopHash) ->
	case findexponent(BitStr) of 
		{ok, Exponent, BitStrTail} ->
			case findremainder(BitStrTail, Factor) of 
				{ok, Remainder, BitStrTail2} ->
					NextHash = Acc + Divisor * Exponent + Remainder,
					case NextHash of 
						HashToCheck ->
							true;
						_ -> 
							check_hash(HashToCheck, BitStrTail2, Factor, 
								Divisor, NextHash, TopHash)
					end;
				error ->
					io:format("Failure of CRC check on bloom filter~n"),
					true 
			end;
		error ->
			io:format("Failure of CRC check on bloom filter~n"),
			true 
	end.

%% Convert the key list into an array of sorted hash lists

create_hashlist([], HashLists, _, _) ->
	HashLists;
create_hashlist([HeadKey|Rest], HashLists, SlotCount, MaxHash) ->
	{Slot, Hash} = get_slothash(HeadKey, MaxHash, SlotCount),
	HashList = array:get(Slot, HashLists),
	create_hashlist(Rest, 
		array:set(Slot, lists:usort([Hash|HashList]), HashLists), 
		SlotCount, MaxHash).

%% Convert an array of hash lists into an serialsed bloom

serialise_bloom(HashLists) ->
	SlotCount = array:size(HashLists),
	serialise_bloom(HashLists, SlotCount, 0,  []).

serialise_bloom(HashLists, SlotCount, Counter, Blooms) ->
	case Counter of 
		SlotCount -> 
			finalise_bloom(Blooms);
		_ ->
			Bloom = serialise_singlebloom(array:get(Counter, HashLists)),
			serialise_bloom(HashLists, SlotCount, Counter + 1, [Bloom|Blooms])
	end.

serialise_singlebloom(HashList) ->
	serialise_singlebloom(HashList, <<>>, 0, ?DIVISOR, ?DIVISOR_BITS).

serialise_singlebloom([], BloomStr, TopHash, _, _) ->
	% io:format("Single bloom created with bloom of ~w and top hash of ~w~n", [BloomStr, TopHash]),
	{BloomStr, TopHash};
serialise_singlebloom([Hash|Rest], BloomStr, TopHash, Divisor, Factor) ->
	HashGap = Hash - TopHash,
	Exponent = buildexponent(HashGap div Divisor),
	Remainder = HashGap rem Divisor,
	NewBloomStr = <<BloomStr/bitstring, Exponent/bitstring, Remainder:Factor/integer>>,
	serialise_singlebloom(Rest, NewBloomStr, Hash, Divisor, Factor).


finalise_bloom(Blooms) ->
	finalise_bloom(Blooms, {<<>>, <<>>}).

finalise_bloom([], BloomAcc) ->
	{BloomIndex, BloomStr} = BloomAcc,
	<<BloomIndex/bitstring, BloomStr/bitstring>>;
finalise_bloom([Bloom|Rest], BloomAcc) ->
	{BloomStr, TopHash} = Bloom,
	{BloomIndexAcc, BloomStrAcc} = BloomAcc,
	Length = bit_size(BloomStr),
	UpdIdx = <<TopHash:24/integer, Length:16/integer, BloomIndexAcc/bitstring>>,
	% io:format("Adding bloom string of ~w to bloom~n", [BloomStr]),
	UpdBloomStr = <<BloomStr/bitstring, BloomStrAcc/bitstring>>, 
	finalise_bloom(Rest, {UpdIdx, UpdBloomStr}).




buildexponent(Exponent) ->
	buildexponent(Exponent, <<0:1>>).

buildexponent(0, OutputBits) ->
	OutputBits;
buildexponent(Exponent, OutputBits) ->
	buildexponent(Exponent - 1, <<1:1, OutputBits/bitstring>>).


findexponent(BitStr) ->
	findexponent(BitStr, 0).

findexponent(<<>>, _) -> 
	error;
findexponent(<<H:1/integer, T/bitstring>>, Acc) ->
	case H of
		1 -> findexponent(T, Acc + 1);
		0 -> {ok, Acc, T}
	end.


findremainder(BitStr, Factor) ->
	case BitStr of 
		<<Remainder:Factor/integer, BitStrTail/bitstring>> ->
			{ok, Remainder, BitStrTail};
		_ ->
			error 
	end.


get_slothash(Key, MaxHash, SlotCount) ->
	Hash = erlang:phash2(Key, MaxHash),
	{Hash rem SlotCount, Hash div SlotCount}.


%%%%%%%%%%%%%%%%
% T E S T 
%%%%%%%%%%%%%%%  

corrupt_bloom(Bloom) ->
	Length = bit_size(Bloom),
	Random = random:uniform(Length),
	<<Part1:Random/bitstring, Bit:1/integer, Rest1/bitstring>> = Bloom,
	case Bit of 
		1 -> 
			<<Part1/bitstring, 0:1/integer, Rest1/bitstring>>;
		0 ->
			<<Part1/bitstring, 1:1/integer, Rest1/bitstring>>
	end.

bloom_test() ->
	KeyList = ["key1", "key2", "key3", "key4"],
	Bloom = create_bloom(KeyList),
	io:format("Bloom of ~w of length ~w ~n", [Bloom, bit_size(Bloom)]),
	?assertMatch(true, check_key("key1", Bloom)),
	?assertMatch(true, check_key("key2", Bloom)),
	?assertMatch(true, check_key("key3", Bloom)),
	?assertMatch(true, check_key("key4", Bloom)),
	?assertMatch(false, check_key("key5", Bloom)).

bloom_corruption_test() ->
	KeyList = ["key1", "key2", "key3", "key4"],
	Bloom = create_bloom(KeyList),
	Bloom1 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom1)),
	Bloom2 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom2)),
	Bloom3 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom3)),
	Bloom4 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom4)),
	Bloom5 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom5)),
	Bloom6 = corrupt_bloom(Bloom),
	?assertMatch(true, check_keys(KeyList, Bloom6)).


