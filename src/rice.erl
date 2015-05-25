-module(rice).
-export([encode/1, 
	encode/2, 
	checkforhash/2, 
	converttohash/1]).
-include_lib("eunit/include/eunit.hrl").

%% Factor is the power of 2 representing the expected normal gap size between
%% members of the hash, and therefore the size of the bitstring to represent the
%% remainder for the gap
%%
%% The encoded output should contain a single byte which is the Factor, followed
%% by a series of exponents and remainders.
%%
%% The exponent is n 1's followed by a 0, where n * (2 ^ Factor) + remainder 
%% represents the gap to the next hash 
%%
%% The size passed in should be the maximum possible value of the hash.
%% If this isn't provided - assumes 2^32 - the default for phash2 

encode(HashList) ->
	encode(HashList, 4 * 1024 * 1024 * 1024).

encode(HashList, Size) ->
	SortedHashList = lists:usort(HashList),
	ExpectedGapSize = Size div length(SortedHashList),
	Factor = findpowerundergap(ExpectedGapSize),
	riceencode(SortedHashList, Factor).

%% Outcome may be suboptimal if lists have not been de-duplicated
%% Will fail on an unsorted list

riceencode(HashList, Factor) when Factor<256 ->
	Divisor = powtwo(Factor),
	riceencode(HashList, Factor, Divisor, <<>>, 0).

riceencode([], Factor, _, BitStrAcc, _) ->
	Prefix  = binary:encode_unsigned(Factor),
	<<Prefix/bytes, BitStrAcc/bitstring>>;
riceencode([HeadHash|TailList], Factor, Divisor, BitStrAcc, LastHash) ->
	HashGap = HeadHash - LastHash,
	case HashGap of 
		0 -> 
			riceencode(TailList, Factor, Divisor, BitStrAcc, HeadHash);
		N when N > 0 ->
			Exponent = buildexponent(HashGap div Divisor),
			Remainder = HashGap rem Divisor,
			ExpandedBitStrAcc = <<BitStrAcc/bitstring, Exponent/bitstring, Remainder:Factor>>,
			riceencode(TailList, Factor, Divisor, ExpandedBitStrAcc, HeadHash)
		end.


%% Checking for a hash needs to roll through the compressed bloom, decoding until
%% the member is found (match!), passed (not matched) or the end of the encoded
%% bitstring has been reached (not matched)

checkforhash(HashToCheck, BitStr) ->
	<<Factor:8/integer, RiceEncodedBitStr/bitstring>> = BitStr,
	Divisor = powtwo(Factor),
	checkforhash(HashToCheck, RiceEncodedBitStr, Factor, Divisor, 0).

checkforhash(_, <<>>, _, _, _) ->
	false;
checkforhash(HashToCheck, BitStr, Factor, Divisor, Acc) ->
	[Exponent, BitStrTail] = findexponent(BitStr),
	[Remainder, BitStrTail2] = findremainder(BitStrTail, Factor),
	NextHash = Acc + Divisor * Exponent + Remainder,
	case NextHash of 
		HashToCheck -> true;
		N when N>HashToCheck -> false;
		_ -> checkforhash(HashToCheck, BitStrTail2, Factor, Divisor, NextHash)
	end.


%% Exported functions - currently used only in testing

converttohash(ItemList) -> 
	converttohash(ItemList, []).

converttohash([], HashList) ->
	HashList;
converttohash([H|T], HashList) ->
	converttohash(T, [erlang:phash2(H)|HashList]).



%% Helper functions

buildexponent(Exponent) ->
	buildexponent(Exponent, <<0:1>>).

buildexponent(0, OutputBits) ->
	OutputBits;
buildexponent(Exponent, OutputBits) ->
	buildexponent(Exponent - 1, <<1:1, OutputBits/bitstring>>).


findexponent(BitStr) ->
	findexponent(BitStr, 0).

findexponent(BitStr, Acc) ->
	<<H:1/bitstring, T/bitstring>> = BitStr,
	case H of
		<<1:1>> -> findexponent(T, Acc + 1);
		<<0:1>> -> [Acc, T]
	end.


findremainder(BitStr, Factor) ->
	<<Remainder:Factor/integer, BitStrTail/bitstring>> = BitStr,
	[Remainder, BitStrTail].


powtwo(N) -> powtwo(N, 1).

powtwo(0, Acc) ->
	Acc;
powtwo(N, Acc) ->
	powtwo(N-1, Acc * 2).

%% Helper method for finding the factor of two which provides the most 
%% efficient compression given an average gap size

findpowerundergap(GapSize) -> findpowerundergap(GapSize, 1, 0).

findpowerundergap(GapSize, Acc, Counter) ->
	case Acc of
		N when N > GapSize -> Counter - 1;
		_ -> findpowerundergap(GapSize, Acc * 2, Counter + 1)
	end.


%% Unit tests

findpowerundergap_test_() -> 
	[
	?_assertEqual(9, findpowerundergap(700)), 
	?_assertEqual(9, findpowerundergap(512)), 
	?_assertEqual(8, findpowerundergap(511))].

encode_test_() ->
	[
	?_assertEqual(<<9, 6, 44, 4:5>>, encode([24,924], 1024)),
	?_assertEqual(<<9, 6, 44, 4:5>>, encode([24,24,924], 1024)),
	?_assertEqual(<<9, 6, 44, 4:5>>, encode([24,924,924], 1024))
	].

check_test_() ->
	[
	?_assertEqual(true, checkforhash(924, <<9, 6, 44, 4:5>>)),
	?_assertEqual(true, checkforhash(24, <<9, 6, 44, 4:5>>)),
	?_assertEqual(false, checkforhash(23, <<9, 6, 44, 4:5>>)),
	?_assertEqual(false, checkforhash(923, <<9, 6, 44, 4:5>>)),
	?_assertEqual(false, checkforhash(925, <<9, 6, 44, 4:5>>))
	].
