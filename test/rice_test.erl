%% Test performance and accuracy of rice-encoded bloom filters
%%
%% Calling check_negative(2048, 1000000) should return about 122 false 
%% positives in around 11 seconds, with a size below 4KB
%%
%% The equivalent positive check is check_positive(2048, 488) and this
%% should take around 6 seconds.
%%
%% So a blooom with 2048 members should support o(100K) checks per second
%% on a modern CPU, whilst requiring 2 bytes per member.

-module(rice_test).

-export([check_positive/2, check_negative/2, calc_hash/2]).



check_positive(KeyCount, LoopCount) ->
	KeyList = produce_keylist(KeyCount),
	Bloom = leveled_rice:create_bloom(KeyList),
	check_positive(KeyList, Bloom, LoopCount).

check_positive(_, Bloom, 0) ->
	{ok, byte_size(Bloom)};
check_positive(KeyList, Bloom, LoopCount) ->
	true = leveled_rice:check_keys(KeyList, Bloom),
	check_positive(KeyList, Bloom, LoopCount - 1).


produce_keylist(KeyCount) ->
	KeyPrefix = lists:concat(["PositiveKey-", leveled_rand:uniform(KeyCount)]),
	produce_keylist(KeyCount, [], KeyPrefix).

produce_keylist(0, KeyList, _) ->
	KeyList;
produce_keylist(KeyCount, KeyList, KeyPrefix) ->
	Key = lists:concat([KeyPrefix, KeyCount]),
	produce_keylist(KeyCount - 1, [Key|KeyList], KeyPrefix).


check_negative(KeyCount, CheckCount) ->
	KeyList = produce_keylist(KeyCount),
	Bloom = leveled_rice:create_bloom(KeyList),
	check_negative(Bloom, CheckCount, 0).

check_negative(Bloom, 0, FalsePos) ->
	{byte_size(Bloom), FalsePos};
check_negative(Bloom, CheckCount, FalsePos) ->
	Key = lists:concat(["NegativeKey-", CheckCount, leveled_rand:uniform(CheckCount)]),
	case leveled_rice:check_key(Key, Bloom) of 
		true -> check_negative(Bloom, CheckCount - 1, FalsePos + 1);
		false -> check_negative(Bloom, CheckCount - 1, FalsePos)
	end.

calc_hash(_, 0) ->
	ok;
calc_hash(Key, Count) ->
	erlang:phash2(lists:concat([Key, Count, "sometxt"])),
	calc_hash(Key, Count -1).
