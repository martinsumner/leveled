-module(leveled_iterator).

-export([termiterator/3]).

-include_lib("eunit/include/eunit.hrl").


%% Takes a list of terms to iterate - the terms being sorted in Erlang term
%% order
%%
%% Helper Functions should have free functions - 
%% {FolderFun, CompareFun, PointerCheck, PointerFetch}
%% FolderFun - function which takes the next item and the accumulator and 
%% returns an updated accumulator.  Note FolderFun can only increase the 
%% accumulator by one entry each time
%% CompareFun - function which should be able to compare two keys (which are 
%% not pointers), and return a winning item (or combination of items)
%% PointerCheck - function for differentiating between keys and pointer
%% PointerFetch - function that takes a pointer an EndKey (which may be 
%% infinite) and returns a ne wslice of ordered results from that pointer
%% 
%% Range can be for the form
%% {StartKey, EndKey, MaxKeys} where EndKey or MaxKeys can be infinite (but 
%% not both)


termiterator(ListToIterate, HelperFuns, Range) ->
	case Range of 
		{_, infinte, infinite} ->
			bad_iterator;
		_ ->
			termiterator(null, ListToIterate, [], HelperFuns, Range)
	end.


termiterator(HeadItem, [], Acc, HelperFuns, _) ->
	case HeadItem of 
		null ->
			Acc;
		_ ->
			{FolderFun, _, _, _} = HelperFuns,
			FolderFun(Acc, HeadItem)
	end;
termiterator(null, [NextItem|TailList], Acc, HelperFuns, Range) ->
	%% Check that the NextItem is not a pointer before promoting to HeadItem
	%% Cannot now promote a HeadItem which is a pointer
	{_, _, PointerCheck, PointerFetch} = HelperFuns,
	case PointerCheck(NextItem) of 
		{true, Pointer} ->
			{_, EndKey, _} = Range,
			NewSlice = PointerFetch(Pointer, EndKey),
			ExtendedList = lists:merge(NewSlice, TailList),
			termiterator(null, ExtendedList, Acc, HelperFuns, Range);
		false ->
			termiterator(NextItem, TailList, Acc, HelperFuns, Range)
	end;
termiterator(HeadItem, [NextItem|TailList], Acc, HelperFuns, Range) ->
	{FolderFun, CompareFun, PointerCheck, PointerFetch} = HelperFuns,
	{_, EndKey, MaxItems} = Range,
	%% HeadItem cannot be pointer, but NextItem might be, so check before 
	%% comparison
	case PointerCheck(NextItem) of 
		{true, Pointer} ->
			NewSlice = PointerFetch(Pointer, EndKey),
			ExtendedList = lists:merge(NewSlice, [HeadItem|TailList]),
			termiterator(null, ExtendedList, Acc, HelperFuns, Range);
		false ->
			%% Compare to see if Head and Next match, or if Head is a winner 
			%% to be added to accumulator
			case CompareFun(HeadItem, NextItem) of 
				{match, StrongItem, _WeakItem} ->
					%% Discard WeakItem, Strong Item might be an aggregation of
					%% the items  
					termiterator(StrongItem, TailList, Acc, HelperFuns, Range);
				{winner, HeadItem} ->
					%% Add next item to accumulator, and proceed with next item
					AccPlus = FolderFun(Acc, HeadItem),
					case length(AccPlus) of 
						MaxItems ->
							AccPlus;
						_ ->
							termiterator(NextItem, TailList, AccPlus, 
								HelperFuns, 
								{HeadItem, EndKey, MaxItems})
					end
			end
	end.
			

%% Initial forms of keys supported are Index Keys and Object Keys
%% 
%% All keys are of the form {Key, Value, SequenceNumber, State}
%%
%% The Key will be of the form:
%% {o, Bucket, Key} - for an Object Key
%% {i, Bucket, IndexName, IndexTerm, Key} - for an Index Key
%%
%% The value will be of the form: 
%% {o, ObjectHash, [vector-clocks]} - for an Object Key
%% null - for an Index Key
%%
%% Sequence number is the sequence number the key was added, and the highest
%% sequence number in the list of keys for an index key.
%%
%% State can be one of the following: 
%% live - an active key
%% tomb - a tombstone key
%% {timestamp, TS} - an active key to a certain timestamp
%% {pointer, Pointer} - to be added by iterators to indicate further data 
%% available in the range from a particular source


pointercheck_indexkey(IndexKey) ->
	case IndexKey of 
		{_Key, _Values, _Sequence, {pointer, Pointer}} ->
			{true, Pointer};
		_ -> 
			false
	end.

folder_indexkey(Acc, IndexKey) ->
	case IndexKey of 
		{_Key, _Value, _Sequence, tomb} ->
			Acc;
		{Key, _Value, _Sequence, live} ->
			{i, _, _, _, ObjectKey} = Key,
			lists:append(Acc, [ObjectKey])
	end.

compare_indexkey(IndexKey1, IndexKey2) ->
	{{i, Bucket1, Index1, Term1, Key1}, _Val1, Sequence1, _St1} = IndexKey1,
	{{i, Bucket2, Index2, Term2, Key2}, _Val2, Sequence2, _St2} = IndexKey2,
	case {Bucket1, Index1, Term1, Key1} of 
		{Bucket2, Index2, Term2, Key2} when Sequence1 >= Sequence2 ->
			{match, IndexKey1, IndexKey2};
		{Bucket2, Index2, Term2, Key2} ->
			{match, IndexKey2, IndexKey1};
		_ when IndexKey2 >= IndexKey1 ->
			{winner, IndexKey1};
		_ ->
			{winner, IndexKey2}
	end.



%% Unit testsß

getnextslice(Pointer, _EndKey) ->
	case Pointer of 
		{test, NewList} ->
			NewList;
		_ ->
			[]
	end.


iterateoverindexkeyswithnopointer_test() ->
	Key1 = {{i, "pdsRecord", "familyName_bin", "1972SMITH", "10001"}, 
	null, 1, live},
	Key2 = {{i, "pdsRecord", "familyName_bin", "1972SMITH", "10001"}, 
	null, 2, tomb},
	Key3 = {{i, "pdsRecord", "familyName_bin", "1971SMITH", "10002"}, 
	null, 2, live},
	Key4 = {{i, "pdsRecord", "familyName_bin", "1972JONES", "10003"}, 
	null, 2, live},
	KeyList = lists:sort([Key1, Key2, Key3, Key4]),
	HelperFuns = {fun folder_indexkey/2, fun compare_indexkey/2, 
		fun pointercheck_indexkey/1, fun getnextslice/2},
	?assertMatch(["10002", "10003"], 
		termiterator(KeyList, HelperFuns, {"1971", "1973", infinite})).

iterateoverindexkeyswithpointer_test() ->
	Key1 = {{i, "pdsRecord", "familyName_bin", "1972SMITH", "10001"}, 
	null, 1, live},
	Key2 = {{i, "pdsRecord", "familyName_bin", "1972SMITH", "10001"}, 
	null, 2, tomb},
	Key3 = {{i, "pdsRecord", "familyName_bin", "1971SMITH", "10002"}, 
	null, 2, live},
	Key4 = {{i, "pdsRecord", "familyName_bin", "1972JONES", "10003"}, 
	null, 2, live},
	Key5 = {{i, "pdsRecord", "familyName_bin", "1972ZAFRIDI", "10004"}, 
	null, 2, live},
	Key6 = {{i, "pdsRecord", "familyName_bin", "1972JONES", "10004"}, 
	null, 0, {pointer, {test, [Key5]}}},
	KeyList = lists:sort([Key1, Key2, Key3, Key4, Key6]),
	HelperFuns = {fun folder_indexkey/2, fun compare_indexkey/2, 
		fun pointercheck_indexkey/1, fun getnextslice/2},
	?assertMatch(["10002", "10003", "10004"], 
		termiterator(KeyList, HelperFuns, {"1971", "1973", infinite})),
	?assertMatch(["10002", "10003"], 
		termiterator(KeyList, HelperFuns, {"1971", "1973", 2})).






