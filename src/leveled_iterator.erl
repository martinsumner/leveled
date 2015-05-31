-module(leveled_internal).

-export([termiterator/6]).

-include_lib("eunit/include/eunit.hrl").


%% We will have a sorted list of terms
%% Some terms will be dummy terms which are pointers to more terms which can be 
%% found.  If a pointer is hit need to replenish the term list before 
%% proceeding.
%%
%% Helper Functions should have free functions - 
%% {FolderFun, CompareFun, PointerCheck}
%% FolderFun - function which takes the next item and the accumulator and 
%% returns an updated accumulator
%% CompareFun - function which should be able to compare two keys (which are 
%% not pointers), and return a winning item (or combination of items)
%% PointerCheck - function for differentiating between keys and pointer

termiterator(HeadItem, [], Acc, HelperFuns, 
	_StartKey, _EndKey) ->
	case HeadItem of 
		null ->
			Acc;
		_ ->
			{FolderFun, _, _} = HelperFuns,
			FolderFun(Acc, HeadItem)
	end;
termiterator(null, [NextItem|TailList], Acc, HelperFuns, 
	StartKey, EndKey) ->
	%% Check that the NextItem is not a pointer before promoting to HeadItem
	%% Cannot now promote a HeadItem which is a pointer
	{_, _, PointerCheck} = HelperFuns,
	case PointerCheck(NextItem) of 
		{true, Pointer} ->
			NewSlice = getnextslice(Pointer, EndKey),
			ExtendedList = lists:merge(NewSlice, TailList),
			termiterator(null, ExtendedList, Acc, HelperFuns, 
				StartKey, EndKey);
		false ->
			termiterator(NextItem, TailList, Acc, HelperFuns, 
				StartKey, EndKey)
	end;
termiterator(HeadItem, [NextItem|TailList], Acc, HelperFuns, 
	StartKey, EndKey) ->
	{FolderFun, CompareFun, PointerCheck} = HelperFuns,
	%% HeadItem cannot be pointer, but NextItem might be, so check before 
	%% comparison
	case PointerCheck(NextItem) of 
		{true, Pointer} ->
			NewSlice = getnextslice(Pointer, EndKey),
			ExtendedList = lists:merge(NewSlice, [NextItem|TailList]),
			termiterator(null, ExtendedList, Acc, HelperFuns, 
				StartKey, EndKey);
		false ->
			%% Compare to see if Head and Next match, or if Head is a winner 
			%% to be added to accumulator
			case CompareFun(HeadItem, NextItem) of 
				{match, StrongItem, _WeakItem} ->
					%% Discard WeakItem, Strong Item might be an aggregation of
					%% the items  
					termiterator(StrongItem, TailList, Acc, HelperFuns, 
						StartKey, EndKey);
				{winner, HeadItem} ->
					%% Add next item to accumulator, and proceed with next item
					AccPlus = FolderFun(Acc, HeadItem),
					termiterator(NextItem, TailList, AccPlus, HelperFuns, 
						HeadItem, EndKey)
			end
	end.
			


pointercheck_indexkey(IndexKey) ->
	case IndexKey of 
		{i, _Bucket, _Index, _Term, _Key, _Sequence, {zpointer, Pointer}} ->
			{true, Pointer};
		_ -> 
			false
	end.

folder_indexkey(Acc, IndexKey) ->
	io:format("Folding index key of - ~w~n", [IndexKey]),
	case IndexKey of 
		{i, _Bucket, _Index, _Term, _Key, _Sequence, tombstone} ->
			Acc;
		{i, _Bucket, _Index, _Term, Key, _Sequence, null} ->
			io:format("Adding key ~s~n", [Key]),
			lists:append(Acc, [Key])
	end.

compare_indexkey(IndexKey1, IndexKey2) ->
	{i, Bucket1, Index1, Term1, Key1, Sequence1, _Value1} = IndexKey1,
	{i, Bucket2, Index2, Term2, Key2, Sequence2, _Value2} = IndexKey2,
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


getnextslice(Pointer, _EndKey) ->
	case Pointer of 
		{test, NewList} ->
			NewList;
		_ ->
			[]
	end.


%% Unit tests


iterateoverindexkeyswithnopointer_test_() ->
	Key1 = {i, "pdsRecord", "familyName_bin", "1972SMITH", "10001", 1, null},
	Key2 = {i, "pdsRecord", "familyName_bin", "1972SMITH", "10001", 2, tombstone},
	Key3 = {i, "pdsRecord", "familyName_bin", "1971SMITH", "10002", 2, null},
	Key4 = {i, "pdsRecord", "familyName_bin", "1972JONES", "10003", 2, null},
	KeyList = lists:sort([Key1, Key2, Key3, Key4]),
	HelperFuns = {fun folder_indexkey/2, fun compare_indexkey/2, fun pointercheck_indexkey/1},
	ResultList = ["10002", "10003"],
	?_assertEqual(ResultList, termiterator(null, KeyList, [], HelperFuns, "1971", "1973")).




