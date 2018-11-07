-module(member_test).

-export([test_membership/0]).

-define(SEGMENTS_TO_CHECK, 32768). % a whole SST file
-define(MEMBERSHIP_LENGTHS, [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]).

segments(Length) ->
    AllSegs = lists:seq(1, ?SEGMENTS_TO_CHECK),
    AllSegsBin = 
        lists:foldl(fun(I, Acc) -> <<Acc/binary, (I - 1):16/integer>> end, 
                        <<>>,
                        AllSegs),
    StartPos = random:uniform(length(AllSegs) - Length),
    {<<AllSegsBin/binary, AllSegsBin/binary, 
            AllSegsBin/binary, AllSegsBin/binary>>,
        lists:sublist(AllSegs, StartPos, Length)}.

test_membership(Length) ->
    {AllSegsBin, TestList} = segments(Length),
    ExpectedOutput = 
        lists:reverse(TestList ++ TestList ++ TestList ++ TestList),

    SW0 = os:timestamp(),
    TestListFun = fun(I) -> lists:member(I, TestList) end,
    true = test_binary(AllSegsBin, [], TestListFun) == ExpectedOutput,
    ListT = timer:now_diff(os:timestamp(), SW0) / 131072,

    SW1 = os:timestamp(),
    TestSet = sets:from_list(TestList),
    TestSetsFun = fun(I) -> sets:is_element(I, TestSet) end,
    true = test_binary(AllSegsBin, [], TestSetsFun) == ExpectedOutput,
    SetsT = timer:now_diff(os:timestamp(), SW1) / 131072,

    io:format("Test with segment count ~w  ..."
                ++ " took ~w ms per 1000 checks with list ..." 
                ++ " took ~w ms per 1000 checks with set~n", [Length, ListT, SetsT]).


test_binary(<<>>, Acc, _TestFun) ->
    Acc;
test_binary(<<0:1/integer, TestSeg:15/integer, Rest/binary>>, Acc, TestFun) ->
    case TestFun(TestSeg) of
        true ->
            test_binary(Rest, [TestSeg|Acc], TestFun);
        false ->
            test_binary(Rest, Acc, TestFun)
    end.

test_membership() ->
    lists:foreach(fun(I) -> test_membership(I) end, ?MEMBERSHIP_LENGTHS).