%% -------- Eval Functions ---------
%%
%% Support for different eval expressions within leveled 
%%

-module(leveled_eval).

-export([apply_eval/4]).

%%%============================================================================
%%% External API
%%%============================================================================

apply_eval({eval, Eval}, Term, Key, AttrMap) ->
    apply_eval(Eval, Term, Key, AttrMap);
apply_eval({'PIPE', Eval1, 'INTO', Eval2}, Term, Key, AttrMap) ->
    apply_eval(Eval2, Term, Key, apply_eval(Eval1, Term, Key, AttrMap));
apply_eval({
        delim, {identifier, _, InKey}, {string, _, Delim}, ExpKeys},
        Term, Key, AttrMap) ->
    TermToSplit = term_to_process(InKey, Term, Key, AttrMap),
    CptTerms = string:split(TermToSplit, Delim, all),
    L = min(length(CptTerms), length(ExpKeys)),
    maps:merge(
        AttrMap,
        maps:from_list(
            lists:zip(lists:sublist(ExpKeys, L), lists:sublist(CptTerms, L)))
    );
apply_eval(
        {join, InKeys, {string, _, Delim}, {identifier, _, OutKey}},
        _Term, _Key, AttrMap) ->
    NewTerm =
        unicode:characters_to_binary(
            lists:join(
                Delim,
                lists:map(
                    fun(InKey) -> maps:get(InKey, AttrMap, <<"">>) end,
                    InKeys)
            )
        ),
    maps:put(OutKey, NewTerm, AttrMap);
apply_eval({
        split, {identifier, _, InKey}, {string, _, Delim}, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    TermToSplit = term_to_process(InKey, Term, Key, AttrMap),
    TermList = string:split(TermToSplit, Delim, all),
    maps:put(OutKey, TermList, AttrMap);
apply_eval(
        {slice, {identifier, _, InKey}, {integer, _, Width}, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    TermToSlice = term_to_process(InKey, Term, Key, AttrMap),
    TermCount = string:length(TermToSlice) div Width,
    TermList =
        lists:map(
            fun(S) -> string:slice(TermToSlice, S, Width) end,
            lists:map(fun(I) -> Width * I end, lists:seq(0, TermCount - 1))),
    maps:put(OutKey, TermList, AttrMap);
apply_eval(
        {index, {identifier, _, InKey},
            {integer, _, Start}, {integer, _, Length},
            {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    TermToIndex = term_to_process(InKey, Term, Key, AttrMap),
    case string:length(TermToIndex) of
        L when L >= (Start + Length) ->
            maps:put(
                OutKey, string:slice(TermToIndex, Start, Length), AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {to_integer, {identifier, _, InKey}, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    TermToConvert = term_to_process(InKey, Term, Key, AttrMap),
    case string:to_integer(TermToConvert) of
        {I, _Rest} when is_integer(I) ->
            maps:put(OutKey, I, AttrMap);
        _ ->
            AttrMap
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

term_to_process(<<"term">>, Term, _Key, _AttrMap) ->
    Term;
term_to_process(<<"key">>, _Term, Key, _AttrMap) ->
    Key;
term_to_process(AttrKey, _Term, _Key, AttrMap) ->
    maps:get(AttrKey, AttrMap, <<"">>).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    EvalString1 = "delim($term, \"|\", ($fn, $dob, $dod, $gns, $pcs))",
    EvalString2 = "delim($gns, \"#\", ($gn1, $gn2, $gn3))",
    
    EvalString3 = EvalString1 ++ " | " ++ EvalString2,
    {ok, Tokens3, _EndLine3} = leveled_evallexer:string(EvalString3),
    {ok, ParsedExp3} = leveled_evalparser:parse(Tokens3),
    EvalOut3 =
        apply_eval(
            ParsedExp3,
            <<"SMITH|19861216||Willow#Mia|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut3)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut3)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut3)),
    ?assertMatch(<<"Willow#Mia">>, maps:get(<<"gns">>, EvalOut3)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut3)),
    ?assertMatch(<<"Willow">>, maps:get(<<"gn1">>, EvalOut3)),
    ?assertMatch(<<"Mia">>, maps:get(<<"gn2">>, EvalOut3)),
    ?assertNot(maps:is_key(<<"gn3">>, EvalOut3)),


    EvalString4 = EvalString3 ++ " | join(($dob, $fn), \"|\", $dobfn)",
    {ok, Tokens4, _EndLine4} = leveled_evallexer:string(EvalString4),
    {ok, ParsedExp4} = leveled_evalparser:parse(Tokens4),
    EvalOut4 =
        apply_eval(
            ParsedExp4,
            <<"SMITH|19861216||Willow#Mia|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut4)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut4)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut4)),
    ?assertMatch(<<"Willow#Mia">>, maps:get(<<"gns">>, EvalOut4)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut4)),
    ?assertMatch(<<"Willow">>, maps:get(<<"gn1">>, EvalOut4)),
    ?assertMatch(<<"Mia">>, maps:get(<<"gn2">>, EvalOut4)),
    ?assertNot(maps:is_key(<<"gn3">>, EvalOut4)),
    ?assertMatch(<<"19861216|SMITH">>, maps:get(<<"dobfn">>, EvalOut4)),


    EvalString5 = EvalString4 ++ " | index($dob, 0, 4, $yob) | to_integer($yob, $yob)",
    {ok, Tokens5, _EndLine5} = leveled_evallexer:string(EvalString5),
    {ok, ParsedExp5} = leveled_evalparser:parse(Tokens5),
    EvalOut5 =
        apply_eval(
            ParsedExp5,
            <<"SMITH|19861216||Willow#Mia|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut5)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut5)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut5)),
    ?assertMatch(<<"Willow#Mia">>, maps:get(<<"gns">>, EvalOut5)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut5)),
    ?assertMatch(<<"Willow">>, maps:get(<<"gn1">>, EvalOut5)),
    ?assertMatch(<<"Mia">>, maps:get(<<"gn2">>, EvalOut5)),
    ?assertNot(maps:is_key(<<"gn3">>, EvalOut5)),
    ?assertMatch(<<"19861216|SMITH">>, maps:get(<<"dobfn">>, EvalOut5)),
    ?assertMatch(1986, maps:get(<<"yob">>, EvalOut5)),

    EvalString6 = EvalString1 ++ " | slice($gns, 2, $gns)",
    {ok, Tokens6, _EndLine6} = leveled_evallexer:string(EvalString6),
    {ok, ParsedExp6} = leveled_evalparser:parse(Tokens6),
    EvalOut6 =
        apply_eval(
            ParsedExp6,
            <<"SMITH|19861216||MAN1Ve|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut6)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut6)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut6)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut6)),
    ?assertMatch([<<"MA">>, <<"N1">>, <<"Ve">>], maps:get(<<"gns">>, EvalOut6)),

    EvalOut7 =
        apply_eval(
            ParsedExp6,
            <<"SMITH|19861216||MAN1VeZ|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut7)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut7)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut7)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut7)),
    ?assertMatch([<<"MA">>, <<"N1">>, <<"Ve">>], maps:get(<<"gns">>, EvalOut7)),

    EvalString8 = EvalString1 ++ " | split($gns, \"#\", $gns)",
    {ok, Tokens8, _EndLine8} = leveled_evallexer:string(EvalString8),
    {ok, ParsedExp8} = leveled_evalparser:parse(Tokens8),
    EvalOut8 =
        apply_eval(
            ParsedExp8,
            <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut8)),
    ?assertMatch(<<"19861216">>, maps:get(<<"dob">>, EvalOut8)),
    ?assertMatch(<<"">>, maps:get(<<"dod">>, EvalOut8)),
    ?assertMatch(<<"LS1 4BT#LS8 1ZZ">>, maps:get(<<"pcs">>, EvalOut8)),
    ?assertMatch([<<"Willow">>, <<"Mia">>, <<"Vera">>], maps:get(<<"gns">>, EvalOut8)),

    EvalString9 =
        "delim($term, \"|\", ($name, $height, $weight, $pick)) |"
        " to_integer($height, $height) |"
        " to_integer($weight, $weight) |"
        " to_integer($pick, $pick) |"
        " delim($key, \"|\", ($team, $number)) |"
        " index($team, 0, 9, $doh)",
    {ok, Tokens9, _EndLine9} = leveled_evallexer:string(EvalString9),
    {ok, ParsedExp9} = leveled_evalparser:parse(Tokens9),
    EvalOut9 =
        apply_eval(
            ParsedExp9,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertMatch(<<"WEMBANYAMA">>, maps:get(<<"name">>, EvalOut9)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOut9)),
    ?assertMatch(95, maps:get(<<"weight">>, EvalOut9)),
    ?assertMatch(<<"#1">>, maps:get(<<"pick">>, EvalOut9)),
        % Not changes as not starting with integer
    ?assertMatch(<<"SPURS">>, maps:get(<<"team">>, EvalOut9)),
    ?assertMatch(<<"00001">>, maps:get(<<"number">>, EvalOut9)),
    ?assertNot(maps:is_key(<<"doh">>, EvalOut9))
    .



-endif.