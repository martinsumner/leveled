%% -------- Eval Functions ---------
%%
%% Support for different eval expressions within leveled 
%%

-module(leveled_eval).

-export([generate_eval_function/2]).

%%%============================================================================
%%% External API
%%%============================================================================

-spec generate_eval_function(
        string(), map()) -> fun((binary(), binary()) -> map()).
generate_eval_function(EvalString, Substitutions) ->
    {ok, ParsedEval} = generate_eval_expression(EvalString, Substitutions),
    fun(Term, Key) ->
        apply_eval(ParsedEval, Term, Key, maps:new())
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================


generate_eval_expression(EvalString, Substitutions) ->
    CodePointList = unicode:characters_to_list(EvalString),
    {ok, Tokens, _EndLine} = leveled_evallexer:string(CodePointList),
    case leveled_filter:substitute_items(Tokens, Substitutions, []) of
        {error, Error} ->
            {error, Error};
        UpdTokens ->
            leveled_evalparser:parse(UpdTokens)
    end.


apply_eval({eval, Eval}, Term, Key, AttrMap) ->
    apply_eval(Eval, Term, Key, AttrMap);
apply_eval({'PIPE', Eval1, 'INTO', Eval2}, Term, Key, AttrMap) ->
    apply_eval(Eval2, Term, Key, apply_eval(Eval1, Term, Key, AttrMap));
apply_eval({
        delim, {identifier, _, InKey}, {string, _, Delim}, ExpKeys},
        Term, Key, AttrMap) ->
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToSplit when is_binary(TermToSplit) ->
            CptTerms = string:split(TermToSplit, Delim, all),
            L = min(length(CptTerms), length(ExpKeys)),
            maps:merge(
                AttrMap,
                maps:from_list(
                    lists:zip(
                        lists:sublist(ExpKeys, L),
                        lists:sublist(CptTerms, L)
                    )
                )
            );
        _ ->
            AttrMap
    end;
apply_eval(
        {join, InKeys, {string, _, Delim}, {identifier, _, OutKey}},
        _Term, _Key, AttrMap) ->
    NewTerm =
        unicode:characters_to_binary(
            lists:join(
                Delim,
                lists:filter(
                    fun(V) -> is_binary(V) end,
                    lists:map(
                        fun(InKey) -> maps:get(InKey, AttrMap, <<"">>) end,
                        InKeys
                    )
                )
            )
        ),
    maps:put(OutKey, NewTerm, AttrMap);
apply_eval({
        split, {identifier, _, InKey}, DelimAttr, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToSplit when is_binary(TermToSplit) ->
            TermList = string:split(TermToSplit, element(3, DelimAttr), all),
            maps:put(OutKey, TermList, AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {slice, {identifier, _, InKey}, WidthAttr, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    Width = element(3, WidthAttr),
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToSlice when is_binary(TermToSlice) ->
            TermCount = string:length(TermToSlice) div Width,
            TermList =
                lists:map(
                    fun(S) -> string:slice(TermToSlice, S, Width) end,
                    lists:map(
                        fun(I) -> Width * I end,
                        lists:seq(0, TermCount - 1))),
            maps:put(OutKey, TermList, AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {index,
            {identifier, _, InKey},
            StartAtr, LengthAttr,
            {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    Start = element(3, StartAtr),
    Length = element(3, LengthAttr),
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToIndex when is_binary(TermToIndex) ->
            case string:length(TermToIndex) of
                L when L >= (Start + Length) ->
                    maps:put(
                        OutKey,
                        string:slice(TermToIndex, Start, Length),
                        AttrMap
                    );
                _ ->
                    AttrMap
            end;
        _ ->
            AttrMap
    end;
apply_eval(
        {kvsplit,
            {identifier, _, InKey},
            {string, _, DelimPair}, {string, _, DelimKV}},
        Term, Key, AttrMap) ->
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToSplit when is_binary(TermToSplit) ->
            lists:foldl(
                fun(S, AccMap) ->
                    case string:split(S, DelimKV, all) of
                        [K, V] ->
                            maps:put(K, V, AccMap);
                        _ ->
                            AccMap
                    end
                end,
                AttrMap,
                string:split(TermToSplit, DelimPair, all)
            );
        _ ->
            AttrMap
    end;
apply_eval(
        {to_integer, {identifier, _, InKey}, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToConvert when is_binary(TermToConvert) ->
            case string:to_integer(TermToConvert) of
                {I, _Rest} when is_integer(I) ->
                    maps:put(OutKey, I, AttrMap);
                _ ->
                    AttrMap
            end;
        AlreadyInteger when is_integer(AlreadyInteger) ->
            maps:put(OutKey, AlreadyInteger, AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {to_string, {identifier, _, InKey}, {identifier, _, OutKey}},
        Term, Key, AttrMap) ->
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToConvert when is_integer(TermToConvert) ->
            maps:put(
                OutKey,
                list_to_binary(integer_to_list(TermToConvert)),
                AttrMap
            );
        AlreadyString when is_binary(AlreadyString) ->
            maps:put(OutKey, AlreadyString, AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {map, InID, Comparator, MapList, Default, OutID},
        Term, Key, AttrMap) ->
    {identifier, _, InKey} = InID,
    {identifier, _, OutKey} = OutID,
    TermToCompare = term_to_process(InKey, Term, Key, AttrMap),
    F = reverse_compare_mapping(element(2, Comparator), TermToCompare),
    case lists:dropwhile(F, MapList) of
        [] ->
            maps:put(OutKey, element(3, Default), AttrMap);
        [{mapping, _T, Assignment}|_Rest] ->
            maps:put(OutKey, element(3, Assignment), AttrMap)
    end;
apply_eval(
        {MathOp, OperandX, OperandY, {identifier, _, OutKey}},
        _Term, _Key, AttrMap)
        when MathOp == add; MathOp == subtract ->
    X = maybe_fetch_operand(OperandX, AttrMap),
    Y = maybe_fetch_operand(OperandY, AttrMap),
    case MathOp of
        add when is_integer(X), is_integer(Y) ->
            maps:put(OutKey, X + Y, AttrMap);
        subtract when is_integer(X), is_integer(Y) ->
            maps:put(OutKey, X - Y, AttrMap);
        _ ->
            AttrMap
    end;
apply_eval(
        {regex, {identifier, _, InKey}, CompiledRE, ExpKeys},
        Term, Key, AttrMap) ->
    ExpectedKeyLength = length(ExpKeys),
    Opts = [{capture, all_but_first, binary}],
    case term_to_process(InKey, Term, Key, AttrMap) of
        TermToCapture when is_binary(TermToCapture)->
            case leveled_util:regex_run(TermToCapture, CompiledRE, Opts) of
                {match, CptTerms} ->
                    L = min(length(CptTerms), ExpectedKeyLength),
                    CptMap =
                        maps:from_list(
                            lists:zip(
                                lists:sublist(ExpKeys, L),
                                lists:sublist(CptTerms, L))),
                    maps:merge(AttrMap, CptMap);
                _ ->
                    AttrMap
            end;
        _ ->
            AttrMap
    end.

maybe_fetch_operand({identifier, _, ID}, AttrMap) ->
    maps:get(ID, AttrMap, 0);
maybe_fetch_operand(Op, _AttrMap) ->
    element(3, Op).

term_to_process(<<"term">>, Term, _Key, _AttrMap) ->
    Term;
term_to_process(<<"key">>, _Term, Key, _AttrMap) ->
    Key;
term_to_process(AttrKey, _Term, _Key, AttrMap) ->
    maps:get(AttrKey, AttrMap, not_found).

reverse_compare_mapping('<', Term) ->
    fun({mapping, T, _A}) -> Term >= element(3, T) end;
reverse_compare_mapping('<=', Term) ->
    fun({mapping, T, _A}) -> Term > element(3, T) end;
reverse_compare_mapping('>', Term) ->
    fun({mapping, T, _A}) -> Term =< element(3, T) end;
reverse_compare_mapping('>=', Term) ->
    fun({mapping, T, _A}) -> Term < element(3, T) end;
reverse_compare_mapping('=', Term) ->
    fun({mapping, T, _A}) -> Term =/= element(3, T) end.

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
    ?assertNot(maps:is_key(<<"doh">>, EvalOut9)),

    %% Age at 30 April 2024
    EvalString10 =
        EvalString5 ++ 
        " | index($dob, 4, 4, $birthday)"
        " | map($birthday, <=, ((\"0430\", 2024)), 2023, $yoc)"
        " | subtract($yoc, $yob, $age)"
        " | add($age, 1, $age_next)"
        " | to_string($age, $age)"
        ,
    {ok, Tokens10, _EndLine10} = leveled_evallexer:string(EvalString10),
    {ok, ParsedExp10} = leveled_evalparser:parse(Tokens10),
    EvalOut10A =
        apply_eval(
                ParsedExp10,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"37">>, maps:get(<<"age">>, EvalOut10A)),
    ?assertMatch(38, maps:get(<<"age_next">>, EvalOut10A)),
    EvalOut10B =
        apply_eval(
                ParsedExp10,
                <<"SMITH|19860216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"38">>, maps:get(<<"age">>, EvalOut10B)),
    EvalString10F =
        EvalString1 ++ 
        " | index($dob, 0, 4, $yob)"
        " | index($dob, 4, 4, $birthday)"
        " | map($birthday, <=, ((\"0430\", 2024)), 2023, $yoc)"
        " | subtract($yoc, $yob, $age)"
            % yob has not been converted to an integer,
            % so the age will not be set
        " | to_string($age, $age)"
        ,
    {ok, Tokens10F, _EndLine10F} = leveled_evallexer:string(EvalString10F),
    {ok, ParsedExp10F} = leveled_evalparser:parse(Tokens10F),
    EvalOut10F =
        apply_eval(
                ParsedExp10F,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertNot(maps:is_key(<<"age">>, EvalOut10F)),

    EvalString11A =
        EvalString1 ++
            " | map($dob, <, "
                "((\"1946\", \"Silent\"), (\"1966\", \"Boomer\"),"
                "(\"1980\", \"GenX\"), (\"1997\", \"Millenial\")), \"GenZ\","
                " $generation)",
    {ok, Tokens11A, _EndLine11A} = leveled_evallexer:string(EvalString11A),
    {ok, ParsedExp11A} = leveled_evalparser:parse(Tokens11A),
    EvalOut11A =
        apply_eval(
                ParsedExp11A,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"Millenial">>, maps:get(<<"generation">>, EvalOut11A)),
    EvalString11B =
        EvalString1 ++
            " | map($dob, <=, "
                "((\"1945\", \"Silent\"), (\"1965\", \"Boomer\"),"
                "(\"1979\", \"GenX\"), (\"1996\", \"Millenial\")), \"GenZ\","
                " $generation)",
    {ok, Tokens11B, _EndLine11B} = leveled_evallexer:string(EvalString11B),
    {ok, ParsedExp11B} = leveled_evalparser:parse(Tokens11B),
    EvalOut11B =
        apply_eval(
                ParsedExp11B,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"Millenial">>, maps:get(<<"generation">>, EvalOut11B)),
    EvalString11C =
        EvalString1 ++
            " | map($dob, >, "
                "((\"1996\", \"GenZ\"), (\"1979\", \"Millenial\"),"
                "(\"1965\", \"GenX\"), (\"1945\", \"Boomer\")), \"Silent\","
                " $generation)",
    {ok, Tokens11C, _EndLine11C} = leveled_evallexer:string(EvalString11C),
    {ok, ParsedExp11C} = leveled_evalparser:parse(Tokens11C),
    EvalOut11C =
        apply_eval(
                ParsedExp11C,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"Millenial">>, maps:get(<<"generation">>, EvalOut11C)),
    EvalString11D =
        EvalString1 ++
            " | map($dob, >=, "
                "((\"1997\", \"GenZ\"), (\"1980\", \"Millenial\"),"
                "(\"1966\", \"GenX\"), (\"1946\", \"Boomer\")), \"Silent\","
                " $generation)",
    {ok, Tokens11D, _EndLine11D} = leveled_evallexer:string(EvalString11D),
    {ok, ParsedExp11D} = leveled_evalparser:parse(Tokens11D),
    EvalOut11D =
        apply_eval(
                ParsedExp11D,
                <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
                <<"9000000001">>,
                maps:new()
            ),
    ?assertMatch(<<"Millenial">>, maps:get(<<"generation">>, EvalOut11D)),

    EvalString12 =
        "kvsplit($term, \"|\", \"=\") | index($term, 0, 12, $ts) |"
        " to_integer($ts, $ts) |"
        " to_integer($DEBUG, $DEBUG) |"
        " to_integer($INFO, $INFO) |"
        " to_integer($WARN, $WARN) |"
        " to_integer($ERROR, $ERROR) |"
        " to_integer($CRITICAL, $CRITICAL) |"
        " add($DEBUG, $INFO, $TOTAL) |"
        " add($TOTAL, $WARN, $TOTAL) |"
        " add($TOTAL, $ERROR, $TOTAL) |"
        " add($TOTAL, $CRITICAL, $TOTAL)"
        ,
    {ok, Tokens12, _EndLine12} = leveled_evallexer:string(EvalString12),
    {ok, ParsedExp12} = leveled_evalparser:parse(Tokens12),
    EvalOut12 =
        apply_eval(
                ParsedExp12,
                <<"063881703147|DEBUG=804|INFO=186|WARN=10">>,
                <<"ABC1233">>,
                maps:new()
            ),
    ?assertMatch(63881703147, maps:get(<<"ts">>, EvalOut12)),
    ?assertMatch(1000, maps:get(<<"TOTAL">>, EvalOut12)),
    ?assertNot(maps:is_key(<<"CRITICAL">>, EvalOut12)),

    EvalString13 =
        "kvsplit($term, \"|\", \":\") |"
        " map($cup_year, =, "
            "((\"1965\", \"bad\"), (\"1970\", \"bad\"), "
            "(\"1972\", \"good\"), (\"1974\", \"bad\")), "
            "\"indifferent\", $cup_happy) ",
    {ok, Tokens13, _EndLine13} = leveled_evallexer:string(EvalString13),
    {ok, ParsedExp13} = leveled_evalparser:parse(Tokens13),
    EvalOut13A =
        apply_eval(ParsedExp13, <<"cup_year:1972">>, <<"ABC1">>, maps:new()),
    ?assertMatch(<<"good">>, maps:get(<<"cup_happy">>, EvalOut13A)),
    EvalOut13B =
        apply_eval(ParsedExp13, <<"cup_year:1970">>, <<"ABC1">>, maps:new()),
    ?assertMatch(<<"bad">>, maps:get(<<"cup_happy">>, EvalOut13B)),
    EvalOut13C =
        apply_eval(ParsedExp13, <<"cup_year:2024">>, <<"ABC1">>, maps:new()),
    ?assertMatch(<<"indifferent">>, maps:get(<<"cup_happy">>, EvalOut13C)),

    ExtractRegex =
        "(?P<fn>[^\\|]*)\\|(?P<dob>[0-9]{8})\\|(?P<dod>[0-9]{0,8})\\|"
        "(?P<gns>[^\\|]*)\\|(?P<pcs>[^\\|]*)|.",
    ok =
        check_regex_eval(
            "regex($term, :regex, pcre, ($fn, $dob, $dod, $gns, $pcs))",
            ExtractRegex
        ),
    ok =
        check_regex_eval(
            "regex($term, :regex, ($fn, $dob, $dod, $gns, $pcs))",
            ExtractRegex
        )
    .

unicode_test() ->
    EvalString1 = "delim($term, \"|\", ($fn, $dob, $dod, $gns, $pcs))",
    EvalString2 = "delim($gns, \"#\", ($gn1, $gn2, $gn3))",
    
    EvalString3 = EvalString1 ++ " | " ++ EvalString2,
    {ok, Tokens3, _EndLine3} = leveled_evallexer:string(EvalString3),
    {ok, ParsedExp3} = leveled_evalparser:parse(Tokens3),

    EvalOutUnicode0 =
        apply_eval(
            ParsedExp3,
            <<"ÅßERG|19861216||Willow#Mia|LS1 4BT#LS8 1ZZ"/utf8>>,
                % Note index terms will have to be unicode_binary() type
                % for this to work a latin-1 binary of
                % <<"ÅßERG|19861216||Willow#Mia|LS1 4BT#LS8 1ZZ">> will fail to
                % match - use unicode:characters_to_binary(B, latin1, utf8) to
                % convert
            <<"9000000001">>,
            maps:new()
            ),
    ?assertMatch(<<"ÅßERG"/utf8>>, maps:get(<<"fn">>, EvalOutUnicode0)),
    FE19 = "begins_with($fn, :prefix)",
    {ok, Filter19} =
        leveled_filter:generate_filter_expression(
            FE19,
            #{<<"prefix">> => <<"ÅßE"/utf8>>}
        ),
    ?assert(
        leveled_filter:apply_filter(
            Filter19,
            EvalOutUnicode0
        )
    ),
    
    EvalString4 = EvalString1 ++ "| slice($gns, 2, $gns)",
    {ok, Tokens4, _EndLine4} = leveled_evallexer:string(EvalString4),
    {ok, ParsedExp4} = leveled_evalparser:parse(Tokens4),
    EvalOutUnicode1 =
        apply_eval(
            ParsedExp4,
            <<"ÅßERG|19861216||Åbß0Ca|LS1 4BT#LS8 1ZZ"/utf8>>,
            <<"9000000001">>,
            maps:new()
            ),
    FE20 = ":gsc_check IN $gns",
    {ok, Filter20} =
        leveled_filter:generate_filter_expression(
            FE20,
            #{<<"gsc_check">> => <<"Åb"/utf8>>}
        ),
    ?assert(
        leveled_filter:apply_filter(
            Filter20,
            EvalOutUnicode1
        )
    ),
    {ok, Filter21} =
        leveled_filter:generate_filter_expression(
            FE20,
            #{<<"gsc_check">> => <<"ß0"/utf8>>}
        ),
    ?assert(
        leveled_filter:apply_filter(
            Filter21,
            EvalOutUnicode1
        )
    ),
    {ok, Filter22} =
        leveled_filter:generate_filter_expression(
            FE20,
            #{<<"gsc_check">> => <<"Ca">>}
        ),
    ?assert(
        leveled_filter:apply_filter(
            Filter22,
            EvalOutUnicode1
        )
    ),
    {ok, Filter23} =
        leveled_filter:generate_filter_expression(
            FE20,
            #{<<"gsc_check">> => <<"Ca"/utf8>>}
        ),
    ?assert(
        leveled_filter:apply_filter(
            Filter23,
            EvalOutUnicode1
        )
    )
    .


check_regex_eval(EvalString14, ExtractRegex) ->
    {ok, ParsedExp14} =
        generate_eval_expression(
            EvalString14,
            #{<<"regex">> => list_to_binary(ExtractRegex)}  
        ),
    EvalOut14 =
        apply_eval(
            ParsedExp14,
            <<"SMITH|19861216||Willow#Mia#Vera|LS1 4BT#LS8 1ZZ">>,
            <<"9000000001">>,
            maps:new()
        ),
    ?assertMatch(<<"SMITH">>, maps:get(<<"fn">>, EvalOut14)),
    ok.

bad_type_test() ->
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
    ?assertNot(maps:is_key(<<"doh">>, EvalOut9)),
    
    EvalStringF1 = EvalString9 ++ " | delim($height, \"|\", ($foo, $bar))",
    {ok, TokensF1, _EndLineF1} = leveled_evallexer:string(EvalStringF1),
    {ok, ParsedExpF1} = leveled_evalparser:parse(TokensF1),
    EvalOutF1 =
        apply_eval(
            ParsedExpF1,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"foo">>, EvalOutF1)),
    ?assertNot(maps:is_key(<<"bar">>, EvalOutF1)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF1)),
    
    EvalStringF2 = EvalString9 ++ " | split($height, \"|\", $foo)",
    {ok, TokensF2, _EndLineF2} = leveled_evallexer:string(EvalStringF2),
    {ok, ParsedExpF2} = leveled_evalparser:parse(TokensF2),
    EvalOutF2 =
        apply_eval(
            ParsedExpF2,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"foo">>, EvalOutF2)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF2)),

    EvalStringF3 = EvalString9 ++ " | slice($height, 1, $foo)",
    {ok, TokensF3, _EndLineF3} = leveled_evallexer:string(EvalStringF3),
    {ok, ParsedExpF3} = leveled_evalparser:parse(TokensF3),
    EvalOutF3 =
        apply_eval(
            ParsedExpF3,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"foo">>, EvalOutF3)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF3)),

    EvalStringF4 = EvalString9 ++ " | index($height, 1, 1, $foo)",
    {ok, TokensF4, _EndLineF4} = leveled_evallexer:string(EvalStringF4),
    {ok, ParsedExpF4} = leveled_evalparser:parse(TokensF4),
    EvalOutF4 =
        apply_eval(
            ParsedExpF4,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"foo">>, EvalOutF4)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF4)),

    EvalStringF5 = EvalString9 ++ " | kvsplit($height, \"|\", \"#\")",
    {ok, TokensF5, _EndLineF5} = leveled_evallexer:string(EvalStringF5),
    {ok, ParsedExpF5} = leveled_evalparser:parse(TokensF5),
    EvalOutF5 =
        apply_eval(
            ParsedExpF5,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"foo">>, EvalOutF5)),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF5)),

    EvalStringF6 = EvalString9 ++ " | to_integer($height, $height_int)",
    {ok, TokensF6, _EndLineF6} = leveled_evallexer:string(EvalStringF6),
    {ok, ParsedExpF6} = leveled_evalparser:parse(TokensF6),
    EvalOutF6 =
        apply_eval(
            ParsedExpF6,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertMatch(224, maps:get(<<"height">>, EvalOutF6)),
    ?assertMatch(224, maps:get(<<"height_int">>, EvalOutF6)),

    EvalStringF7 = EvalString9 ++ " | to_string($name, $name_str)",
    {ok, TokensF7, _EndLineF7} = leveled_evallexer:string(EvalStringF7),
    {ok, ParsedExpF7} = leveled_evalparser:parse(TokensF7),
    EvalOutF7 =
        apply_eval(
            ParsedExpF7,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertMatch(<<"WEMBANYAMA">>, maps:get(<<"name">>, EvalOutF7)),
    ?assertMatch(<<"WEMBANYAMA">>, maps:get(<<"name_str">>, EvalOutF7)),

    EvalStringF8 =
        EvalString9 ++
        " | regex($height, :regex, ($height_int)) |"
        " to_integer($height_int, $height_int)",
    
    {ok, ParsedExpF8} =
        generate_eval_expression(
            EvalStringF8,
            #{<<"regex">> => list_to_binary("(?P<height_int>[0-9]+)")}
        ),
    EvalOutF8 =
        apply_eval(
            ParsedExpF8,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
    ?assertNot(maps:is_key(<<"height_int">>, EvalOutF8)),

    EvalStringF9 =
        EvalString9 ++
        " | to_string($height, $height)"
        " | regex($height, :regex, ($height_int)) |"
        " to_integer($height_int, $height_int)",
    
    {ok, ParsedExpF9} =
        generate_eval_expression(
            EvalStringF9,
            #{<<"regex">> => list_to_binary("(?P<height_int>[0-9]+)")}
        ),
    EvalOutF9 =
        apply_eval(
            ParsedExpF9,
            <<"WEMBANYAMA|224cm|95kg|#1">>,
            <<"SPURS|00001">>,
            maps:new()
        ),
        ?assertMatch(224, maps:get(<<"height_int">>, EvalOutF9))
    .
    

generate_test() ->
    EvalString13 =
        "kvsplit($term, \"|\", \":\") |"
        " map($cup_year, =, "
            "((\"1965\", \"bad\"), (\"1970\", \"bad\"), "
            "(:clarke, \"good\"), (\"1974\", \"bad\")), "
            "\"indifferent\", $cup_happy) ",
    {ok, ParsedExp13} =
        generate_eval_expression(EvalString13, #{<<"clarke">> => <<"1972">>}),
    EvalOut13A =
        apply_eval(ParsedExp13, <<"cup_year:1972">>, <<"ABC1">>, maps:new()),
    ?assertMatch(<<"good">>, maps:get(<<"cup_happy">>, EvalOut13A)),
    ?assertMatch(
        {error, "Substitution <<\"clarke\">> not found"},
        generate_eval_expression(EvalString13, maps:new())
    ).

-endif.