%% -------- Filter Functions ---------
%%
%% Support for different filter expressions within leveled 
%%

-module(leveled_filter).

-export([validate_comparison_expression/2]).

%%%============================================================================
%%% External API
%%%============================================================================

%% @doc validate a comaprison expression
%% A comparison expression allows for string comparisons to be made using
%% >, <, >=, =< with andalso, or as well as () to group different expressions
%% The output is a function which will be passed a Map where the Map may be the
%% result of reading some projected attributes on an index string.
%% To compare a string with a key in the map, the name of the key must begin
%% with a "$".
%% In creating the function, the potential keys must be known and passed as a
%% string in the list (without the leading $). 
-spec validate_comparison_expression(
    string(), list(string())) ->
        fun((map()) -> {value, boolean(), map()}) |
        {error, string(), erl_anno:location()}.
validate_comparison_expression(
        Expression, AvailableArgs) when is_list(Expression) ->
    case erl_scan:string(Expression) of
        {ok, Tokens, _EndLoc} ->
            case vert_compexpr_tokens(Tokens, AvailableArgs, []) of
                {ok, UpdTokens} ->
                    case erl_parse:parse_exprs(UpdTokens) of
                        {ok, [Form]} ->
                            io:format("Form ~p~n", [Form]),
                            fun(LookupMap) ->
                                erl_eval:expr(Form, LookupMap)
                            end;
                        {error, ErrorInfo} ->
                            {error, ErrorInfo, undefined}
                    end;
                {error, Error, ErrorLocation} ->
                    {error, lists:flatten(Error), ErrorLocation}
            end;
        {error, Error, ErrorLocation} ->
            {error, Error, ErrorLocation}
    end;
validate_comparison_expression(_Expression, _AvailableArgs) ->
    {error, "Expression not a valid string", undefined}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

vert_compexpr_tokens([], _Args, ParsedTokens) ->
    {ok, lists:reverse(ParsedTokens)};
vert_compexpr_tokens([{dot, LN}], Args, ParsedTokens) ->
    vert_compexpr_tokens([], Args, [{dot, LN}|ParsedTokens]);
vert_compexpr_tokens(L, _Args, _ParsedTokens) when length(L) == 1 ->
    {error, "Query does not end with `.`", undefined};
vert_compexpr_tokens([{string, LN, String}|Rest], Args, ParsedTokens) ->
    case hd(String) of
        36 -> % 36 == $
            Arg = tl(String),
            case lists:member(Arg, Args) of
                true ->
                    VarToken = {var, LN, list_to_atom(Arg)},
                    vert_compexpr_tokens(Rest, Args, [VarToken|ParsedTokens]);
                false ->
                    {error, io_lib:format("Unavailable Arg ~s", [Arg]), LN}
            end;
        _ -> % Not a key, treat as a string
            vert_compexpr_tokens(
                Rest, Args, [{string, LN, String}|ParsedTokens])
    end;
vert_compexpr_tokens([{'(',LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'(',LN}|ParsedTokens]);
vert_compexpr_tokens([{')',LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{')',LN}|ParsedTokens]);
vert_compexpr_tokens([{'andalso', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'andalso', LN}|ParsedTokens]);
vert_compexpr_tokens([{'or', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'or', LN}|ParsedTokens]);
vert_compexpr_tokens([{'>', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'>', LN}|ParsedTokens]);
vert_compexpr_tokens([{'<', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'<', LN}|ParsedTokens]);
vert_compexpr_tokens([{'>=', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'>=', LN}|ParsedTokens]);
vert_compexpr_tokens([{'=<', LN}|Rest], Args, ParsedTokens) ->
    vert_compexpr_tokens(Rest, Args, [{'=<', LN}|ParsedTokens]);
vert_compexpr_tokens([InvalidToken|_Rest], _Args, _ParsedTokens) ->
    {error, io_lib:format("Invalid token ~w", [InvalidToken]), undefined}.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

good_querystring_test() ->
    QueryString =
        "(\"$dob\" >= \"19740301\" andalso \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\" andalso \"$dod\" < \"20230101\").",
    AvailableArgs = ["dob", "dod"],
    F = validate_comparison_expression(QueryString, AvailableArgs),
    M1 = maps:from_list([{dob, "19750202"}, {dod, "20221216"}]),
    M2 = maps:from_list([{dob, "19750202"}, {dod, "20191216"}]),
    M3 = maps:from_list([{dob, "19790202"}, {dod, "20221216"}]),
    M4 = maps:from_list([{dob, "19790202"}, {dod, "20191216"}]),
    M5 = maps:from_list([{dob, "19790202"}, {dod, "20241216"}]),
    ?assertMatch(true, element(2, F(M1))),
    ?assertMatch(true, element(2, F(M2))),
    ?assertMatch(true, element(2, F(M3))),
    ?assertMatch(false, element(2, F(M4))),
    ?assertMatch(false, element(2, F(M5))).

bad_querystring_test() ->
    QueryString =
        "(\"$dob\" >= \"19740301\" andalso \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\").",
    BadString1 =
        "(\"$dob\" >= \"19740301\" andalso \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\")",
    BadString2 =
        "(\"$dob\" >= \"19740301\" andalso \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\")).",
    BadString3 =
        "(\"$dob\" >= \"19740301\" and \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\").",
    BadString4 = "os:cmd(foo)",
    BadString5 =
        [42,63,52,10,240,159,140,190] ++
        "(\"$dob\" >= \"19740301\" andalso \"$dob\" =< \"19761030\")"
        " or (\"$dod\" > \"20200101\").",
    BadString6 = [(16#110000 + 1)|QueryString],
    BadString7 = <<42:32/integer>>,
    ?assertMatch(
        {error, "Unavailable Arg dod", 1},
        validate_comparison_expression(QueryString, ["dob", "pv"])
    ),
    ?assertMatch(
        {error, "Query does not end with `.`", undefined},
        validate_comparison_expression(BadString1, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, {1 ,erl_parse, ["syntax error before: ","')'"]}, undefined},
        validate_comparison_expression(BadString2, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, "Invalid token {'and',1}", undefined},
        validate_comparison_expression(BadString3, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, "Invalid token {atom,1,os}", undefined},
        validate_comparison_expression(BadString4, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, "Invalid token {'*',1}", undefined},
        validate_comparison_expression(BadString5, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, {1, erl_scan, {illegal,character}}, 1},
        validate_comparison_expression(BadString6, ["dob", "dod"])
    ),
    ?assertMatch(
        {error, "Expression not a valid string", undefined},
        validate_comparison_expression(BadString7, ["dob", "dod"])
    ).

-endif.
