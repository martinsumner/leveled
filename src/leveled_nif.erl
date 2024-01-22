%% -------- Utility Functions ---------
%%
%% Nifs to potentially accelerate commons tasks
%%

-module(leveled_nif).

-on_load(init/0).

-export([magic_chash/1, pos_finder/3, int_checker/3]).

init() ->
    PrivDir = 
        case code:priv_dir(?MODULE) of
            {error, bad_name} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
            Path ->
                Path
        end,
    erlang:load_nif(filename:join(PrivDir, leveled), 0).


-spec magic_chash(binary()) -> 0..16#FFFFFFFF.
magic_chash(_) ->
    erlang:nif_error(?LINE).

-spec pos_finder(binary(), binary(), 32768..16#FFFF) -> list(1..128).
pos_finder(_, _, _) ->
    erlang:nif_error(?LINE).

-spec int_checker(binary(), 32768..16#FFFF, 32768..16#FFFF) -> boolean().
int_checker(_, _, _) ->
    erlang:nif_error(?LINE).
