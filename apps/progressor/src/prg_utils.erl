-module(prg_utils).

%% API
-export([registered_name/2]).
-export([pipe/2]).

-spec registered_name(atom(), string()) -> atom().
registered_name(BaseAtom, PostfixStr) ->
    erlang:list_to_atom(erlang:atom_to_list(BaseAtom) ++ PostfixStr).

-spec pipe([function()], term()) -> term().
pipe([], Result) -> Result;
pipe(_Funs, {error, _} = Error) -> Error;
pipe([F | Rest], Acc) ->
    pipe(Rest, F(Acc)).
