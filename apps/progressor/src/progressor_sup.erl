-module(progressor_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = lists:foldl(
        fun({ID, _NsOpts} = NS, Acc) ->
            [
                #{
                    id => ID,
                    start => {prg_namespace_sup, start_link, [NS]},
                    type => supervisor
                } | Acc
            ]
        end,
        [],
        namespaces()
    ),
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

namespaces() ->
    application:get_env(progressor, namespaces, []).
