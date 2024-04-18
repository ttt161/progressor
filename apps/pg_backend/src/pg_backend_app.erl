%%%-------------------------------------------------------------------
%% @doc pg_backend public API
%% @end
%%%-------------------------------------------------------------------

-module(pg_backend_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Databases = application:get_env(pg_backend, databases, []),
    Pools = application:get_env(pg_backend, pools, []),
    ok = start_pools(Pools, maps:from_list(Databases)),
    pg_backend_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

start_pools(Pools, Databases) ->
    lists:foreach(
        fun({PoolName, Opts}) ->
            #{
                database := DB,
                size := Size
            } = maps:from_list(Opts),
            DbParams = maps:get(DB, Databases),
            {ok, _} = epgsql_pool:start(PoolName, Size, Size, maps:from_list(DbParams))
        end,
        Pools
    ).
