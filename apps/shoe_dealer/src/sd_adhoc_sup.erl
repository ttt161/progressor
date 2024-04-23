-module(sd_adhoc_sup).

%% API
-export([start_link/2]).
-export([start_link/3]).

%% supervisor
-behaviour(supervisor).

-export([init/1]).

%%
%% API
%%
-spec start_link(supervisor:sup_flags(), [supervisor:child_spec()]) -> {ok, pid()} | ignore | {error, _}.
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

-spec start_link({local, atom()}, supervisor:sup_flags(), [supervisor:child_spec()]) -> {ok, pid()} | ignore | {error, _}.
start_link(RegName, Flags, ChildsSpecs) ->
    supervisor:start_link(RegName, ?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({supervisor:sup_flags(), [supervisor:child_spec()]}) -> ignore | {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, ChildsSpecs}}.
