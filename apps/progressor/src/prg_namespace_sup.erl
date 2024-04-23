-module(prg_namespace_sup).

-include("progressor.hrl").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link({namespace_id(), namespace_opts()}) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({NsId, _} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_namespace_sup"),
    supervisor:start_link({local, RegName}, ?MODULE, NS).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init({NsId, NsOpts}) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = #{strategy => one_for_all,
        intensity => MaxRestarts,
        period => MaxSecondsBetweenRestarts},
    SchedulerSpec = scheduler_spec(NsId, NsOpts),
    Specs = [
        SchedulerSpec
    ],

    {ok, {SupFlags, Specs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

scheduler_spec(NsId, NsOpts) ->
    QueueHandler = {prg_generic_queue, maps:merge(maps:from_list(NsOpts), #{
        ns_id => NsId,
        processing_timeout => 60000,
        min_scan_delay => 1000,
        lookahead => 60
    })},
    SchedulerOptions = #{
        start_interval => 1000,
        capacity => 3500,
        queue_handler => QueueHandler,
        max_scan_limit => 2000,
        scan_ahead => {1.0, 0},
        retry_scan_delay => 1000,
        task_handler => QueueHandler
    },
    sd_scheduler_sup:child_spec(
        prg_utils:registered_name(NsId, "_scheduler"),
        SchedulerOptions,
        {scheduler, NsId}
    ).
