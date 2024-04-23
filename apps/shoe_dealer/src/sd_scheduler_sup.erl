-module(sd_scheduler_sup).

-export([child_spec/3]).
-export([start_link/2]).

%%

child_spec(ID, Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [ID, Options]},
        restart => permanent,
        type => supervisor
    }.

start_link(SchedulerID, Options) ->
    ManagerOptions = maps:with(
        [start_interval, capacity],
        Options
    ),
    ScannerOptions = maps:with(
        [queue_handler, max_scan_limit, scan_ahead, retry_scan_delay],
        Options
    ),
    WorkerOptions = maps:with(
        [task_handler],
        Options
    ),
    sd_adhoc_sup:start_link(
        #{strategy => one_for_all},
        [
            sd_queue_scanner:child_spec(SchedulerID, ScannerOptions, queue),
            sd_scheduler_worker:child_spec(SchedulerID, WorkerOptions, tasks),
            sd_scheduler:child_spec(SchedulerID, ManagerOptions, manager)
        ]
    ).
