-module(prg_storage).

-include("progressor.hrl").

%% Task management
-export([search_tasks/2]).
-export([search_waiting_timer_tasks/5]).
-export([save_task/3]).
-export([delete_task/3]).
%% Process management
-export([is_exists/3]).
-export([get_process/3]).
-export([save_process/3]).
%% Events management
-export([save_events/4]).

%% Task management
-spec search_tasks(storage_opts(), namespace_id()) -> [task()].
search_tasks(#{client := pg_backend, options := PgOpts}, NsId) ->
    pg_backend:search_tasks(maps:from_list(PgOpts), NsId).

search_waiting_timer_tasks(#{client := pg_backend, options := PgOpts}, NsId, FromTs, ToTs, Limit) ->
    pg_backend:search_waiting_timer_tasks(maps:from_list(PgOpts), NsId, FromTs, ToTs, Limit).

-spec save_task(storage_opts(), namespace_id(), task()) -> ok.
save_task(#{client := pg_backend, options := PgOpts}, NsId, Task) ->
    pg_backend:save_task(maps:from_list(PgOpts), NsId, Task).

-spec delete_task(storage_opts(), namespace_id(), id()) -> ok.
delete_task(#{client := pg_backend, options := PgOpts}, NsId, ProcessId) ->
    pg_backend:delete_task(maps:from_list(PgOpts), NsId, ProcessId).

%% Process management
-spec is_exists(storage_opts(), namespace_id(), id()) -> boolean().
is_exists(#{client := pg_backend, options := PgOpts}, NsId, Id) ->
    pg_backend:is_exists(maps:from_list(PgOpts), NsId, Id).

-spec get_process(storage_opts(), namespace_id(), id()) -> process().
get_process(#{client := pg_backend, options := PgOpts}, NsId, ProcessId) ->
    pg_backend:get_process(maps:from_list(PgOpts), NsId, ProcessId).

-spec save_process(storage_opts(), namespace_id(), process()) -> ok.
save_process(#{client := pg_backend, options := PgOpts}, NsId, Process) ->
    pg_backend:save_process(maps:from_list(PgOpts), NsId, Process).

%% Events management
-spec save_events(storage_opts(), namespace_id(), id(), [event()]) -> ok.
save_events(#{client := pg_backend, options := PgOpts}, NsId, ProcessId, Events) ->
    pg_backend:save_events(maps:from_list(PgOpts), NsId, ProcessId, Events).
