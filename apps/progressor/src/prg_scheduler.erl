-module(prg_scheduler).

-include("progressor.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([handle_continue/2]).

%% API
-export([push_task/3]).
-export([pop_task/2]).
-export([continuation_task/3]).

-record(prg_scheduler_state, {ns_id, ns_opts, timers, ready, free_workers}).

%%%
%%% API
%%%

-spec push_task(namespace_id(), task_type(), task()) -> ok.
push_task(NsId, TaskType, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:cast(RegName, {push_task, TaskType, Task}).

-spec pop_task(namespace_id(), pid()) -> {task_type(), task()} | not_found.
pop_task(NsId, Worker) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {pop_task, Worker}).

-spec continuation_task(namespace_id(), pid(), task()) -> {task_type(), task()} | ok.
continuation_task(NsId, Worker, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {continuation_task, Worker, Task}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link({NsId, _NsOpts} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:start_link({local, RegName}, ?MODULE, NS, []).

init({NsId, NsOpts}) ->
    Opts = maps:from_list(NsOpts),
    _ = start_workers(NsId, Opts),
    State = #prg_scheduler_state{
        ns_id = NsId,
        ns_opts = Opts,
        timers = #{},
        ready = queue:new(),
        free_workers = queue:new()
    },
    {ok, State, {continue, scan_tasks}}.

handle_continue(scan_tasks, State = #prg_scheduler_state{ns_id = NsId, ns_opts = NsOpts}) ->
    Tasks = search_tasks(NsId, NsOpts),
    NewState = lists:foldl(fun(Task, Acc) -> do_push_task(signal, Task, Acc) end, State, Tasks),
    {noreply, NewState}.

handle_call({pop_task, Worker}, _From, State) ->
    case queue:out(State#prg_scheduler_state.ready) of
        {{value, TaskData}, NewReady} ->
            {reply, TaskData, State#prg_scheduler_state{ready = NewReady}};
        {empty, _} ->
            Workers = State#prg_scheduler_state.free_workers,
            {reply, not_found, State#prg_scheduler_state{free_workers = queue:in(Worker, Workers)}}
    end;
handle_call({continuation_task, Task}, _From, State) ->
    case queue:out(State#prg_scheduler_state.ready) of
        {{value, TaskData}, NewReady} ->
            {reply, TaskData, State#prg_scheduler_state{ready = queue:in({signal, Task}, NewReady)}};
        {empty, _} ->
            {reply, ok, State}
    end;
handle_call(_Request, _From, State = #prg_scheduler_state{}) ->
    {reply, ok, State}.

handle_cast({push_task, Type, Task}, State) ->
    NewState = do_push_task(Type, Task, State),
    {noreply, NewState};
handle_cast(_Request, State = #prg_scheduler_state{}) ->
    {noreply, State}.

handle_info(
    {timeout, TimerRef, task_timer},
    State = #prg_scheduler_state{timers = Timers, free_workers = Workers, ready = Ready}
) ->
    {{TaskType, Task} = TaskData, NewTimers} = maps:take(TimerRef, Timers),
    NewState = case queue:out(Workers) of
        {{value, Worker}, NewWorkers} ->
            ok = prg_worker:process_task(Worker, TaskType, Task),
            State#prg_scheduler_state{timers = NewTimers, free_workers = NewWorkers};
        {empty, _} ->
            State#prg_scheduler_state{timers = NewTimers, ready = queue:in(TaskData, Ready)}
    end,
    {noreply, NewState};
handle_info(_Info, State = #prg_scheduler_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_scheduler_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_scheduler_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_workers(NsId, NsOpts) ->
    WorkerPoolSize = maps:get(worker_pool_size, NsOpts, ?DEFAULT_WORKER_POOL_SIZE),
    WorkerSup = prg_utils:registered_name(NsId, "_worker_sup"),
    lists:foreach(fun(N) ->
        supervisor:start_child(WorkerSup, [N])
    end, lists:seq(1, WorkerPoolSize)).

search_tasks(NsId, #{storage := StorageOpts}) ->
    prg_storage:search_tasks(maps:from_list(StorageOpts), NsId).

do_push_task(TaskType, #{timestamp := Ts} = Task, State) ->
    Now = erlang:system_time(second),
    case Ts > Now of
        true ->
            OldTimers = State#prg_scheduler_state.timers,
            Timeout = Ts - Now,
            TRef = erlang:start_timer(Timeout * 1000, self(), task_timer),
            State#prg_scheduler_state{timers = OldTimers#{TRef => {TaskType, Task}}};
        false ->
            FreeWorkers = State#prg_scheduler_state.free_workers,
            case queue:out(FreeWorkers) of
                {{value, Worker}, NewQueue} ->
                    ok = prg_worker:process_task(Worker, TaskType, Task),
                    State#prg_scheduler_state{free_workers = NewQueue};
                {empty, _} ->
                    OldReady = State#prg_scheduler_state.ready,
                    State#prg_scheduler_state{ready = queue:in({TaskType, Task}, OldReady)}
            end
    end.
