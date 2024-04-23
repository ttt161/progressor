-module(prg_generic_queue).

-behaviour(sd_queue_scanner).
-export([init/1]).
-export([search_tasks/3]).

-behaviour(sd_scheduler_worker).
-export([execute_task/2]).

%% Types

-record(state, {}).

-define(STATUS_RUNNING, <<"running">>).
-define(STATUS_WAITING, <<"waiting">>).
-define(STATUS_FINISHED, <<"finished">>).

%%
%% API
%%

init(_Opts) ->
    {ok, #state{}}.

search_tasks(Options = #{ns_id := NsId}, Limit, State) ->
    %% Достаем все таски относящиееся к машинам в статусе `waiting`
    CurrentTs = sd_queue_task:current_time(),
    Lookahead = maps:get(lookahead, Options, 0),
    Tasks = lists:map(fun wrap_task/1, search_tasks(Options, NsId, 1, CurrentTs + Lookahead, Limit)),
    %% TODO Если есть ещё таски до скорректировать задержку скана
    %%      относительно отметки времени последней таски
    MinDelay = maps:get(min_scan_delay, Options, 1000),
    OptimalDelay = seconds_to_delay(Lookahead),
    Delay = erlang:max(OptimalDelay, MinDelay),
    {{Delay, Tasks}, State}.

execute_task(Options, Task = #{payload := {Type, PayloadTask}}) ->
    %% Сейчас нет никаких гарантий что две таски по одному процессу не
    %% будут исполняться в одно время.  Чтобы добавить эту гарантию
    %% надо чтобы таска лишь делала вызов в глобально и однозначно
    %% идентифицируемый выделенный эрланг процесс в котором и
    %% выполнять необходимые операции над состоянием -- таким образом
    %% работа по таскам будет хотябы упорядочена.
    reply(Task, process_task(Options, extract_task_type(Type), PayloadTask)).

%% Processing

process_task(Options, init, #{process_id := ProcessId} = Task) ->
    call_processor(Options, init, Task, new_process(ProcessId));
process_task(Options, TypeOrImpact, #{process_id := ProcessId} = Task) ->
    Process = load_process(Options, ProcessId),
    case validate_type(Options, TypeOrImpact, Process) of
        ok ->
            call_processor(Options, TypeOrImpact, Task, Process);
        {error, Reason} ->
            %% Log it?
            {reply, {error, Reason}}
    end.

validate_type(_Options, signal, #{status := Status}) when Status =/= ?STATUS_WAITING ->
    {error, <<"toO LaTE">>};
validate_type(_Options, _TypeOrImpact, _Process) ->
    ok.

call_processor(Options, TypeOrImpact, #{args := Args} = Task, Process) ->
    case prg_processor:call(processor_opts(Options), {TypeOrImpact, Args, Process}) of
        {ok, Result} ->
            ok = handle_process_transition(Options, Result, Process),
            ok = handle_timer_scheduling(Options, Result, Process),
            {reply, maps:get(response, Result, undefined)};
        {error, Reason} ->
            handle_processor_error(Options, TypeOrImpact, Task, Process, Reason)
    end.

handle_timer_scheduling(Options, #{action := #{set_timer := Timestamp}}, Process) ->
    NsId = ns_id(Options),
    StorageOpts = storage_opts(Options),
    #{process_id := ProcessId} = Process,
    NewTask = #{
        process_id => ProcessId,
        timestamp => Timestamp,
        args => #{},
        last_retry_interval => 0,
        attempts_count => 0
    },
    ok = prg_storage:save_task(StorageOpts, NsId, NewTask),
    ok = sd_scheduler:send_task(prg_utils:registered_name(NsId, "_scheduler"), wrap_task(NewTask));
handle_timer_scheduling(_Options, _NoTimerAction, _Process) ->
    ok.

handle_process_transition(Options, #{events := Events} = Result, Process) ->
    NsId = ns_id(Options),
    StorageOpts = storage_opts(Options),
    #{metadata := OldMeta, history := OldHistory, process_id := ProcessId} = Process,
    Status = case Result of
        #{action := #{set_timer := _}} -> ?STATUS_WAITING;
        _ -> ?STATUS_FINISHED
    end,
    Meta = maps:get(metadata, Result, OldMeta),
    NewHistory = Events ++ OldHistory,
    NewProcess = Process#{metadata => Meta, history => NewHistory, status => Status},
    ok = prg_storage:save_process(StorageOpts, NsId, NewProcess),
    ok = prg_storage:save_events(StorageOpts, NsId, ProcessId, Events).

handle_processor_error(Options, TypeOrImpact, Task, Process, Reason) ->
    NsId = ns_id(Options),
    StorageOpts = storage_opts(Options),
    RetryPolicy = maps:get(retry_policy, Options, [
        {initial_timeout, 5},
        {backoff_coefficient, 1.0},
        {max_attempts, 3}
    ]),
    case reschedule(TypeOrImpact, Task, maps:from_list(RetryPolicy), Reason) of
        {ok, NewTask} ->
            ok = prg_storage:save_task(StorageOpts, NsId, NewTask),
            noreply;
        {error, not_retryable} ->
            NewProcess = Process#{status => <<"error">>, detail => Reason},
            ok = prg_storage:save_process(StorageOpts, NsId, NewProcess),
            {reply, {error, Reason}}
    end.

%%
%% Util
%%

reschedule(TaskType, Task, RetryPolicy, Error) ->
    #{
        initial_timeout := InitialTimeout,
        backoff_coefficient := Backoff
    } = RetryPolicy,
    #{
        last_retry_interval := LastInterval,
        attempts_count := Count
    } = Task,
    Timeout = case LastInterval =:= 0 of
                  true -> InitialTimeout;
                  false -> LastInterval * Backoff
              end,
    Attempts = Count + 1,
    IsRetryable =
        TaskType =:= signal andalso
        Timeout < maps:get(max_timeout, RetryPolicy, infinity) andalso
        Attempts < maps:get(max_attempts, RetryPolicy, infinity) andalso
        not lists:any(fun(E) -> Error =:= E end, maps:get(non_retryable_errors, RetryPolicy, [])),
    case IsRetryable of
        true ->
            {ok, Task#{
                timestamp => erlang:system_time(second) + Timeout,
                last_retry_interval => Timeout,
                attempts_count => Attempts
            }};
        false ->
            {error, not_retryable}
    end.

processor_opts(#{processor := ProcessorOpts}) ->
    maps:from_list(ProcessorOpts).

storage_opts(#{storage := StorageOpts}) ->
    maps:from_list(StorageOpts).

ns_id(#{ns_id := NsId}) ->
    NsId.

new_process(ProcessId) ->
    #{
        process_id => ProcessId,
        status => ?STATUS_RUNNING,
        detail => <<>>,
        history => [],
        metadata => #{}
    }.

load_process(Options, ProcessId) ->
    prg_storage:get_process(storage_opts(Options), ns_id(Options), ProcessId).

reply(#{payload := {{_Type, Pid, Ref}, _}}, {reply, Result}) ->
    Pid ! {Ref, Result},
    ok;
reply(_Task, _Result) ->
    ok.

wrap_task(Task = #{process_id := ProcessId, timestamp := Timestamp}) ->
    #{
        id => ProcessId,
        target_time => Timestamp,
        machine_id => ProcessId,
        payload => {signal, Task}
    }.

extract_task_type({Type, _, _}) ->
    Type;
extract_task_type(Type) ->
    Type.

search_tasks(Options, NsId, FromTs, ToTs, Limit) ->
    prg_storage:search_waiting_timer_tasks(storage_opts(Options), NsId, FromTs, ToTs, Limit).

seconds_to_delay(Seconds) ->
    erlang:convert_time_unit(Seconds, second, millisecond).
