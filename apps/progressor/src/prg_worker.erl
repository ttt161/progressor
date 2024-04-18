-module(prg_worker).

-behaviour(gen_server).

-include("progressor.hrl").

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([handle_continue/2]).

-export([process_task/3]).

-define(SERVER, ?MODULE).

-record(prg_worker_state, {ns_id, ns_opts, num, process}).

%%%
%%% API
%%%

-spec process_task(pid(), task_type(), task()) -> ok.
process_task(Worker, TaskType, Task) ->
    gen_server:cast(Worker, {process_task, TaskType, Task}).

-spec continuation_task(pid(), task()) -> ok.
continuation_task(Worker, Task) ->
    gen_server:cast(Worker, {continuation_task, Task}).

-spec next_task(pid()) -> ok.
next_task(Worker) ->
    gen_server:cast(Worker, next_task).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(NsId, NsOpts, Num) ->
    gen_server:start_link(?MODULE, [NsId, maps:from_list(NsOpts), Num], []).

init([NsId, #{storage := StorageOpts, processor := ProcessorOpts} = NsOpts, Num]) ->
    {ok, #prg_worker_state{
        ns_id = NsId,
        ns_opts = NsOpts#{storage => maps:from_list(StorageOpts), processor => maps:from_list(ProcessorOpts)},
        num = Num
    }, {continue, do_start}}.

handle_continue(do_start, State = #prg_worker_state{ns_id = NsId}) ->
    case prg_scheduler:pop_task(NsId, self()) of
        {TaskType, Task} ->
            ok = process_task(self(), TaskType, Task),
            {noreply, State};
        not_found ->
            {noreply, State}
    end.

handle_call(_Request, _From, State = #prg_worker_state{}) ->
    {reply, ok, State}.

handle_cast({process_task, {init, _, _} = TaskType, #{process_id := ProcessId} = Task}, State = #prg_worker_state{}) ->
    NewState = do_process_task(TaskType, Task, State#prg_worker_state{process = new_process(ProcessId)}),
    {noreply, NewState};
handle_cast({process_task, TaskType, Task}, State = #prg_worker_state{ns_id = NsId, ns_opts = NsOpts}) ->
    StorageOpts = maps:get(storage, NsOpts),
    Process = prg_storage:get_process(StorageOpts, NsId, maps:get(process_id, Task)),
    NewState = do_process_task(TaskType, Task, State#prg_worker_state{process = Process}),
    {noreply, NewState};
handle_cast({continuation_task, Task}, State = #prg_worker_state{}) ->
    NewState = do_process_task(signal, Task, State),
    {noreply, NewState};
handle_cast(next_task, State = #prg_worker_state{ns_id = NsId}) ->
    NewState =
        case prg_scheduler:pop_task(NsId, self()) of
            {TaskType, Task} ->
                do_process_task(TaskType, Task, State);
            not_found ->
                State
        end,
    {noreply, NewState};
handle_cast(_Request, State = #prg_worker_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #prg_worker_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_worker_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_worker_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_process(ProcessId) ->
    #{
        process_id => ProcessId,
        status => <<"running">>,
        detail => <<>>,
        history => [],
        metadata => #{}
    }.

maybe_reply({_, Receiver, Ref}, {error, _} = Error) ->
    progressor:reply(Receiver, {Ref, Error});
maybe_reply({_, Receiver, Ref}, undefined) ->
    progressor:reply(Receiver, {Ref, {ok, ok}});
maybe_reply({_, Receiver, Ref}, Response) ->
    progressor:reply(Receiver, {Ref, {ok, Response}});
maybe_reply(_, _) ->
    skip.

extract_task_type({Type, _, _}) ->
    Type;
extract_task_type(Type) ->
    Type.

update_if_retryable(TaskType, Task, RetryPolicy, Error) ->
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
            Task#{
                timestamp => erlang:system_time(second) + Timeout,
                last_retry_interval => Timeout,
                attempts_count => Attempts
            };
        false ->
            not_retryable
    end.

do_process_task(TaskType, Task, State = #prg_worker_state{ns_id = NsId, ns_opts = NsOpts, process = Process}) ->
    #{
        storage := StorageOpts,
        processor := ProcessorOpts
    } = NsOpts,
    RetryPolicy0 = maps:get(retry_policy, NsOpts, ?DEFAULT_RETRY_POLICY),
    RetryPolicy = maps:from_list(RetryPolicy0),
    #{
        process_id := ProcessId,
        args := Args
    } = Task,
    #{metadata := OldMeta, history := OldHistory} = Process,
    case prg_processor:call(ProcessorOpts, {extract_task_type(TaskType), Args, Process}) of
        {ok, #{action := #{set_timer := Timestamp}, events := Events} = Result} ->
            Meta = maps:get(metadata, Result, OldMeta),
            NewTask = #{
                process_id => ProcessId,
                timestamp => Timestamp,
                args => #{},
                last_retry_interval => 0,
                attempts_count => 0
            },
            NewProcess = Process#{metadata => Meta},
            ok = prg_storage:save_process(StorageOpts, NsId, NewProcess),
            ok = prg_storage:save_events(StorageOpts, NsId, ProcessId, Events),
            ok = prg_storage:save_task(StorageOpts, NsId, NewTask),
            _ = maybe_reply(TaskType, maps:get(response, Result, undefined)),
            case Timestamp > erlang:system_time(second) of
                true ->
                    ok = prg_scheduler:push_task(NsId, signal, NewTask),
                    ok = next_task(self()),
                    State#prg_worker_state{process = undefined};
                false ->
                    case prg_scheduler:continuation_task(NsId, self(), NewTask) of
                        ok ->
                            NewHistory = Events ++ OldHistory,
                            ok = continuation_task(self(), NewTask),
                            State#prg_worker_state{process = NewTask#{history => NewHistory}};
                        {OtherTaskType, OtherTask} ->
                            ok = process_task(self(), OtherTaskType, OtherTask),
                            State#prg_worker_state{process = undefined}
                    end
            end;
        {ok, #{events := Events} = Result} ->
            Meta = maps:get(metadata, Result, OldMeta),
            NewHistory = Events ++ OldHistory,
            NewProcess = Process#{metadata => Meta, history => NewHistory, status => <<"finished">>},
            ok = prg_storage:save_process(StorageOpts, NsId, NewProcess),
            ok = prg_storage:save_events(StorageOpts, NsId, ProcessId, Events),
            ok = prg_storage:delete_task(StorageOpts, NsId, ProcessId),
            _ = maybe_reply(TaskType, maps:get(response, Result, undefined)),
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {error, Reason} ->
            case update_if_retryable(TaskType, Task, RetryPolicy, Reason) of
                not_retryable ->
                    NewProcess = Process#{status => <<"error">>, detail => Reason},
                    ok = prg_storage:save_process(StorageOpts, NsId, NewProcess),
                    ok = prg_storage:delete_task(StorageOpts, NsId, ProcessId),
                    _ = maybe_reply(TaskType, {error, Reason}),
                    ok = next_task(self()),
                    State#prg_worker_state{process = undefined};
                NewTask ->
                    ok = prg_storage:save_task(StorageOpts, NsId, NewTask),
                    ok = prg_scheduler:push_task(NsId, signal, NewTask),
                    ok = next_task(self()),
                    State#prg_worker_state{process = undefined}
            end
    end.
