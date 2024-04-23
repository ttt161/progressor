-module(sd_scheduler).

-export([child_spec/3]).
-export([start_link/2]).

-export([inquire/1]).
-export([send_task/2]).
-export([distribute_tasks/2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1]).
-export([handle_info/2]).
-export([handle_cast/2]).
-export([handle_call/3]).

%% Types

-type name() :: atom().
-type id() :: {name(), NS :: binary()}.
-export_type([id/0]).

-type task_id() :: sd_queue_task:id().
-type task() :: sd_queue_task:task().
-type target_time() :: sd_queue_task:target_time().

%% Internal types
-record(state, {
    id :: id(),
    capacity :: non_neg_integer(),
    timer :: timer:tref(),
    waiting_tasks :: task_queue(),
    active_tasks :: #{task_id() => pid()},
    task_monitors :: #{monitor() => task_id()}
}).
-type monitor() :: reference().

%%

-type task_set() :: #{task_id() => task()}.
-type task_rank() :: {target_time(), integer()}.

-record(task_queue, {
    runnable = #{} :: task_set(),
    runqueue = gb_trees:empty() :: gb_trees:tree(task_rank(), task_id()),
    counter = 1 :: integer()
}).

-type task_queue() :: #task_queue{}.

%%
%% API
%%

child_spec(ID, Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [ID, Options]},
        type => worker
    }.

start_link(ID, Options) ->
    gen_server:start_link({local, registered_name(ID)}, ?MODULE, {ID, Options}, []).

inquire(ID) ->
    gen_server:call(registered_name(ID), inquire).

send_task(ID, Task) ->
    gen_server:cast(registered_name(ID), {tasks, [Task]}).

distribute_tasks(Pid, Tasks) when is_pid(Pid) ->
    gen_server:cast(Pid, {tasks, Tasks}).

%% gen_server callbacks

init({ID, Options}) ->
    {ok, TimerRef} = timer:send_interval(maps:get(start_interval, Options, 1000), start),
    {ok, #state{
        id = ID,
        capacity = maps:get(capacity, Options),
        active_tasks = #{},
        task_monitors = #{},
        waiting_tasks = #task_queue{},
        timer = TimerRef
    }}.

handle_call(inquire, _From, State) ->
    Status = #{
        pid => self(),
        active_tasks => get_active_task_count(State),
        waiting_tasks => get_waiting_task_count(State),
        capacity => State#state.capacity
    },
    {reply, Status, State};
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

handle_cast({tasks, Tasks}, State0) ->
    State1 = add_tasks(Tasks, State0),
    State2 = start_new_tasks(State1),
    {noreply, State2};
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

handle_info({'DOWN', Monitor, process, _Object, _Info}, State0) ->
    State1 = forget_about_task(Monitor, State0),
    State2 = start_new_tasks(State1),
    {noreply, State2};
handle_info(start, State0) ->
    State1 = start_new_tasks(State0),
    {noreply, State1};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info received: ~p", [Info]),
    {noreply, State}.

% Process registration

registered_name({T, NSID}) ->
    erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ erlang:atom_to_list(T) ++ erlang:binary_to_list(NSID));
registered_name(ID) when is_atom(ID) ->
    erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ erlang:atom_to_list(ID)).

% Helpers

forget_about_task(Monitor, State) ->
    #state{active_tasks = Tasks, task_monitors = Monitors} = State,
    case maps:find(Monitor, Monitors) of
        {ok, TaskID} ->
            State#state{
                active_tasks = maps:remove(TaskID, Tasks),
                task_monitors = maps:remove(Monitor, Monitors)
            };
        error ->
            State
    end.

add_tasks(Tasks, State = #state{waiting_tasks = WaitingTasks}) ->
    NewWaitingTasks = lists:foldl(fun enqueue_task/2, WaitingTasks, Tasks),
    State#state{waiting_tasks = NewWaitingTasks}.

enqueue_task(
    Task = #{id := TaskID, target_time := TargetTime},
    Queue = #task_queue{runnable = Runnable, runqueue = RQ, counter = Counter}
) ->
    % TODO
    % Blindly overwriting a task with same ID here if there's one. This is not the best strategy out
    % there but sufficient enough. For example we could overwrite most recent legit task with an
    % outdated one appointed a bit late by some remote queue scanner.
    NewRunnable = Runnable#{TaskID => Task},
    % NOTE
    % Inclusion of the unique counter value here helps to ensure FIFO semantics among tasks with the
    % same target timestamp.
    NewRQ = gb_trees:insert({TargetTime, Counter}, TaskID, RQ),
    Queue#task_queue{runnable = NewRunnable, runqueue = NewRQ, counter = Counter + 1}.

start_new_tasks(State = #state{waiting_tasks = WaitingTasks}) ->
    NewTasksNumber = get_waiting_task_count(State),
    CurrentTime = unow(),
    Iterator = make_iterator(CurrentTime, WaitingTasks),
    start_multiple_tasks(NewTasksNumber, Iterator, State).

start_multiple_tasks(0, _Iterator, State) ->
    State;
start_multiple_tasks(N, Iterator, State) when N > 0 ->
    #state{
        id = ID,
        waiting_tasks = WaitingTasks,
        active_tasks = ActiveTasks,
        task_monitors = Monitors
    } = State,
    case next_task(Iterator) of
        {Rank, TaskID, IteratorNext} when not is_map_key(TaskID, ActiveTasks) ->
            % Task appears not to be running on the scheduler...
            case dequeue_task(Rank, WaitingTasks) of
                {Task = #{}, NewWaitingTasks} ->
                    % ...so let's start it.
                    {ok, Pid, Monitor} = sd_scheduler_worker:start_task(ID, Task),
                    NewState = State#state{
                        waiting_tasks = NewWaitingTasks,
                        active_tasks = ActiveTasks#{TaskID => Pid},
                        task_monitors = Monitors#{Monitor => TaskID}
                    },
                    start_multiple_tasks(N - 1, IteratorNext, NewState);
                {outdated, NewWaitingTasks} ->
                    % ...but the queue entry seems outdated, let's skip.
                    NewState = State#state{waiting_tasks = NewWaitingTasks},
                    start_multiple_tasks(N, IteratorNext, NewState)
            end;
        {_Rank, _TaskID, IteratorNext} ->
            % Task is running already, possibly with earlier target time, let's leave it for later.
            start_multiple_tasks(N, IteratorNext, State);
        none ->
            State
    end.

make_iterator(TargetTimeCutoff, #task_queue{runqueue = Queue}) ->
    {gb_trees:iterator(Queue), TargetTimeCutoff}.

next_task({Iterator, TargetTimeCutoff}) ->
    case gb_trees:next(Iterator) of
        {{TargetTime, _} = Rank, TaskID, IteratorNext} when TargetTime =< TargetTimeCutoff ->
            {Rank, TaskID, {IteratorNext, TargetTimeCutoff}};
        {{TargetTime, _}, _, _} when TargetTime > TargetTimeCutoff ->
            none;
        none ->
            none
    end.

dequeue_task(Rank = {TargetTime, _}, Queue = #task_queue{runnable = Runnable, runqueue = RQ}) ->
    {TaskID, RQLeft} = gb_trees:take(Rank, RQ),
    case maps:take(TaskID, Runnable) of
        {Task = #{target_time := TargetTime}, RunnableLeft} ->
            {Task, Queue#task_queue{runnable = RunnableLeft, runqueue = RQLeft}};
        {_DifferentTask, _} ->
            % NOTE
            % It's not the same task we have in the task set. Well just consider it outdated because
            % the queue can hold stale tasks, in contrast to the task set which can hold only single
            % task with some ID that is considered actual.
            {outdated, Queue#task_queue{runqueue = RQLeft}};
        error ->
            % NOTE
            % No such task in the task set. Well just consider it outdated too.
            {outdated, Queue#task_queue{runqueue = RQLeft}}
    end.

get_task_queue_size(#task_queue{runnable = Runnable}) ->
    maps:size(Runnable).

get_active_task_count(#state{active_tasks = ActiveTasks}) ->
    maps:size(ActiveTasks).

get_waiting_task_count(#state{waiting_tasks = WaitingTasks}) ->
    get_task_queue_size(WaitingTasks).

%% There is 62167219200 seconds between Jan 1, 0 and Jan 1, 1970.
-define(EPOCH_DIFF, 62167219200).

unow() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?EPOCH_DIFF.
