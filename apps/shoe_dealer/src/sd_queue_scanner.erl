-module(sd_queue_scanner).

-type scheduler_id() :: sd_scheduler:id().
-type scan_delay() :: milliseconds().
-type scan_limit() :: non_neg_integer().
% as in A×X + B
-type scan_ahead() :: {_A :: float(), _B :: scan_limit()}.

-type milliseconds() :: non_neg_integer().

%%

-type queue_state() :: any().
-type queue_options() :: any().
-type queue_handler() :: {module(), queue_options()}.

-callback child_spec(queue_options(), atom()) -> supervisor:child_spec() | undefined.
-callback init(queue_options()) -> {ok, queue_state()}.
-callback search_tasks(Options, Limit, State) -> {{Delay, Tasks}, State} when
    Options :: queue_options(),
    Limit :: scan_limit(),
    Tasks :: [sd_queue_task:task()],
    Delay :: scan_delay(),
    State :: queue_state().

-optional_callbacks([child_spec/2]).

%%

-define(DEFAULT_MAX_LIMIT, unlimited).
-define(DEFAULT_SCAN_AHEAD, {1.0, 0}).
-define(DEFAULT_RETRY_SCAN_DELAY, 1000).

-export([child_spec/3]).
-export([start_link/2]).
-export([where_is/1]).

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

%%

child_spec(SchedulerID, Options, ChildID) ->
    Flags = #{strategy => rest_for_one},
    ChildSpecs = lists_compact([
        handler_child_spec(Options, {ChildID, handler}),
        #{
            id => {ChildID, scanner},
            start => {?MODULE, start_link, [SchedulerID, Options]},
            restart => permanent,
            type => worker
        }
    ]),
    #{
        id => ChildID,
        start => {sd_adhoc_sup, start_link, [Flags, ChildSpecs]},
        type => supervisor
    }.

handler_child_spec(#{queue_handler := {Mod, Opts}}, ChildID) ->
    case erlang:function_exported(Mod, child_spec, 2) of
        true ->
            Mod:child_spec(Opts, ChildID);
        false ->
            undefined
    end.

%%

start_link(SchedulerID, Options) ->
    gen_server:start_link({local, registered_name(SchedulerID)}, ?MODULE, {SchedulerID, Options}, []).

where_is(SchedulerID) ->
    erlang:whereis(registered_name(SchedulerID)).

%%

-type queue_handler_state() :: {queue_handler(), queue_state()}.

-record(st, {
    scheduler_id :: scheduler_id(),
    queue_handler :: queue_handler_state(),
    max_limit :: scan_limit(),
    scan_ahead :: scan_ahead(),
    retry_delay :: scan_delay(),
    timer :: reference() | undefined
}).

init({SchedulerID, Options}) ->
    St = #st{
        scheduler_id = SchedulerID,
        queue_handler = init_handler(maps:get(queue_handler, Options)),
        max_limit = maps:get(max_scan_limit, Options, ?DEFAULT_MAX_LIMIT),
        scan_ahead = maps:get(scan_ahead, Options, ?DEFAULT_SCAN_AHEAD),
        retry_delay = maps:get(retry_scan_delay, Options, ?DEFAULT_RETRY_SCAN_DELAY)
    },
    %% stupid wait for scheduler to go online
    {ok, start_timer(erlang:monotonic_time(), 1000, St)}.


handle_cast(Cast, St) ->
    ok = logger:error("unexpected gen_server cast received: ~p, state ~p", [Cast, St]),
    {noreply, St}.

handle_call(Call, From, St) ->
    ok = logger:error("unexpected gen_server call received: ~p, from ~p, state ~p", [Call, From, St]),
    {noreply, St}.

handle_info(scan, St) ->
    {noreply, handle_scan(St)};
handle_info(Info, St) ->
    ok = logger:warning("unexpected gen_server info received: ~p, state ~p", [Info, St]),
    {noreply, St}.

handle_scan(St0 = #st{max_limit = MaxLimit}) ->
    StartedAt = erlang:monotonic_time(),
    %% Try to find out which schedulers are here, getting their statuses
    case inquire_schedulers(St0) of
        Schedulers = [_ | _] ->
            %% Compute total limit given capacity left on each scheduler
            Capacities = [compute_adjusted_capacity(S, St0) || S <- Schedulers],
            Limit = erlang:min(lists:sum(Capacities), MaxLimit),
            {{Delay, Tasks}, St1} = scan_queue(Limit, St0),
            %% Distribute tasks taking into account respective capacities
            ok = disseminate_tasks(Tasks, Schedulers, Capacities, St1),
            start_timer(StartedAt, Delay, St1)
    end.

scan_queue(Limit, St = #st{queue_handler = HandlerState, retry_delay = RetryDelay}) ->
    {Result, HandlerStateNext} =
        try
            run_handler(HandlerState, search_tasks, [Limit])
        catch
            throw:({ErrorType, _Details} = Reason):Stacktrace when
                ErrorType =:= transient orelse
                    ErrorType =:= timeout
            ->
                %% Log it
                _Exception = {throw, Reason, Stacktrace},
                {{RetryDelay, []}, HandlerState}
        end,
    {Result, St#st{queue_handler = HandlerStateNext}}.

disseminate_tasks(Tasks, [_Scheduler = #{pid := Pid}], _Capacities, _St) ->
    %% A single scheduler, just send him all tasks optimizing away meaningless partitioning
    sd_scheduler:distribute_tasks(Pid, Tasks).

inquire_schedulers(#st{scheduler_id = SchedulerID}) ->
    [sd_scheduler:inquire(SchedulerID)].

compute_adjusted_capacity(#{waiting_tasks := W, capacity := C}, #st{scan_ahead = {A, B}}) ->
    erlang:max(erlang:round(A * erlang:max(C - W, 0)) + B, 0).

%%

start_timer(RefTime, Delay, St) ->
    FireTime = erlang:convert_time_unit(RefTime, native, millisecond) + Delay,
    St#st{timer = erlang:send_after(FireTime, self(), scan, [{abs, true}])}.

-spec init_handler(queue_handler()) -> queue_handler_state().
init_handler({Mod, Opts} = Handler) ->
    {ok, InitialState} = Mod:init(Opts),
    {Handler, InitialState}.

-spec run_handler(queue_handler_state(), _Function :: atom(), _Args :: list()) ->
    {_Result, queue_handler_state()}.
run_handler({{Mod, Opts} = Handler, State}, Function, Args) ->
    {Result, NextState} = erlang:apply(Mod, Function, [Opts | Args] ++ [State]),
    {Result, {Handler, NextState}}.

registered_name(ID) ->
    erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ erlang:atom_to_list(ID)).

lists_compact(List) ->
    lists:filter(
        fun
            (undefined) -> false;
            (_) -> true
        end,
        List
    ).
