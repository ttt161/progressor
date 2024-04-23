-module(progressor).

-include("progressor.hrl").

%% API
-export([start/1]).
-export([call/1]).
-export([repair/1]).
-export([notify/1]).
-export([get/1]).
%-export([remove/1]).

-export([reply/2]).

-type opts() :: #{
    ns := namespace_id(),
    id := id(),
    args => map(),
    ns_opts => namespace_opts(),
    type => task_type(),
    task => task()
}.

%% see receive blocks bellow in this module
-spec reply(pid(), term()) -> term().
reply(Pid, Msg) ->
    Pid ! Msg.

%% API
-spec start(opts()) -> {ok, _Result} | {error, _Reason}.
start(Opts) ->
    do_init(Opts#{type => init}).

-spec call(opts()) -> {ok, _Result} | {error, _Reason}.
call(Opts) ->
    do_call(Opts#{type => call}).

-spec repair(opts()) -> {ok, _Result} | {error, _Reason}.
repair(Opts) ->
    do_repair(Opts#{type => repair}).

-spec notify(opts()) -> {ok, _Result} | {error, _Reason}.
notify(Opts) ->
    do_notify(Opts#{type => signal}).

-spec get(opts()) -> {ok, _Result} | {error, _Reason}.
get(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun do_get/1
    ], Opts).


%% Internal functions

do_init(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun is_not_exists/1,
        fun process_call/1
    ], Opts).

do_call(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun is_exists/1,
        fun process_call/1
    ], Opts).

do_repair(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun is_exists/1,
        %% fun is_broken/1, TODO
        fun process_call/1
    ], Opts).

do_notify(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun is_exists/1,
        fun save_task/1,
        fun process_signal/1
    ], Opts).

add_ns_opts(#{ns := NsId} = Opts) ->
    NSs = application:get_env(progressor, namespaces, []),
    case lists:keyfind(NsId, 1, NSs) of
        false ->
            {error, <<"namespace not found">>};
        {_Id, NsOpts} ->
            Opts#{ns_opts => maps:from_list(NsOpts)}
    end.

is_not_exists(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId} = Opts) ->
    case prg_storage:is_exists(maps:from_list(StorageOpts), NsId, Id) of
        true ->
            {error, <<"process already exists">>};
        false ->
            Opts
    end.

is_exists(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId} = Opts) ->
    case prg_storage:is_exists(maps:from_list(StorageOpts), NsId, Id) of
        true ->
            Opts;
        false ->
            {error, <<"process not_found">>}
    end.

save_task(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId, args := Args} = Opts) ->
    Task = make_task(#{process_id => Id, args => Args}),
    ok = prg_storage:save_task(StorageOpts, NsId, Task),
    Opts#{task => Task}.

do_get(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId}) ->
    prg_storage:get_process(maps:from_list(StorageOpts), NsId, Id).

process_call(#{ns := NsId, ns_opts := NsOpts, id := Id, args := Args, type := Type0}) ->
    TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
    Timeout = TimeoutSec * 1000,
    Task = make_task(#{process_id => Id, args => Args}),
    Ref = make_ref(),
    Type = make_type(Type0, Ref),
    ok = sd_scheduler:send_task(prg_utils:registered_name(NsId, "_scheduler"), #{
        id => Ref,
        target_time => sd_queue_task:current_time(),
        machine_id => Id,
        %% Пардон за грязь
        payload => {Type, Task}
    }),
    %% see fun reply/2
    receive
        {Ref, Result} -> Result
    after
        Timeout ->
            {error, <<"timeout">>}
    end.

process_signal(#{ns := NsId, type := Type, task := Task, id := Id}) ->
    %% Для уведомлений должна быть другая очередь с шедулингом и
    %% другие правила выборки тасок. Для этого наверное нужно бы иметь
    %% стоблец с типом в таблице хранящей таски.
    ok = sd_scheduler:send_task(prg_utils:registered_name(NsId, "_scheduler"), #{
        id => make_ref(),
        target_time => sd_queue_task:current_time(),
        machine_id => Id,
        payload => {Type, Task}
    }).

make_type(init, Ref) ->
    {init, self(), Ref};
make_type(call, Ref) ->
    {call, self(), Ref};
make_type(repair, Ref) ->
    {repair, self(), Ref};
make_type(signal, _Ref) ->
    signal.

make_task(TaskData) ->
    Defaults = #{
        timestamp => erlang:system_time(second),
        last_retry_interval => 0,
        attempts_count => 0
    },
    maps:merge(Defaults, TaskData).
