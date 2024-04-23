-module(pg_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("progressor/include/progressor.hrl").

%% API
-export([is_exists/3]).
-export([search_tasks/2]).
-export([search_waiting_timer_tasks/5]).
-export([save_task/3]).
-export([delete_task/3]).

-export([get_process/3]).
-export([save_process/3]).

-export([save_events/4]).

-type pg_opts() :: #{pool := atom()}.

-define(EPOCH_DIFF, 62167219200).

-spec is_exists(pg_opts(), namespace_id(), id()) -> term().
is_exists(#{pool := Pool}, NsId, Id) ->
    Table = construct_table_name(NsId, "_processes"),
    {ok, Columns, Rows} = epgsql_pool:query(
        Pool,
        "SELECT EXISTS(SELECT 1 FROM " ++ Table ++ " WHERE process_id = $1)",
        [Id]
    ),
    [#{<<"exists">> := Result}] = to_maps(Columns, Rows),
    Result.

-spec search_tasks(pg_opts(), namespace_id()) -> [map()].
search_tasks(#{pool := Pool}, NsId) ->
    Table = construct_table_name(NsId, "_timers"),
    {ok, Columns, Rows} = epgsql_pool:query(Pool, "SELECT * from " ++ Table),
    to_maps(Columns, Rows, fun marshall_task/1).

search_waiting_timer_tasks(#{pool := Pool}, NsId, FromTs, ToTs, Limit) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    TimersTable = construct_table_name(NsId, "_timers"),
    Query = ["SELECT t.* from ", TimersTable, " AS t ",
             "INNER JOIN ", ProcessesTable, " AS p ON p.process_id = t.process_id ",
             "WHERE p.status = $1 AND ",
             "t.timestamp >= (to_timestamp($2) AT TIME ZONE 'UTC') AND ",
             "t.timestamp <= (to_timestamp($3) AT TIME ZONE 'UTC') ",
             "LIMIT $4"],
    {ok, Columns, Rows} = epgsql_pool:query(Pool, Query, ["waiting", FromTs, ToTs, Limit]),
    to_maps(Columns, Rows, fun marshall_task/1).

-spec save_task(pg_opts(), namespace_id(), task()) -> ok.
save_task(#{pool := Pool}, NsId, Task) ->
    Table = construct_table_name(NsId, "_timers"),
    #{
        process_id := ProcessId,
        timestamp := Ts,
        args := Args,
        last_retry_interval := LastRetryInterval,
        attempts_count := AttemptsCount
    } = Task,
    {ok, _} = epgsql_pool:query(
        Pool,
        "INSERT INTO " ++ Table ++ " (process_id, timestamp, args, last_retry_interval, attempts_count)"
        "VALUES ($1, $2, $3, $4, $5)"
        "ON CONFLICT (process_id)"
        "DO UPDATE SET timestamp = $2, args = $3, last_retry_interval = $4, attempts_count = $5",
        [ProcessId, unixtime_to_datetime(Ts), jsx:encode(Args), LastRetryInterval, AttemptsCount]
    ),
    ok.

-spec delete_task(pg_opts(), namespace_id(), id()) -> ok.
delete_task(#{pool := Pool}, NsId, ProcessId) ->
    Table = construct_table_name(NsId, "_timers"),
    {ok, _} = epgsql_pool:query(Pool, "DELETE FROM " ++ Table ++ " WHERE process_id = $1", [ProcessId]),
    ok.

-spec get_process(pg_opts(), namespace_id(), id()) -> process().
get_process(#{pool := Pool}, NsId, ProcessId) ->
    EventsTable = construct_table_name(NsId, "_events"),
    ProcessesTable = construct_table_name(NsId, "_processes"),
    Result = epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, ProcColumns, ProcRows} = epgsql_pool:query(
                Connection,
                "SELECT * FROM " ++ ProcessesTable ++ " WHERE process_id = $1",
                [ProcessId]
            ),
            [Proc] = to_maps(ProcColumns, ProcRows, fun marshall_process/1),
            {ok, EvColumns, EvRows} = epgsql_pool:query(
                Connection,
                "SELECT * FROM " ++ EventsTable ++ " WHERE process_id = $1 ORDER BY event_id DESC",
                [ProcessId]
            ),
            Events = to_maps(EvColumns, EvRows, fun marshall_event/1),
            Proc#{history => Events}
        end
    ),
    Result.

-spec save_process(pg_opts(), namespace_id(), process()) -> ok.
save_process(#{pool := Pool}, NsId, Process) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    #{
        process_id := ProcessId,
        status := Status,
        detail := Detail,
        metadata := Meta
    } = Process,
    {ok, _} = epgsql_pool:query(
        Pool,
        "INSERT INTO " ++ ProcessesTable ++ " (process_id, status, detail, metadata)"
        "VALUES ($1, $2, $3, $4)"
        "ON CONFLICT (process_id)"
        "DO UPDATE SET status = $2, detail = $3, metadata = $4",
        [ProcessId, Status, Detail, jsx:encode(Meta)]
    ),
    ok.

-spec save_events(pg_opts(), namespace_id(), id(), [event()]) -> ok.
save_events(#{pool := Pool}, NsId, ProcessId, Events) ->
    EventsTable = construct_table_name(NsId, "_events"),
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            lists:foreach(fun(Event) ->
                #{
                    event_id := EventId,
                    timestamp := Ts,
                    payload := Payload
                } = Event,
                {ok, _} = epgsql_pool:query(
                    Connection,
                    "INSERT INTO " ++ EventsTable ++ " (process_id, event_id, timestamp, payload)"
                    "VALUES ($1, $2, $3, $4)",
                    [ProcessId, EventId, unixtime_to_datetime(Ts), jsx:encode(Payload)]
                )
            end, Events)
        end
    ),
    ok.


%% Internal functions

construct_table_name(NsId, Postfix) ->
    erlang:atom_to_list(NsId) ++ Postfix.

to_maps(Columns, Rows) ->
    to_maps(Columns, Rows, fun(V) -> V end).

to_maps(Columns, Rows, TransformRowFun) ->
    ColNumbers = erlang:length(Columns),
    Seq = lists:seq(1, ColNumbers),
    lists:map(
        fun(Row) ->
            Data = lists:foldl(
                fun(Pos, Acc) ->
                    #column{name = Field, type = Type} = lists:nth(Pos, Columns),
                    Acc#{Field => convert(Type, erlang:element(Pos, Row))}
                end,
                #{},
                Seq
            ),
            TransformRowFun(Data)
        end,
        Rows
    ).

%% for reference https://github.com/epgsql/epgsql#data-representation
convert(jsonb, null) ->
    #{};
convert(json, null) ->
    #{};
convert(text, null) ->
    <<>>;
convert(_Type, null) ->
    null;
convert(timestamp, Value) ->
    daytime_to_unixtime(Value);
convert(timestamptz, Value) ->
    daytime_to_unixtime(Value);
convert(jsonb, Value) ->
    jsx:decode(Value, [return_maps]);
convert(json, Value) ->
    jsx:decode(Value, [return_maps]);
convert(_Type, Value) ->
    Value.

daytime_to_unixtime({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    daytime_to_unixtime({Date, {Hour, Minute, trunc(Second)}});
daytime_to_unixtime(Daytime) ->
    to_unixtime(calendar:datetime_to_gregorian_seconds(Daytime)).

to_unixtime(Time) when is_integer(Time) ->
    Time - ?EPOCH_DIFF.

unixtime_to_datetime(Timestamp) ->
    calendar:gregorian_seconds_to_datetime(Timestamp + ?EPOCH_DIFF).

%% Marshalling

marshall_task(#{
    <<"process_id">> := ProcessId,
    <<"timestamp">> := Ts,
    <<"args">> := Args,
    <<"last_retry_interval">> := LastRetryInterval,
    <<"attempts_count">> := AttemptsCount
}) ->
    #{
        process_id => ProcessId,
        timestamp => Ts,
        args => Args,
        last_retry_interval => LastRetryInterval,
        attempts_count => AttemptsCount
    }.

marshall_process(#{
    <<"process_id">> := ProcessId,
    <<"status">> := Status,
    <<"detail">> := Detail,
    <<"metadata">> := Meta
}) ->
    #{
        process_id => ProcessId,
        status => Status,
        detail => Detail,
        metadata => Meta
    }.

marshall_event(#{
    <<"process_id">> := ProcessId,
    <<"event_id">> := EventId,
    <<"timestamp">> := Ts,
    <<"payload">> := Payload
}) ->
    #{
        process_id => ProcessId,
        event_id => EventId,
        timestamp => Ts,
        payload => Payload
    }.

%datetime_to_binary(DateTime) ->
%    UnixTime = daytime_to_unixtime(DateTime),
%    format(UnixTime, second).

%format(Value, Unit) when is_integer(Value) andalso is_atom(Unit) ->
%    Str = calendar:system_time_to_rfc3339(Value, [{unit, Unit}, {offset, "Z"}]),
%    erlang:list_to_binary(Str).
