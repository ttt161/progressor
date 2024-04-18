
%-export_type([process/0]).
%-export_type([namespace_opts/0]).
%-export_type([namespace_id/0]).
%-export_type([storage_opts/0]).
%-export_type([task/0]).
%-export_type([task_type/0]).
%-export_type([process_result/0]).
%-export_type([call_type/0]).

-type process() :: #{
    process_id := id(),
    status := process_status(),
    history := [event()],
    detail => binary(),
    metadata => #{binary() => binary()}
}.

-type process_status() :: binary().
%    'running' | 'waiting' | 'error' | 'finished'.

-type namespace_opts() :: #{
    storage := storage_opts(),
    processor := processor_opts(),
    retry_policy => retry_policy(),
    non_retryable_errors => [term()],
    worker_pool_size => pos_integer(),
    process_step_timeout => pos_integer()
}.

-type task_type() :: {init | call | repair, pid(), reference()} | signal.
-type call_type() :: init | call | repair | signal.

-type task() :: #{
    process_id := id(),
    timestamp := timestamp_ms(),
    args := map(),
    %% retry_policy => retry_policy(), TODO
    last_retry_interval => non_neg_integer(),
    attempts_count => non_neg_integer()
}.

-type id() :: binary().

-type namespace_id() :: atom().

-type processor_opts() :: #{
    client := module(),
    options => term()
}.

-type storage_opts() :: #{
    client := module(),
    options => term()
}.

-type retry_policy() :: #{
    initial_timeout => timeout_sec(),
    backoff_coefficient => float(),
    max_timeout => timeout_sec(),
    max_attempts => pos_integer()
}.

-type event() :: #{
    event_id := term(),
    timestamp := timestamp_ms(),
    payload := map()
}.

-type process_result() ::
    {ok, #{
        events := [event()],
        action => action(),
        response => term(),
        metadata => map()
    }}
    | {error, binary()}.

-type action() :: #{set_timer := pos_integer()} | unset_timer.

-type timestamp_ms() :: non_neg_integer().
-type timeout_sec() :: non_neg_integer().

-define(DEFAULT_STEP_TIMEOUT_SEC, 60).

-define(DEFAULT_RETRY_POLICY, #{
    initial_timeout => 5, %% second
    backoff_coefficient => 1.0,
    max_attempts => 3
}).

-define(DEFAULT_WORKER_POOL_SIZE, 10).
