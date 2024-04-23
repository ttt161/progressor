-module(sd_queue_task).

-type id() :: any().
-type payload() :: any().
% unix timestamp in seconds
-type target_time() :: pos_integer().

-type machine_id() :: binary().
-type task(TaskID, TaskPayload) :: #{
    id := TaskID,
    target_time := target_time(),
    machine_id := machine_id(),
    payload => TaskPayload
}.

-type task() :: task(id(), payload()).

-export_type([id/0]).
-export_type([target_time/0]).
-export_type([task/2]).
-export_type([task/0]).

-export([current_time/0]).

%%

current_time() ->
    unow().

%% There is 62167219200 seconds between Jan 1, 0 and Jan 1, 1970.
-define(EPOCH_DIFF, 62167219200).

unow() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?EPOCH_DIFF.
