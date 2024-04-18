-module(echo_processor).

-include("progressor.hrl").

%% API
-export([call/2]).

-spec call(term(), {call_type(), map(), process()}) -> process_result().
call(_EchoOpts, {Type, Args, #{history := Events} = Process}) ->
    io:format(user, "PROCESSOR CALL. Type: ~p / Args: ~p / Process: ~p~n", [Type, Args, Process]),
    EventId = last_event_id(Events) + 1,
    case EventId < 3 of
        true ->
            Result = #{
                events => [
                    #{
                        event_id => EventId,
                        timestamp =>  erlang:system_time(second),
                        payload => #{event_id => EventId}
                    }
                ],
                action => #{set_timer => erlang:system_time(second) + 10},
                metadata => #{<<"this_event">> => EventId}
            },
            {ok, Result};
        false ->
            Result = #{
                events => [
                    #{
                        event_id => EventId,
                        timestamp =>  erlang:system_time(second),
                        payload => #{event_id => EventId}
                    }
                ]
            },
            {ok, Result}
    end;
call(_EchoOpts, {_Type, #{result := error}, _Process}) ->
    {error, <<"processor_error">>}.

last_event_id([]) ->
    0;
last_event_id([#{event_id := Id} | _T]) ->
    Id.
