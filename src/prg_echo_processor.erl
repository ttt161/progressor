-module(prg_echo_processor).

-export([process/3]).

-spec process(_Req, _Opts, _Ctx) -> _.
process({_, _, Process}, _Opts, _Ctx) ->
    case maps:get(current_generation, Process, 0) of
        10 ->
            Result = #{
                state => state()
            },
            {ok, Result};
        _ ->
            Result = #{
                state => state(),
                action => timeout
            },
            {ok, Result}
    end.

%%

state() ->
    #{
        timestamp => erlang:system_time(microsecond),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.
