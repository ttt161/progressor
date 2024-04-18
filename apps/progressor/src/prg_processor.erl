-module(prg_processor).

-include("progressor.hrl").

%% API
-export([call/2]).

-spec call(processor_opts(), {call_type(), map(), process()}) -> process_result().
call(#{client := echo_processor, options := EchoOptions}, Request) ->
    echo_processor:call(EchoOptions, Request).