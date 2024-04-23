-module(sd_scheduler_worker).

-export([child_spec/3]).
-export([start_link/2]).

-export([start_task/2]).

%% Internal API
-export([do_start_task/3]).
-export([execute/3]).

-callback execute_task(Options :: any(), sd_queue_task:task()) -> ok.

%%
%% API
%%

child_spec(SchedulerID, Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [SchedulerID, Options]},
        restart => permanent,
        type => supervisor
    }.

start_link(SchedulerID, Options) ->
    sd_adhoc_sup:start_link(
        {local, registered_name(SchedulerID)},
        #{strategy => simple_one_for_one},
        [
            #{
                id => tasks,
                start => {?MODULE, do_start_task, [SchedulerID, Options]},
                restart => temporary
            }
        ]
    ).

start_task(SchedulerID, Task) ->
    case supervisor:start_child(registered_name(SchedulerID), [Task]) of
        {ok, Pid} ->
            Monitor = erlang:monitor(process, Pid),
            {ok, Pid, Monitor};
        Error ->
            Error
    end.

do_start_task(SchedulerID, Options, Task) ->
    proc_lib:start_link(?MODULE, execute, [SchedulerID, Options, Task]).

execute(_SchedulerID, #{task_handler := {Mod, Opts}}, Task) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok =
        try
            ok = Mod:execute_task(Opts, Task)
        catch
            Class:Reason:ST ->
                _Exception = {Class, Reason, ST}
                %% Log it
        end.

%% Internlas

% Process registration

registered_name({T, NSID}) ->
    erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ erlang:atom_to_list(T) ++ erlang:binary_to_list(NSID));
registered_name(ID) when is_atom(ID) ->
    erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ erlang:atom_to_list(ID)).
