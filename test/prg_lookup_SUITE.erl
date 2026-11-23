-module(prg_lookup_SUITE).

-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    lookups/1,
    updates/1,
    invalid_intents/1,
    constraints/1,
    init_without_snapshot/1,
    arguments/1,
    cache/1,
    damaged_rows/1,
    metrics/1,
    unicode_identifiers/1
]).

-define(NS, 'default/default').

all() ->
    [
        lookups,
        updates,
        invalid_intents,
        constraints,
        init_without_snapshot,
        arguments,
        cache,
        damaged_rows,
        metrics,
        unicode_identifiers
    ].

init_per_suite(Config) ->
    _ = prg_ct_hook:start_applications(),
    ok = meck:new(prg_ct_processor, [no_link]),
    ok = meck:expect(prg_ct_processor, process, fun({_, Args, _}, _, _) ->
        {ok, binary_to_term(Args)}
    end),
    Config.

end_per_suite(_) ->
    ok = meck:unload(prg_ct_processor),
    prg_ct_hook:stop_applications().

lookups(_) ->
    Fields = #{subnamespace => <<"Owner">>, sub_id => <<"same">>, aliases => [<<"shared">>, <<"shared">>, <<"%_">>]},
    A = create(?NS, Fields),
    B = create(?NS, Fields),
    _ = create(?NS, Fields#{subnamespace => <<"owner">>, aliases => []}),
    {ok, ok} = update(?NS, A, #{metadata => #{<<"process">> => 2}}),
    {ok, Matches} = progressor:get_by_sub_id(?NS, <<"Owner">>, <<"same">>),
    ?assertEqual(lists:sort([A, B]), ids(Matches)),
    {ok, AliasMatches} = progressor:get_by_alias(?NS, <<"shared">>),
    ?assertEqual(ids(Matches), ids(AliasMatches)),
    [Process] = [P || #{process_id := Id} = P <- Matches, Id =:= A],
    ?assertMatch(
        #{
            current_generation := 2,
            metadata := #{<<"process">> := 2},
            state := #{generation := 2, metadata := #{<<"snapshot">> := true}},
            aliases := [<<"%_">>, <<"shared">>]
        },
        Process
    ),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"%">>)),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"SHARED">>)),
    ?assertMatch({ok, [_, _]}, progressor:get_by_alias(?NS, <<"%_">>)),
    ?assertEqual({ok, []}, progressor:get_by_sub_id(?NS, <<"Owner">>, <<"missing">>)),
    ?assertEqual({ok, []}, progressor:get_by_alias('cached/namespace', <<"shared">>)).

updates(_) ->
    Id = create(?NS, #{subnamespace => <<"updates">>, sub_id => <<"id">>, aliases => [<<"old">>]}),
    {ok, ok} = update(?NS, Id, #{}),
    ?assertMatch({ok, [#{sub_id := <<"id">>}]}, progressor:get_by_alias(?NS, <<"old">>)),
    {ok, ok} = update(?NS, Id, #{sub_id => undefined, aliases => [<<"new">>, <<"new">>]}),
    {ok, [P]} = progressor:get_by_alias(?NS, <<"new">>),
    ?assertNot(maps:is_key(sub_id, P)),
    ?assertEqual(<<"updates">>, maps:get(subnamespace, P)),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"old">>)),
    {ok, ok} = update(?NS, Id, #{subnamespace => undefined, aliases => []}),
    {ok, Cleared} = get(?NS, Id),
    ?assertNot(maps:is_key(subnamespace, Cleared)),
    ?assertEqual([], maps:get(aliases, Cleared)),
    {ok, ok} = update(?NS, Id, #{subnamespace => <<"again">>, sub_id => <<"id">>}),
    {ok, ok} = update(?NS, Id, #{subnamespace => undefined, sub_id => undefined}),
    ?assertEqual({ok, []}, progressor:get_by_sub_id(?NS, <<"again">>, <<"id">>)).

invalid_intents(_) ->
    ct:timetrap({seconds, 20}),
    Invalid =
        [
            #{Field => Value}
         || Field <- [subnamespace, sub_id, aliases],
            Bad <- [<<0>>, <<"a", 0, "b">>, <<255>>, <<16#C3>>],
            Value <- [
                case Field of
                    aliases -> [Bad];
                    _ -> Bad
                end
            ]
        ] ++
            [
                #{subnamespace => undefined},
                #{subnamespace => <<>>},
                #{sub_id => <<>>},
                #{sub_id => null},
                #{subnamespace => 1},
                #{aliases => undefined},
                #{aliases => <<"x">>},
                #{aliases => [null]},
                #{aliases => [<<>>]},
                #{aliases => [[1]]}
            ],
    lists:foreach(
        fun(Fields) ->
            Id = create(?NS, #{subnamespace => <<"valid">>, sub_id => <<"id">>}),
            ?assertEqual(
                {error, {exception, error, {invalid_processor_fields, invalid_lookup_fields}}},
                update(?NS, Id, Fields)
            ),
            {ok, P} = get(?NS, Id),
            ?assertMatch(
                #{
                    status := <<"corrupted">>,
                    subnamespace := <<"valid">>,
                    sub_id := <<"id">>,
                    current_generation := 1
                },
                P
            )
        end,
        Invalid
    ),
    Id = id(),
    ?assertMatch(
        {error, {exception, error, {invalid_processor_fields, _}}},
        progressor:init(#{ns => ?NS, id => Id, args => term_to_binary(intent(#{sub_id => <<"orphan">>}))})
    ).

constraints(_) ->
    Id = create(?NS, #{}),
    #{processes := Table} = prg_pg_utils:tables(?NS),
    lists:foreach(
        fun(Set) ->
            ?assertMatch(
                {error, _},
                epg_pool:query(
                    default_pool,
                    "UPDATE " ++ Table ++ " SET " ++ Set ++ " WHERE process_id = $1",
                    [Id]
                )
            )
        end,
        [
            "subnamespace = ''",
            "sub_id = ''",
            "sub_id = 'orphan'",
            "aliases = NULL",
            "aliases = ARRAY['']::text[]",
            "aliases = ARRAY[NULL]::text[]"
        ]
    ).

init_without_snapshot(_) ->
    Id = id(),
    #{processes := Table} = prg_pg_utils:tables(?NS),
    {ok, 1} = epg_pool:query(
        default_pool,
        "INSERT INTO " ++ Table ++
            " (process_id, status, previous_status, status_changed_at, subnamespace, sub_id, aliases)"
            " VALUES ($1, 'init', 'init', now(), 'pending', 'id', ARRAY['pending'])",
        [Id]
    ),
    {ok, [P]} = progressor:get_by_alias(?NS, <<"pending">>),
    ?assertMatch(#{process_id := Id, status := <<"init">>}, P),
    ?assertNot(maps:is_key(state, P)),
    ?assertEqual({ok, [P]}, progressor:get_by_sub_id(?NS, <<"pending">>, <<"id">>)).

arguments(_) ->
    lists:foreach(
        fun(Value) ->
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_alias(?NS, Value)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(?NS, Value, <<"id">>)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(?NS, <<"ns">>, Value))
        end,
        [<<>>, undefined, null, "text", 1, <<0>>, <<"a", 0, "b">>, <<255>>, <<16#C3>>]
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_alias(Ns, <<"a">>)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(Ns, <<"ns">>, <<"id">>))
        end,
        [<<"default/default">>, "default/default", 1, #{}, []]
    ),
    ?assertEqual({error, <<"namespace not found">>}, progressor:get_by_alias(unknown_namespace, <<"a">>)),
    ?assertEqual(
        {error, <<"namespace not found">>},
        progressor:get_by_sub_id(unknown_namespace, <<"ns">>, <<"id">>)
    ).

cache(_) ->
    Ns = 'cached/namespace',
    Aliases = [
        <<"cached">>,
        <<"comma,value">>,
        <<"quote\"value">>,
        <<"back\\slash">>,
        <<"NULL">>,
        <<"NULLfoo">>,
        <<"{braces}">>,
        <<" space ">>
    ],
    Id = create(Ns, #{subnamespace => <<"cache">>, sub_id => <<"id">>, aliases => Aliases}),
    P = assert_cached(Ns, Id, 1, Aliases),
    ?assertEqual(<<"cache">>, maps:get(subnamespace, P)),
    {ok, ok} = update(Ns, Id, #{subnamespace => undefined, sub_id => undefined, aliases => []}),
    Cleared = await_cache(Ns, Id, 2, 100),
    ?assertNot(maps:is_key(subnamespace, Cleared)),
    ?assertNot(maps:is_key(sub_id, Cleared)),
    ?assertNot(maps:is_key(aliases, Cleared)),
    assert_cached(Ns, Id, 2, []),
    {ok, ok} = update(Ns, Id, #{aliases => [<<"NULLfoo">>]}),
    assert_cached(Ns, Id, 3, [<<"NULLfoo">>]),
    {ok, ok} = update(Ns, Id, #{}),
    assert_cached(Ns, Id, 4, [<<"NULLfoo">>]).

assert_cached(Ns, Id, Generation, Aliases) ->
    P = await_cache(Ns, Id, Generation, 100),
    ?assertNot(maps:is_key(aliases, P)),
    [{_, Raw}] = ets:lookup('cached/namespace_processes', Id),
    ?assertNot(maps:is_key(<<"aliases">>, Raw)),
    Labels = [atom_to_binary(Ns)],
    Hits = prometheus_counter:value(progressor_cache_hit_counter, Labels),
    ?assertEqual({ok, P}, progressor:get(#{ns => Ns, id => Id})),
    ?assertEqual(Hits + 1, prometheus_counter:value(progressor_cache_hit_counter, Labels)),
    {ok, SQL} = get(Ns, Id),
    ?assertEqual(P, maps:remove(aliases, SQL)),
    ?assertEqual(lists:sort(Aliases), maps:get(aliases, SQL)),
    lists:foreach(
        fun(Alias) ->
            {ok, Matches} = progressor:get_by_alias(Ns, Alias),
            ?assert(lists:member(SQL, Matches))
        end,
        Aliases
    ),
    P.

await_cache(_, _, _, 0) ->
    ct:fail(cache_not_updated);
await_cache(Ns, Id, Generation, Attempts) ->
    case prg_pg_cache:get(Ns, Id, latest) of
        {ok, #{current_generation := Generation} = P} ->
            P;
        _ ->
            timer:sleep(20),
            await_cache(Ns, Id, Generation, Attempts - 1)
    end.

damaged_rows(_) ->
    Fields = #{subnamespace => <<"pending">>, sub_id => <<"id">>, aliases => [<<"pending">>]},
    Good = create(?NS, Fields),
    Bad = create(?NS, Fields),
    #{generations := Table} = prg_pg_utils:tables(?NS),
    {ok, 1} = epg_pool:query(default_pool, "DELETE FROM " ++ Table ++ " WHERE process_id = $1", [Bad]),
    Metric = progressor_lookup_skipped_damaged_records_counter,
    Labels = [atom_to_binary(?NS)],
    prometheus_counter:inc(Metric, Labels, 0),
    Before = prometheus_counter:value(Metric, Labels),
    {ok, Matches} = progressor:get_by_alias(?NS, <<"pending">>),
    ?assert(lists:member(Good, ids(Matches))),
    ?assertNot(lists:member(Bad, ids(Matches))),
    ?assertMatch([#{status := <<"init">>}], [P || P <- Matches, not maps:is_key(current_generation, P)]),
    {ok, SubMatches} = progressor:get_by_sub_id(?NS, <<"pending">>, <<"id">>),
    ?assertEqual(lists:sort(Matches), lists:sort(SubMatches)),
    ?assertEqual(Before + 2, prometheus_counter:value(Metric, Labels)),
    #{processes := Processes} = prg_pg_utils:tables(?NS),
    {ok, 1} = epg_pool:query(
        default_pool,
        "UPDATE " ++ Processes ++ " SET aliases = ARRAY['only-damaged'] WHERE process_id = $1",
        [Bad]
    ),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"only-damaged">>)).

metrics(_) ->
    lists:foreach(
        fun({Metric, Fun}) ->
            Labels = [atom_to_binary(?NS)],
            {Before, _} = prometheus_histogram:value(Metric, Labels),
            ?assertEqual({ok, []}, Fun()),
            {After, _} = prometheus_histogram:value(Metric, Labels),
            ?assertEqual(lists:sum(Before) + 1, lists:sum(After))
        end,
        [
            {progressor_get_by_alias_duration_ms, fun() -> progressor:get_by_alias(?NS, <<"metric-missing">>) end},
            {progressor_get_by_sub_id_duration_ms, fun() ->
                progressor:get_by_sub_id(?NS, <<"metric-missing">>, <<"id">>)
            end}
        ]
    ).

unicode_identifiers(_) ->
    Value = unicode:characters_to_binary([16#4E16, 16#754C, 16#1F600]),
    Id = create(?NS, #{subnamespace => Value, sub_id => Value, aliases => [Value]}),
    {ok, [P]} = progressor:get_by_alias(?NS, Value),
    ?assertEqual(Id, maps:get(process_id, P)),
    ?assertEqual([Value], maps:get(aliases, P)),
    ?assertEqual({ok, [P]}, progressor:get_by_sub_id(?NS, Value, Value)).

create(Ns, Fields) ->
    Id = id(),
    {ok, ok} = progressor:init(#{ns => Ns, id => Id, args => term_to_binary(intent(Fields))}),
    Id.

update(Ns, Id, Fields) ->
    progressor:call(#{ns => Ns, id => Id, args => term_to_binary(intent(Fields))}).

get(Ns, Id) -> progressor:get(#{ns => Ns, id => Id, options => #{use_cache => false}}).

intent(Fields) ->
    Fields#{
        state => #{
            timestamp => erlang:system_time(second) * 1000000,
            payload => <<"snapshot">>,
            metadata => #{<<"snapshot">> => true}
        }
    }.

id() -> binary:encode_hex(crypto:strong_rand_bytes(16)).

ids(Processes) -> lists:sort([Id || #{process_id := Id} <- Processes]).
