-module(ws_send_backpressure_tests).
-include_lib("eunit/include/eunit.hrl").
-export([init/2, handle_in/2, send/2, activate/1, close/1, classify/2]).

init(_, Observer) -> {ok, Observer}.
handle_in(_, State) -> {ok, State}.
send(Observer, Data) ->
    Observer ! {sending, self(), iolist_to_binary(Data)},
    receive {release, Result} -> Result end.
activate(_) -> ok.
close(_) -> ok.
classify(_, _) -> ignore.

session() ->
    {ok, Pid} = ws_session:start(#{transport => ?MODULE, handle => self(),
        role => server, handler => ?MODULE, handler_opts => self(),
        idle_timeout => infinity}),
    Pid.

entered(Pid) ->
    receive {sending, Pid, Bytes} -> Bytes
    after 1000 -> error(transport_not_entered)
    end.

sync_ack_waits_for_transport_in_both_states_test() ->
    lists:foreach(fun(Activate) ->
        Pid = session(),
        Parent = self(),
        try
            case Activate of true -> ws_session:activate(Pid); false -> ok end,
            spawn(fun() -> Parent ! {result, ws_session:send_sync(Pid,
                {text, <<"held">>}, 1000)} end),
            ?assertEqual(iolist_to_binary(ws_frame:encode({text, <<"held">>}, server)),
                         entered(Pid)),
            receive {result, _} -> error(early_ack) after 20 -> ok end,
            ?assertEqual({message_queue_len, 0}, process_info(Pid, message_queue_len)),
            Pid ! {release, ok},
            receive {result, Result} -> ?assertEqual(ok, Result)
            after 1000 -> error(no_ack) end
        after exit(Pid, kill)
        end
    end, [false, true]).

sync_rejects_closing_test() ->
    Pid = session(),
    try
        ws_session:close(Pid, 1000),
        _ = entered(Pid),
        Pid ! {release, ok},
        ?assertEqual({error, closing}, ws_session:send_sync(Pid, {text, <<"no">>}, 100))
    after exit(Pid, kill)
    end.

sync_transport_failure_test() ->
    Pid = session(),
    Parent = self(),
    spawn(fun() -> Parent ! {result, ws_session:send_sync(Pid,
        {text, <<"fail">>}, 1000)} end),
    _ = entered(Pid),
    Pid ! {release, {error, epipe}},
    receive {result, Result} -> ?assertEqual({error, epipe}, Result)
    after 1000 -> error(no_failure) end.

sync_timeout_kills_blocked_owner_test() ->
    Pid = session(),
    Monitor = erlang:monitor(process, Pid),
    ?assertEqual({error, timeout}, ws_session:send_sync(Pid, {text, <<"stall">>}, 30)),
    _ = entered(Pid),
    receive {'DOWN', Monitor, process, Pid, killed} -> ok
    after 1000 -> error(owner_retained) end,
    ?assertEqual({error, closed}, ws_session:send_sync(Pid, [], 30)).

tcp_timeout_retains_driver_port_reproduction_test() ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {recbuf, 1024}]),
    {ok, {_, Port}} = inet:sockname(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port,
        [binary, {active, false}, {recbuf, 1024}]),
    {ok, Socket} = gen_tcp:accept(Listener),
    ok = inet:setopts(Socket, [{sndbuf, 1024}]),
    {ok, Pid} = ws_session:start(#{transport => ws_transport_gen_tcp,
        handle => Socket, role => server, handler => ?MODULE,
        handler_opts => self(), idle_timeout => infinity}),
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:controlling_process(Socket, Pid),
    SocketMonitor = erlang:monitor(port, Socket),
    try
        ws_session:activate(Pid),
        {Accepted, Result} = fill_tcp(Pid, {binary, binary:copy(<<0>>, 8388608)}, 0),
        io:format("nonreading TCP peer accepted frames before timeout=~p~n", [Accepted]),
        ?assertEqual({error, timeout}, Result),
        receive {'DOWN', Monitor, process, Pid, killed} -> ok
        after 1000 -> error(tcp_owner_retained) end,
        receive {'DOWN', SocketMonitor, port, Socket, _} -> ok
        after 1000 -> error(tcp_socket_retained) end,
        receive after 250 -> ok end,
        Retained = erlang:port_info(Socket) =/= undefined,
        io:format("TCP driver port retained after owner death=~p~n", [Retained]),
        ?assert(Retained)
    after
        exit(Pid, kill),
        exit(Socket, kill),
        gen_tcp:close(Client),
        gen_tcp:close(Listener)
    end.

fill_tcp(_Pid, _Frame, 8) -> {8, never_blocked};
fill_tcp(Pid, Frame, N) ->
    case ws_session:send_sync(Pid, Frame, 100) of
        ok -> fill_tcp(Pid, Frame, N + 1);
        Result -> {N, Result}
    end.

cast_queue_growth_reproduction_test() ->
    Pid = session(),
    try
        ws_session:send(Pid, {binary, <<0>>}),
        _ = entered(Pid),
        lists:foreach(fun(N) ->
            ws_session:send(Pid, {binary, <<N:32, 0:8192>>})
        end, lists:seq(1, 200)),
        {message_queue_len, Queued} = process_info(Pid, message_queue_len),
        io:format("blocked transport queued casts=~p~n", [Queued]),
        ?assertEqual(200, Queued)
    after exit(Pid, kill)
    end.
