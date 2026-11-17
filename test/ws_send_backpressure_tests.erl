-module(ws_send_backpressure_tests).
-include_lib("eunit/include/eunit.hrl").
-export([init/2, handle_in/2, send/2, send/3, abort/1, activate/1, close/1, classify/2]).

init(_, Observer) -> {ok, Observer}.
handle_in(_, State) -> {ok, State}.
send(Observer, Data) -> send(Observer, Data, 5000).
send(Observer, Data, Timeout) ->
    Observer ! {sending, self(), iolist_to_binary(Data)},
    receive {release, Result} -> Result
    after Timeout -> {error, timeout}
    end.
abort(_) -> ok.
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

expired_queued_send_never_reaches_transport_test() ->
    Pid = session(),
    Parent = self(),
    ws_session:send(Pid, {text, <<"first">>}),
    _ = entered(Pid),
    spawn(fun() -> Parent ! {queued_result, ws_session:send_sync(Pid,
        {text, <<"expired">>}, 30)} end),
    receive after 60 -> ok end,
    Pid ! {release, ok},
    receive {queued_result, Result} -> ?assertEqual({error, timeout}, Result)
    after 1000 -> error(no_deadline) end,
    receive {sending, Pid, _} -> error(expired_output_sent)
    after 20 -> ok end.

sync_transport_failure_test() ->
    Pid = session(),
    Parent = self(),
    spawn(fun() -> Parent ! {result, ws_session:send_sync(Pid,
        {text, <<"fail">>}, 1000)} end),
    _ = entered(Pid),
    Pid ! {release, {error, epipe}},
    receive {result, Result} -> ?assertEqual({error, epipe}, Result)
    after 1000 -> error(no_failure) end.

sync_timeout_aborts_blocked_owner_test() ->
    Pid = session(),
    Monitor = erlang:monitor(process, Pid),
    ?assertEqual({error, timeout}, ws_session:send_sync(Pid, {text, <<"stall">>}, 30)),
    _ = entered(Pid),
    receive {'DOWN', Monitor, process, Pid, {shutdown, {transport_error, timeout}}} -> ok
    after 1000 -> error(owner_retained) end,
    ?assertEqual({error, closed}, ws_session:send_sync(Pid, [], 30)).

tcp_send_deadline_releases_driver_output_test() ->
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
        Started = erlang:monotonic_time(millisecond),
        {Accepted, Result} = fill_tcp(Pid, {binary, binary:copy(<<0>>, 8388608)}, 0),
        io:format("nonreading TCP peer accepted frames before timeout=~p~n", [Accepted]),
        ?assertEqual({error, timeout}, Result),
        receive {'DOWN', Monitor, process, Pid, {shutdown, {transport_error, timeout}}} -> ok
        after 1000 -> error(tcp_owner_retained) end,
        receive {'DOWN', SocketMonitor, port, Socket, _} -> ok
        after 1000 -> error(tcp_socket_retained) end,
        %% A port DOWN alone is insufficient: the old driver retained its
        %% output after DOWN. Check the actual driver port as well.
        ?assertEqual(undefined, erlang:port_info(Socket)),
        ?assertNot(is_process_alive(Pid)),
        Elapsed = erlang:monotonic_time(millisecond) - Started,
        io:format("TCP send deadline and owner/socket release: ~p ms~n", [Elapsed]),
        ?assert(Elapsed < 1000)
    after
        exit(Pid, kill),
        exit(Socket, kill),
        gen_tcp:close(Client),
        gen_tcp:close(Listener)
    end.

tcp_close_and_abort_release_output_test() ->
    lists:foreach(fun(Operation) ->
        with_tcp(fun(Socket, _Client) ->
            ok = ws_transport_gen_tcp:send(Socket, binary:copy(<<0>>, 8388608), 100),
            T0 = erlang:monotonic_time(millisecond),
            ok = ws_transport_gen_tcp:Operation(Socket),
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            io:format("TCP ~p with queued output: ~p ms~n", [Operation, Elapsed]),
            ?assert(Elapsed < 2000),
            ?assertEqual(undefined, erlang:port_info(Socket))
        end)
    end, [close, abort]).

tcp_session_abort_while_sending_test_() ->
    {timeout, 8, fun() ->
        with_tcp(fun(Socket, _Client) ->
            {ok, Pid} = ws_session:start(#{transport => ws_transport_gen_tcp,
                handle => Socket, role => server, handler => ?MODULE,
                handler_opts => self(), idle_timeout => infinity}),
            ok = gen_tcp:controlling_process(Socket, Pid),
            Ref = monitor(process, Pid),
            Frame = {binary, binary:copy(<<0>>, 8388608)},
            ok = ws_session:send_sync(Pid, Frame, 100),
            ws_session:send(Pid, Frame),
            T0 = erlang:monotonic_time(millisecond),
            %% This request is queued behind the actual blocked TCP send.
            ok = ws_session:abort(Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 1000 -> error(owner_retained) end,
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            io:format("session abort behind blocked TCP send: ~p ms~n", [Elapsed]),
            ?assert(Elapsed < 6500),
            ?assertEqual(undefined, erlang:port_info(Socket)),
            ?assertNot(is_process_alive(Pid))
        end)
    end}.

tcp_session_close_with_queued_output_test_() ->
    {timeout, 8, fun() ->
        with_tcp(fun(Socket, _Client) ->
            {ok, Pid} = ws_session:start(#{transport => ws_transport_gen_tcp,
                handle => Socket, role => server, handler => ?MODULE,
                handler_opts => self(), idle_timeout => infinity}),
            ok = gen_tcp:controlling_process(Socket, Pid),
            Ref = monitor(process, Pid),
            ok = ws_session:send_sync(Pid, {binary, binary:copy(<<0>>, 8388608)}, 100),
            T0 = erlang:monotonic_time(millisecond),
            ws_session:close(Pid, 1000),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 6500 -> error(owner_retained) end,
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            io:format("session close behind queued TCP output: ~p ms~n", [Elapsed]),
            ?assert(Elapsed < 6500),
            ?assertEqual(undefined, erlang:port_info(Socket))
        end)
    end}.

tcp_normal_output_activation_and_close_test() ->
    with_tcp(fun(Socket, Client) ->
        {ok, Pid} = ws_session:start(#{transport => ws_transport_gen_tcp,
            handle => Socket, role => server, handler => ?MODULE,
            handler_opts => self(), idle_timeout => infinity}),
        ok = gen_tcp:controlling_process(Socket, Pid),
        Ref = monitor(process, Pid),
        Frame = {text, <<"before activation">>},
        ok = ws_session:send_sync(Pid, Frame, 100),
        Expected = iolist_to_binary(ws_frame:encode(Frame, server)),
        ?assertEqual({ok, Expected}, gen_tcp:recv(Client, byte_size(Expected), 1000)),
        ws_session:activate(Pid),
        %% A client-initiated close must receive the server's close echo.
        ok = gen_tcp:send(Client, ws_frame:encode({close, 1000, <<>>}, client)),
        Echo = iolist_to_binary(ws_frame:encode({close, 1000, <<>>}, server)),
        ?assertEqual({ok, Echo}, gen_tcp:recv(Client, byte_size(Echo), 1000)),
        receive {'DOWN', Ref, process, Pid, normal} -> ok
        after 2000 -> error(owner_retained) end,
        ?assertEqual(undefined, erlang:port_info(Socket))
    end).

with_tcp(Fun) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {recbuf, 1024}]),
    {ok, {_, Port}} = inet:sockname(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port,
        [binary, {active, false}, {recbuf, 1024}]),
    {ok, Socket} = gen_tcp:accept(Listener),
    ok = inet:setopts(Socket, [{sndbuf, 1024}]),
    try Fun(Socket, Client)
    after
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
