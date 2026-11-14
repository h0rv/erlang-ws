%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc Reference `ws_transport' implementation over plain `gen_tcp'.
%%
%% Used by the CT test suite as the embedder stand-in. Translates
%% `tcp' / `tcp_closed' / `tcp_error' messages into the canonical
%% `ws_*' messages the session consumes.
-module(ws_transport_gen_tcp).
-behaviour(ws_transport).

-export([send/2, send/3, abort/1, activate/1, close/1, controlling_process/2, peername/1]).
-export([classify/2, recv/2]).

%% A timed-out send may have written only part of a WebSocket frame.
%% Do not use send_timeout_close: the inet driver can retain its output
%% after that close. Abort explicitly while the socket is still open.
send(Sock, Data) ->
    send(Sock, Data, 5000).

send(Sock, Data, Timeout) ->
    case inet:setopts(Sock, [{send_timeout, Timeout}, {send_timeout_close, false}]) of
        ok ->
            case gen_tcp:send(Sock, Data) of
                ok -> ok;
                {error, _} = Error -> abort(Sock), Error
            end;
        {error, _} = Error -> abort(Sock), Error
    end.

activate(Sock) ->
    inet:setopts(Sock, [{active, once}]).

%% WebSocket shutdown is terminal: if the driver still has output, drop
%% it BEFORE closing. Even positive linger followed by exit(Port, kill)
%% cannot reclaim an inet port once it has entered its closing state.
%% With no driver output, preserve ordinary TCP FIN and kernel draining.
%% There is no producer racing this check: the session owns all writes.
close(Sock) ->
    case inet:getstat(Sock, [send_pend]) of
        {ok, [{send_pend, 0}]} ->
            _ = inet:setopts(Sock, [{linger, {false, 0}}]),
            gen_tcp:close(Sock);
        _ -> abort(Sock)
    end.

abort(Sock) ->
    _ = inet:setopts(Sock, [{linger, {true, 0}}]),
    gen_tcp:close(Sock).

controlling_process(Sock, Pid) ->
    gen_tcp:controlling_process(Sock, Pid).

peername(Sock) ->
    inet:peername(Sock).

classify({tcp, Sock, Data}, Sock) ->        {ws_data, Sock, Data};
classify({tcp_closed, Sock}, Sock) ->       {ws_closed, Sock};
classify({tcp_error, Sock, Reason}, Sock) -> {ws_error, Sock, Reason};
classify(_Other, _Sock) ->                   ignore.

recv(Sock, Timeout) ->
    gen_tcp:recv(Sock, 0, Timeout).
