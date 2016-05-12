%%
%% Copyright 2016 Joaquim Rocha <jrocha@gmailbox.org>
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(mdb_hlc).

-define(FRACTIONS_OF_SECOND, 10000).
-record(timestamp, {l, c}).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([timestamp/0, timestamp/1]).
-export([update/1]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

timestamp(Seconds) ->
	Timestamp = #timestamp{l=Logical} = get_timestamp(),
	MS = Seconds * ?FRACTIONS_OF_SECOND,
	encode(Timestamp#timestamp{l = Logical - MS}).

timestamp() ->
	Timestamp = get_timestamp(),
	encode(Timestamp).

update(ExternalTime) ->
	ExternalTS = decode(ExternalTime),
	Timestamp = gen_server:call(?MODULE, {update, ExternalTS}),
	encode(Timestamp).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {last}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{last = current_timestamp()}}.

%% handle_call/3
handle_call({get_timestamp}, _From, State=#state{last=LastTS}) ->
	Now = wall_clock(),
	Logical = max(Now, LastTS#timestamp.l),
	Counter = if 
		Logical =:= LastTS#timestamp.l -> LastTS#timestamp.c + 1;
		true -> 0
	end,
	Timestamp = hlc_timestamp(Logical, Counter),
	{reply, Timestamp, State#state{last=Timestamp}};
handle_call({update, ExternalTS}, _From, State=#state{last=LastTS}) ->
	Now = wall_clock(),
	Logical = max(Now, LastTS#timestamp.l, ExternalTS#timestamp.l),
	Counter = if 
		Logical =:= LastTS#timestamp.l, LastTS#timestamp.l =:= ExternalTS#timestamp.l -> max(LastTS#timestamp.c, ExternalTS#timestamp.c) + 1;
		Logical =:= LastTS#timestamp.l -> LastTS#timestamp.c + 1;
		Logical =:= ExternalTS#timestamp.l -> ExternalTS#timestamp.c + 1;
		true -> 0
	end,
	Timestamp = hlc_timestamp(Logical, Counter),
	{reply, Timestamp, State#state{last=Timestamp}};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

%% handle_cast/2
handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(_Reason, _State) ->
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_timestamp() ->
	gen_server:call(?MODULE, {get_timestamp}).

wall_clock() ->
	TS = {_,_, Micro} = os:timestamp(),
	Utc = calendar:now_to_universal_time(TS),
	Seconds = calendar:datetime_to_gregorian_seconds(Utc),
	Fraction = Micro div 100,
	((Seconds - 62167219200) * ?FRACTIONS_OF_SECOND) + Fraction.

hlc_timestamp(Logical, Counter) -> 
	#timestamp{l=Logical, c=Counter}.

current_timestamp() -> 
	hlc_timestamp(wall_clock(), 0).

encode(#timestamp{l=Logical, c=Counter}) ->
	<<Time:64>> = <<Logical:48, Counter:16>>,
	Time.

decode(Time) ->
	<<Logical:48, Counter:16>> = <<Time:64>>,
	hlc_timestamp(Logical, Counter).

max(A, B, C) -> max(max(A, B), max(B, C)).
