%%
%% Copyright 2016-17 Joaquim Rocha <jrocha@gmailbox.org>
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

-module(mdb_event).

-include("mdb.hrl").
-include("mdb_event.hrl").

-behaviour(gen_server).

%% ====================================================================
%% Constants
%% ====================================================================
-define(SERVER, {local, ?MODULE}).

%% ====================================================================
%% Behavioural functions definition
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([notify/2]).
-export([subscribe/1, unsubscribe/1]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

notify(BI=#bucket{name=Bucket}, Record) ->
	%TODO testes
	gen_server:call(?MODULE, {notify, BI, Record}),
	ok. 
%	case generate_events(BI) of
%		true -> 
%			Event = create_event(Bucket, Record),
%			eb_feed:publish(?MDB_NOTIFICATION_FEED, Event);
%		false -> ok 
%	end.

subscribe(BI=#bucket{name=Bucket}) ->
	case generate_events(BI) of
		true -> eb_filter_by_ref:start_filter(?MDB_NOTIFICATION_FEED, Bucket);
		false -> {error, bucket_do_not_generates_events}
	end.

unsubscribe(#bucket{name=Bucket}) ->
	eb_filter_by_ref:stop_filter(?MDB_NOTIFICATION_FEED, Bucket).


%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{}}.

%% handle_call/3
handle_call(Request, From, State) ->
	error_logger:error_msg("Request: ~w || From: ~w || State: ~w~n", [Request, From, State]),
%5>
%=ERROR REPORT==== 30-May-2017::13:01:12 ===
%Request: {trata,goncalo} || From: {<0.55.0>,#Ref<0.0.3.83>} || State: {state}
%5>    
	Reply = ok,
	{reply, Reply, State}.

%% handle_cast/2
handle_cast(Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info(Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(Reason, State) ->
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

generate_events(#bucket{options=Options}) ->
	lists:member(generate_events, Options).

create_event(Bucket, ?MDB_RECORD(Key, Version, ?MDB_RECORD_DELETED)) ->
	create_event(?MDB_EVENT_DELETED, Bucket, Key, Version);
create_event(Bucket, ?MDB_RECORD(Key, Version, _Value)) ->
	create_event(?MDB_EVENT_UPDATED, Bucket, Key, Version).

create_event(EventName, Bucket, Key, Version) ->
	Info = #{?MDB_EVENT_FIELD_KEY => Key, ?MDB_EVENT_FIELD_VERSION => Version},
	eb_event:new(EventName, Bucket, Info).
