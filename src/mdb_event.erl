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
-define(SUBSCRIPTION_TABLE, '$mdb.subscription.table').

%% ====================================================================
%% Behavioural functions definition
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([subscribe/1, unsubscribe/1, notify/2]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

subscribe(BI=#bucket{name=Bucket}) ->
	do_if_generates_events(BI, fun() -> gen_server:call(?MODULE, {subscribe, Bucket}) end).

unsubscribe(BI=#bucket{name=Bucket}) ->
	do_if_generates_events(BI, fun() -> gen_server:call(?MODULE, {unsubscribe, Bucket}) end).

notify(BI=#bucket{name=Bucket}, Record) ->
	do_if_generates_events(BI, fun() ->
	                               Event = create_event(Bucket, Record),
	                               gen_server:abcast(?MODULE, {notify, Bucket, Event})
	                           end).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {}).
-record(subscription, {bucket, subscriber, monitor_ref}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	% Create the subscription table
	nomad_keeper:create(?SUBSCRIPTION_TABLE, [bag, {keypos, #subscription.bucket}, private, named_table]),
	{ok, #state{}}.


%% handle_call/3
handle_call({subscribe, Bucket}, {Subscriber, _Ref}, State) ->
	case ets:select(?SUBSCRIPTION_TABLE, [{#subscription{bucket=Bucket, subscriber=Subscriber, _='_'}, [], ['$_']}]) of
		[] ->
			% New subscribtion
			MonitorRef = erlang:monitor(process, Subscriber),
			ets:insert(?SUBSCRIPTION_TABLE, #subscription{bucket=Bucket, subscriber=Subscriber, monitor_ref=MonitorRef});
		_ ->
			% Subscription exists
			done
	end,
	{reply, ok, State};

handle_call({unsubscribe, Bucket}, {Subscriber, _Ref}, State) ->
	case ets:select(?SUBSCRIPTION_TABLE, [{#subscription{bucket=Bucket, subscriber=Subscriber, _='_'}, [], ['$_']}]) of
		[] ->
			% Subscription does not exist
			done;
		[Subscription] ->
			% Subscription exists
			erlang:demonitor(Subscription#subscription.monitor_ref),
			ets:delete_object(?SUBSCRIPTION_TABLE, Subscription)
	end,
	{reply, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ok, State}.


%% handle_cast/2
handle_cast({notify, Bucket, Event}, State) ->
	% Send the Event to all subscribers
	Subscribers = ets:select(?SUBSCRIPTION_TABLE, [{#subscription{bucket=Bucket, subscriber='$1', _='_'}, [], ['$1']}]),
	lists:foreach(fun(Subscriber) -> Subscriber ! Event end, Subscribers),
	{noreply, State};

handle_cast(_Request, State) ->
	{noreply, State}.


%% handle_info/2
handle_info({'DOWN', MonitorRef, process, Subscriber, _Info}, State) ->
	% One of the monitored processes ended: clean all subscriptions
	ets:select_delete(?SUBSCRIPTION_TABLE, [{#subscription{subscriber=Subscriber, monitor_ref=MonitorRef, _='_'}, [], [true]}]),
	{noreply, State};

handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(Reason, _State) ->
	drop_table(Reason),
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
do_if_generates_events(BI, Fun) ->
	case generate_events(BI) of
		true -> Fun();
		false -> {error, bucket_do_not_generate_events}
	end.

generate_events(#bucket{options=Options}) ->
	lists:member(generate_events, Options).

create_event(Bucket, ?MDB_RECORD(Key, _Version, ?MDB_RECORD_DELETED)) ->
	create_event(?MDB_EVENT_DELETED, Bucket, Key);
create_event(Bucket, ?MDB_RECORD(Key, _Version, _Value)) ->
	create_event(?MDB_EVENT_UPDATED, Bucket, Key).

create_event(EventName, Bucket, Key) ->
	{EventName, Bucket, Key}.

drop_table(normal) -> nomad_keeper:drop(?SUBSCRIPTION_TABLE);
drop_table(shutdown) -> nomad_keeper:drop(?SUBSCRIPTION_TABLE);
drop_table({shutdown, _}) -> nomad_keeper:drop(?SUBSCRIPTION_TABLE);
drop_table(_) -> ok.
