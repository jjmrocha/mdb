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

%% ====================================================================
%% Constants
%% ====================================================================
-define(get_event_group(Bucket), {'$mdb.event.group', Bucket}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([subscribe/1, unsubscribe/1, notify/2]).

subscribe(BI=#bucket{name=Bucket}) ->
	do_if_generates_events(BI, fun() -> nomad_group:join(?get_event_group(Bucket)) end).

unsubscribe(BI=#bucket{name=Bucket}) ->
	do_if_generates_events(BI, fun() -> nomad_group:leave(?get_event_group(Bucket)) end).

notify(BI=#bucket{name=Bucket}, Record) ->
	do_if_generates_events(BI, fun() ->
	                               Event = create_event(Bucket, Record),
	                               nomad_group:publish(?get_event_group(Bucket), Event)
	                           end).

%% ====================================================================
%% Internal functions
%% ====================================================================
do_if_generates_events(BI, Fun) ->
	case generate_events(BI) of
		true -> Fun();
		false -> {error, bucket_does_not_generate_events}
	end.

generate_events(#bucket{options=Options}) ->
	lists:member(generate_events, Options).

create_event(Bucket, ?MDB_RECORD(Key, _Version, ?MDB_RECORD_DELETED)) ->
	create_event(?MDB_EVENT_DELETED, Bucket, Key);
create_event(Bucket, ?MDB_RECORD(Key, _Version, _Value)) ->
	create_event(?MDB_EVENT_UPDATED, Bucket, Key).

create_event(EventName, Bucket, Key) ->
	{EventName, Bucket, Key}.
