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
-define(EVENT_GROUP(Bucket), {'$mdb.event.group', Bucket}).
-define(EVENT(EventName, Bucket, Key), {EventName, Bucket, Key}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([subscribe/1, unsubscribe/1, notify/2]).

subscribe(BI=#bucket{name=Bucket}) ->
	if_generates_events(BI, fun() -> nomad_group:join(?EVENT_GROUP(Bucket)) end).

unsubscribe(BI=#bucket{name=Bucket}) ->
	if_generates_events(BI, fun() -> nomad_group:leave(?EVENT_GROUP(Bucket)) end).

notify(BI=#bucket{name=Bucket}, Record) ->
	if_generates_events(BI, fun() ->
	                               Event = create_event(Bucket, Record),
	                               nomad_group:publish(?EVENT_GROUP(Bucket), Event)
	                           end).

%% ====================================================================
%% Internal functions
%% ====================================================================
if_generates_events(BI, Fun) ->
	run_if(generate_events(BI), Fun, {error, bucket_does_not_generate_events}).

run_if(true, Fun, _) -> Fun();
run_if(false, _, Else) -> Else.

generate_events(#bucket{options=Options}) ->
	lists:member(generate_events, Options).

create_event(Bucket, ?MDB_RECORD(Key, _Version, ?MDB_RECORD_DELETED)) ->
	?EVENT(?MDB_EVENT_DELETED, Bucket, Key);
create_event(Bucket, ?MDB_RECORD(Key, _Version, _Value)) ->
	?EVENT(?MDB_EVENT_UPDATED, Bucket, Key).
