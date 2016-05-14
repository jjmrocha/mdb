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

-module(mdb_event).

-include("mdb.hrl").
-include("mdb_event.hrl").

-export([notify/2]).
-export([subscribe/1, unsubscribe/1]).

notify(BI=#bucket{name=Bucket}, Record) -> 
	case generate_events(BI) of
		true -> 
			Event = create_event(Bucket, Record),
			eb_feed:publish(?MDB_NOTIFICATION_FEED, Event);
		false -> ok 
	end.

subscribe(BI=#bucket{name=Bucket}) ->
	case generate_events(BI) of
		true -> eb_filter_by_ref:start_filter(?MDB_NOTIFICATION_FEED, Bucket);
		false -> {error, bucket_do_not_generate_events}
	end.

unsubscribe(#bucket{name=Bucket}) ->
	eb_filter_by_ref:stop_filter(?MDB_NOTIFICATION_FEED, Bucket).
  
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
