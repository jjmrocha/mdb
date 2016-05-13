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

-export([notify/2]).

notify(#bucket{name=Bucket, options=Options}, Record) -> 
	case lists:member(generate_events, Options) of
		true -> 
			Event = create_event(Bucket, Record),
			event_broker:publish(Event)
		false -> ok 
	end.
  
%% ====================================================================
%% Internal functions
%% ====================================================================

create_event(Bucket, ?MDB_RECORD(Key, Version, ?MDB_RECORD_DELETED)) ->
	create_event(<<"mdb:deleted">>, Bucket, Key, Version);
create_event(Bucket, ?MDB_RECORD(Key, Version, _Value)) ->
	create_event(<<"mdb:updated">>, Bucket, Key, Version).

create_event(EventName, Bucket, Key, Version) ->
	Info = #{key => Key, version => Version},
	eb_event:new(EventName, Bucket, Info).
