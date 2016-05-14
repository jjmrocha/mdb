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

-module(mdb_app).

-include("mdb.hrl").
-include("mdb_event.hrl").

-behaviour(application).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
	{ok, Pid} = mdb_sup:start_link(),
	{ok, Buckets} = application:get_env(buckets),
	create_buckets(Buckets),
	create_feed(),
	{ok, Pid}.

stop(_State) ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_buckets([]) -> ok;
create_buckets([{Name, Options}|T]) when is_atom(Name) andalso is_list(Options) -> 
	ok = mdb:create(Name, Options),
	create_buckets(T);
create_buckets([H|T]) ->
	error_logger:error_msg("Invalid bucket configuration: ~p\n", [H]),
	create_buckets(T).

create_feed() ->
	ok = eb_feed_sup:create_feed(?MDB_NOTIFICATION_FEED, [?MDB_EVENT_UPDATED, ?MDB_EVENT_DELETED]).