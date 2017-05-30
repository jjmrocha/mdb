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

-module(mdb).

-include("mdb.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([memory/0]).
-export([buckets/0]).
-export([create_bucket/2, drop_bucket/1, size/1, keys/1, memory/1, clear/1]).
-export([to_list/1, from_list/2]).
-export([get/2, get/3, set/3, set/4, remove/2, remove/3]).
-export([version/2, history/2, purge/1]).
-export([fold/3, foreach/2, filter/2]).
-export([delete/2, update/2]).
-export([subscribe/1, unsubscribe/1]).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

%% @doc Used memory by MDB
-spec memory() -> integer().
memory() -> mdb_storage:memory().

%% @doc Returns the list of buckets
-spec buckets() -> list().
buckets() -> 
	mdb_storage:fold(fun(#bucket{name=Name}, Acc) -> 
				[Name|Acc] 
		end, []).

%% @doc Creates a new bucket
%% Options: 
%%	keep_history - Don't remove old versions
%%	generate_events - To generate events every time a key is created/updated or deleted
%%
%% Reasons:
%% 	bucket_already_exists - If the bucket already exists 
-spec create_bucket(Bucket::atom(), Options::list()) -> ok | {error, Reason::term()}.
create_bucket(Bucket, Options) when is_atom(Bucket), is_list(Options) -> 
	mdb_storage:create(Bucket, Options);
create_bucket(_, _) -> {error, badarg}.

%% @doc Drops the buckets
%% Reasons:
%%	bucket_not_found - If the bucket doesn't exists
-spec drop_bucket(Bucket::atom()) -> ok | {error, Reason::term()}.
drop_bucket(Bucket) when is_atom(Bucket) -> 
	mdb_storage:drop(Bucket);
drop_bucket(_) -> {error, badarg}.

clear(Bucket) when is_atom(Bucket) ->	
	mdb_storage:with_bucket(Bucket, fun(BI) ->
				WriteVersion = mdb_clock:timestamp(),
				Acc1 = mdb_mvcc:fold(BI, fun(Key, _Value, _Version, Acc) -> 
								mdb_mvcc:remove_value(BI, Key, WriteVersion),
								Acc + 1
						end, 0),
				{ok, Acc1}
		end);
clear(_) -> {error, badarg}.

%% @doc Returs the number of keys on the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec size(Bucket::atom()) -> {ok, Size::integer()} | {error, Reason::term()}.
size(Bucket) when is_atom(Bucket) -> 
	fold(fun(_Key, _Value, _Version, Acc) -> 
				Acc + 1 
		end, 0, Bucket);
size(_) -> {error, badarg}.

%% @doc Returs the list of keys on the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec keys(Bucket::atom()) -> {ok, Keys::list()} | {error, Reason::term()}.
keys(Bucket) when is_atom(Bucket) -> 
	fold(fun(Key, _Value, _Version, Acc) -> 
				[Key|Acc] 
		end, [], Bucket);
keys(_) -> {error, badarg}.

%% @doc Returs the memory used by the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec memory(Bucket::atom()) -> {ok, Size::integer()} | {error, Reason::term()}.
memory(Bucket) when is_atom(Bucket) -> 
	mdb_storage:memory(Bucket);
memory(_) -> {error, badarg}.

%% @doc Returs the Key/Value par from the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec to_list(Bucket::atom()) -> {ok, KeyValueList::list()} | {error, Reason::term()}.
to_list(Bucket) when is_atom(Bucket) ->
	fold(fun(Key, Value, _Version, Acc) -> 
				[{Key, Value}|Acc] 
		end, [], Bucket);
to_list(_) -> {error, badarg}.

%% @doc Loads the Key/Value tuple list into the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec from_list(Bucket::atom(), KeyValueList::list()) -> ok | {error, Reason::term()}.
from_list(Bucket, KeyValueList) when is_atom(Bucket), is_list(KeyValueList) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				WriteVersion = mdb_clock:timestamp(),
				lists:foreach(fun({Key, Value}) ->
							mdb_mvcc:update_value(BI, Key, Value, WriteVersion)
					end, KeyValueList)
		end);
from_list(_, _) -> {error, badarg}.

%% @doc Return the (specif version of the) value for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
%%	version_not_found - If the version doesn't exists
%%	deleted - If the key was deleted
-spec get(Bucket::atom(), Key::term(), Version::integer()) -> {ok, Value::term()} | {error, Reason::term()}.
get(Bucket, Key, Version) when is_atom(Bucket), is_integer(Version) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				PK = ?MDB_PK_RECORD(Key, Version),
				case mdb_mvcc:get_value(BI, PK) of 
					{ok, Value, Version} -> {ok, Value};
					{ok, _Value, _Version} -> {error, version_not_found};
					Other -> Other
				end
		end);
get(_, _, _) -> {error, badarg}.

%% @doc Return the value (and version) for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
%%	version_not_found - If the version doesn't exists
-spec get(Bucket::atom(), Key::term()) -> {ok, Value::term(), Version::integer()} | {error, Reason::term()}.
get(Bucket, Key) when is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				case mdb_mvcc:get_value(BI, Key) of
					{error, deleted} -> {error, key_not_found};
					Other -> Other
				end
		end);
get(_, _) -> {error, badarg}.

%% @doc Return the current version for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
-spec version(Bucket::atom(), Key::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
version(Bucket, Key) when is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				case mdb_mvcc:get_last_version(BI, Key, ?MDB_VERSION_LAST) of
					?MDB_KEY_NOT_FOUND -> {error, key_not_found};
					?MDB_PK_RECORD(_Key, Version) -> {ok, Version}		
				end
		end);
version(_, _) -> {error, badarg}.

%% @doc Return the list of versions for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
-spec history(Bucket::atom(), Key::term()) -> {ok, Versions::list()} | {error, Reason::term()}.		
history(Bucket, Key) when is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				case mdb_mvcc:versions(BI, Key) of
					?MDB_KEY_NOT_FOUND -> {error, key_not_found};
					Versions -> {ok, Versions}
				end
		end);
history(_, _) -> {error, badarg}.

-spec purge(Bucket::atom()) -> ok | {error, Reason::term()}.	
purge(Bucket) when is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				mdb_mvcc:clean(BI)
		end);
purge(_) -> {error, badarg}.

-spec set(Bucket::atom(), Key::term(), Value::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
set(Bucket, Key, Value) when is_atom(Bucket) ->
	set(Bucket, Key, Value, ?MDB_VERSION_LAST);
set(_, _, _) -> {error, badarg}.

-spec set(Bucket::atom(), Key::term(), Value::term(), ReadVersion::integer()) -> {ok, Version::integer()} | {error, Reason::term()}.
set(Bucket, Key, Value, ReadVersion) when is_atom(Bucket) ->
	mdb_storage:with_bucket(Bucket, fun(BI) ->
				WriteVersion = mdb_clock:timestamp(),
				?catcher(mdb_mvcc:update_value(BI, Key, Value, WriteVersion, ReadVersion))
		end);
set(_, _, _, _) -> {error, badarg}.

-spec remove(Bucket::atom(), Key::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
remove(Bucket, Key) when is_atom(Bucket) -> 
	remove(Bucket, Key, ?MDB_VERSION_LAST);
remove(_, _) -> {error, badarg}.

-spec remove(Bucket::atom(), Key::term(), ReadVersion::integer()) -> {ok, Version::integer()} | {error, Reason::term()}.
remove(Bucket, Key, ReadVersion) when is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) ->
				case mdb_mvcc:get_value(BI, Key, ReadVersion) of
					{ok, _Value, Version} ->
						WriteVersion = mdb_clock:timestamp(),
						?catcher(mdb_mvcc:remove_value(BI, Key, WriteVersion, Version));
					Other -> Other
				end
		end);
remove(_, _, _) -> {error, badarg}.

-spec fold(Fun::fun((Key::term(), Value::term(), Acc::term()) -> Acc1::term()), Acc::term(), Bucket::atom()) -> {ok, Acc1::term()} | {error, Reason::term()}.
fold(Fun, Acc, Bucket) when is_function(Fun, 4), is_atom(Bucket) -> 
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				Acc1 = mdb_mvcc:fold(BI, Fun, Acc),
				{ok, Acc1}
		end);
fold(_, _, _) -> {error, badarg}.

foreach(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) -> 
	fold(fun(Key, Value, _Version, Acc) -> 
				Fun(Key, Value),
				Acc + 1
		end, 0, Bucket);
foreach(_, _) -> {error, badarg}.

filter(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) -> 
	fold(fun(Key, Value, _Version, Acc) -> 
				case Fun(Key, Value) of
					true -> [{Key, Value}|Acc];
					false -> Acc
				end
		end, [], Bucket);
filter(_, _) -> {error, badarg}.

delete(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) ->
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				WriteVersion = mdb_clock:timestamp(),
				Acc1 = mdb_mvcc:fold(BI, fun(Key, Value, _Version, Acc) -> 
								case Fun(Key, Value) of
									true -> 
										mdb_mvcc:remove_value(BI, Key, WriteVersion),
										Acc + 1;
									false -> Acc
								end
						end, 0),
				{ok, Acc1}
		end);
delete(_, _) -> {error, badarg}.

update(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) ->
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				WriteVersion = mdb_clock:timestamp(),
				Acc1 = mdb_mvcc:fold(BI, fun(Key, Value, _Version, Acc) -> 
								case Fun(Key, Value) of
									{true, NewValue} -> 
										mdb_mvcc:update_value(BI, Key, NewValue, WriteVersion),
										Acc + 1;
									false -> Acc
								end
						end, 0),
				{ok, Acc1}
		end);
update(_, _) -> {error, badarg}.

subscribe(Bucket) when is_atom(Bucket) ->
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				mdb_event:subscribe(BI)
		end);
subscribe(_) -> {error, badarg}.

unsubscribe(Bucket) when is_atom(Bucket) ->
	mdb_storage:with_bucket(Bucket, fun(BI) -> 
				mdb_event:unsubscribe(BI)
		end);
unsubscribe(_) -> {error, badarg}.

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {timer_ref}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	mdb_storage:create(),
	{ok, Interval} = application:get_env(obsolete_purge_interval),
	{ok, Timer} = timer:send_interval(Interval * 1000, {run_db_clean}),
	{ok, #state{timer_ref=Timer}}.

%% handle_call/3
handle_call(_Request, _From, State) ->
	{noreply, State}.

%% handle_cast/2
handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info({run_db_clean}, State) ->
	mdb_mvcc:clean(),
	{noreply, State};
handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(_Reason, #state{timer_ref=Timer}) ->
	timer:cancel(Timer),
	mdb_storage:drop(),
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

