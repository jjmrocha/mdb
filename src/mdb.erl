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
-export([create/2, drop/1, size/1, keys/1, memory/1, delete/1]).
-export([to_list/1, from_list/2]).
-export([get/2, get/3, put/3, put/4, remove/2, remove/3]).
-export([version/2, history/2, purge/1]).
-export([fold/3, foreach/2, filter/2]).
-export([delete/2, update/2]).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

%% @doc Used memory by MDB
-spec memory() -> integer().
memory() -> 
	Master = ets:info(?MDB_STORAGE, memory),
	ets:foldl(fun(#bucket{ets=TID}, Acc) -> 
				Acc + ets:info(TID, memory) 
		end, Master, ?MDB_STORAGE).	

%% @doc Returns the list of buckets
-spec buckets() -> list().
buckets() -> 
	ets:foldr(fun(#bucket{name=Name}, Acc) -> 
				[Name|Acc] 
		end, [], ?MDB_STORAGE).

%% @doc Creates a new bucket
%% Options: 
%%	keep_history - Don't remove old versions
%%
%% Reasons:
%% 	bucket_already_exists - If the bucket already exists 
-spec create(Bucket::atom(), Options::list()) -> ok | {error, Reason::term()}.
create(Bucket, Options) when is_atom(Bucket), is_list(Options) -> 
	case get_bucket(Bucket) of
		{ok, _} -> {error, bucket_already_exists};
		{error, bucket_not_found} ->
			TID = ets:new(?MDB_STORAGE, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]),
			true = ets:insert(?MDB_STORAGE, #bucket{name=Bucket, ets=TID, options=Options}),
			ok
	end.

%% @doc Drops the buckets
%% Reasons:
%%	bucket_not_found - If the bucket doesn't exists
-spec drop(Bucket::atom()) -> ok | {error, Reason::term()}.
drop(Bucket) when is_atom(Bucket) -> 
	case get_bucket(Bucket) of
		{ok, #bucket{ets=TID}} -> 
			ets:delete(TID),
			ets:delete(?MDB_STORAGE, Bucket),
			ok;
		Other -> Other
	end.
	
delete(Bucket) when is_atom(Bucket) ->	
	WriteVersion = timestamp(),
	with_bucket(Bucket, fun(BI) -> 
				Acc1 = do_fold(BI, fun(Key, Value, _Version, Acc) -> 
								write_key_value(BI, Key, ?MDB_VERSION_LAST, ?MDB_RECORD_DELETED, WriteVersion),
								Acc + 1
						end, 0),
				{ok, Acc1}
		end).
		
%% @doc Returs the number of keys on the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec size(Bucket::atom()) -> {ok, Size::integer()} | {error, Reason::term()}.
size(Bucket) when is_atom(Bucket) -> 
	fold(fun(_Key, _Value, _Version, Acc) -> 
				Acc + 1 
		end, 0, Bucket).

%% @doc Returs the list of keys on the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec keys(Bucket::atom()) -> {ok, Keys::list()} | {error, Reason::term()}.
keys(Bucket) when is_atom(Bucket) -> 
	fold(fun(Key, _Value, _Version, Acc) -> 
				[Key|Acc] 
		end, [], Bucket).

%% @doc Returs the memory used by the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec memory(Bucket::atom()) -> {ok, Size::integer()} | {error, Reason::term()}.
memory(Bucket) when is_atom(Bucket) -> 
	case get_bucket(Bucket) of
		{ok, #bucket{ets=TID}} -> 
			Size = ets:info(TID, memory),
			{ok, Size};
		Other -> Other
	end.

%% @doc Returs the Key/Value par from the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec to_list(Bucket::atom()) -> {ok, KeyValueList::list()} | {error, Reason::term()}.
to_list(Bucket) when is_atom(Bucket) ->
	fold(fun(Key, Value, _Version, Acc) -> 
				[{Key, Value}|Acc] 
		end, [], Bucket).

%% @doc Loads the Key/Value tuple list into the bucket
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
-spec from_list(Bucket::atom(), KeyValueList::list()) -> ok | {error, Reason::term()}.
from_list(Bucket, KeyValueList) when is_atom(Bucket), is_list(KeyValueList) -> 
	lists:foreach(fun({Key, Value}) ->
				put(Bucket, Key, Value)
		end, KeyValueList).

%% @doc Return the (specif version of the) value for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
%%	version_not_found - If the version doesn't exists
%%	deleted - If the key was deleted
-spec get(Bucket::atom(), Key::term(), Version::integer()) -> {ok, Value::term()} | {error, Reason::term()}.
get(Bucket, Key, Version) when is_atom(Bucket), is_integer(Version) -> 
	with_bucket(Bucket, fun(BI) -> 
				PK = ?MDB_PK_RECORD(Key, Version),
				case get_value(BI, PK) of 
					{ok, Value, Version} -> {ok, Value};
					{ok, _Value, _Version} -> {error, version_not_found};
					Other -> Other
				end
		end).

%% @doc Return the value (and version) for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
%%	version_not_found - If the version doesn't exists
-spec get(Bucket::atom(), Key::term()) -> {ok, Value::term(), Version::integer()} | {error, Reason::term()}.
get(Bucket, Key) when is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) -> 
				case get_value(BI, Key) of
					{error, deleted} -> {error, key_not_found};
					Other -> Other
				end
		end).

%% @doc Return the current version for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
-spec version(Bucket::atom(), Key::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
version(Bucket, Key) when is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) -> 
				case get_last_version(BI, Key, ?MDB_VERSION_LAST) of
					?MDB_KEY_NOT_FOUND -> {error, key_not_found};
					?MDB_PK_RECORD(_Key, Version) -> {ok, Version}		
				end
		end).

%% @doc Return the list of versions for a key 
%% Returns:
%%	bucket_not_found - If the bucket doesn't exists
%%	key_not_found - If the key doesn't exists
-spec history(Bucket::atom(), Key::term()) -> {ok, Versions::list()} | {error, Reason::term()}.		
history(Bucket, Key) when is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) -> 
				case versions(BI, Key) of
					?MDB_KEY_NOT_FOUND -> {error, key_not_found};
					Versions -> {ok, Versions}
				end
		end).

-spec purge(Bucket::atom()) -> ok | {error, Reason::term()}.	
purge(Bucket) when is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) -> 
				do_clean(BI)
		end).

-spec put(Bucket::atom(), Key::term(), Value::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
put(Bucket, Key, Value) when is_atom(Bucket) ->
	put(Bucket, Key, Value, ?MDB_VERSION_LAST).

-spec put(Bucket::atom(), Key::term(), Value::term(), ReadVersion::integer()) -> {ok, Version::integer()} | {error, Reason::term()}.
put(Bucket, Key, Value, ReadVersion) when is_atom(Bucket) ->
	with_bucket(Bucket, fun(BI) ->
				WriteVersion = timestamp(),
				?catcher(write_key_value(BI, Key, ReadVersion, Value, WriteVersion))
		end).

-spec remove(Bucket::atom(), Key::term()) -> {ok, Version::integer()} | {error, Reason::term()}.
remove(Bucket, Key) when is_atom(Bucket) -> 
	remove(Bucket, Key, ?MDB_VERSION_LAST).

-spec remove(Bucket::atom(), Key::term(), ReadVersion::integer()) -> {ok, Version::integer()} | {error, Reason::term()}.
remove(Bucket, Key, ReadVersion) when is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) ->
				case get_value(BI, Key, ReadVersion) of
					{ok, _Value, Version} ->
						WriteVersion = timestamp(),
						?catcher(write_key_value(BI, Key, Version, ?MDB_RECORD_DELETED, WriteVersion));
					Other -> Other
				end
		end).

-spec fold(Fun::fun((Key::term(), Value::term(), Acc::term()) -> Acc1::term()), Acc::term(), Bucket::atom()) -> {ok, Acc1::term()} | {error, Reason::term()}.
fold(Fun, Acc, Bucket) when is_function(Fun, 4), is_atom(Bucket) -> 
	with_bucket(Bucket, fun(BI) -> 
				Acc1 = do_fold(BI, Fun, Acc),
				{ok, Acc1}
		end).

foreach(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) -> 
	fold(fun(Key, Value, _Version, Acc) -> 
				Fun(Key, Value),
				Acc + 1
		end, 0, Bucket).

filter(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) -> 
	fold(fun(Key, Value, _Version, Acc) -> 
				case Fun(Key, Value) of
					true -> [{Key, Value}|Acc];
					false -> Acc
				end
		end, [], Bucket).

delete(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) ->
	WriteVersion = timestamp(),
	with_bucket(Bucket, fun(BI) -> 
				Acc1 = do_fold(BI, fun(Key, Value, _Version, Acc) -> 
								case Fun(Key, Value) of
									true -> 
										write_key_value(BI, Key, ?MDB_VERSION_LAST, ?MDB_RECORD_DELETED, WriteVersion),
										Acc + 1;
									false -> Acc
								end
						end, 0),
				{ok, Acc1}
		end).

update(Fun, Bucket) when is_function(Fun, 2), is_atom(Bucket) ->
	WriteVersion = timestamp(),
	with_bucket(Bucket, fun(BI) -> 
				Acc1 = do_fold(BI, fun(Key, Value, _Version, Acc) -> 
								case Fun(Key, Value) of
									{true, NewValue} -> 
										write_key_value(BI, Key, ?MDB_VERSION_LAST, NewValue, WriteVersion),
										Acc + 1;
									false -> Acc
								end
						end, 0),
				{ok, Acc1}
		end).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {timer_ref}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	ets:new(?MDB_STORAGE, [set, public, named_table, {read_concurrency, true}, {keypos, 2}]),
	{ok, Timer} = timer:send_interval(?MDB_DB_CLEAN_INTERVAL, {run_db_clean}),
	{ok, #state{timer_ref=Timer}}.

%% handle_call/3
handle_call(_Request, _From, State) ->
	{noreply, State}.

%% handle_cast/2
handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info({run_db_clean}, State) ->
	db_clean(),
	{noreply, State};
handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(_Reason, #state{timer_ref=Timer}) ->
	ets:foldl(fun(#bucket{ets=TID}, _Acc) -> 
				ets:delete(TID) 
		end, 0, ?MDB_STORAGE),
	ets:delete(?MDB_STORAGE),
	timer:cancel(Timer),
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

with_bucket(Bucket, Fun) ->
	case get_bucket(Bucket) of
		{ok, BI} -> Fun(BI);
		Other -> Other
	end.

get_bucket(Bucket) ->
	case ets:lookup(?MDB_STORAGE, Bucket) of
		[] -> {error, bucket_not_found};
		[BI] -> {ok, BI}
	end.

db_clean() ->
	{ok, Threshold} = application:get_env(obsolete_threshold),
	TS = timestamp(Threshold),
	ets:foldl(fun(BI=#bucket{ets=TID, options=Options}, _Acc) ->
				case lists:member(keep_history, Options) of
					true -> ok;
					false -> do_clean(BI=#bucket{ets=TID}, TS) 
				end
		end, 0, ?MDB_STORAGE).

do_clean(BI=#bucket{ets=TID}) ->
	{ok, Threshold} = application:get_env(mdb, obsolete_threshold),
	TS = timestamp(Threshold),
	do_clean(BI=#bucket{ets=TID}, TS).
	
do_clean(BI=#bucket{ets=TID}, TS) ->
	MatchSpec = [{?MDB_RECORD('$1', '$2', '$3'), [{'<', '$2', TS}], ['$_']}],
	case ets:select_reverse(TID, MatchSpec, 500) of
		{Matched, Continuation} -> do_clean(BI, Continuation, '$mdb_no_key', Matched);
		'$end_of_table' -> ok
	end. 

do_clean(BI=#bucket{ets=TID}, Continuation, LastKey, [?MDB_RECORD(LastKey, Version, _)|T]) ->
	ets:delete(TID, ?MDB_PK_RECORD(LastKey, Version)),
	do_clean(BI, Continuation, LastKey, T);
do_clean(BI=#bucket{ets=TID}, Continuation, _LastKey, [?MDB_RECORD(Key, Version, ?MDB_RECORD_DELETED)|T]) ->
	ets:delete(TID, ?MDB_PK_RECORD(Key, Version)),
	do_clean(BI, Continuation, Key, T);
do_clean(BI=#bucket{ets=TID}, Continuation, _LastKey, [?MDB_RECORD(Key, Version, _)|T]) ->
	PK = ?MDB_PK_RECORD(Key, Version),
	case is_last_version(BI, PK) of
		true -> ok;
		false -> ets:delete(TID, PK)
	end,
	do_clean(BI, Continuation, Key, T);
do_clean(BI, Continuation, LastKey, []) ->
	case ets:select_reverse(Continuation) of
		{Matched, Continuation1} -> do_clean(BI, Continuation1, LastKey, Matched);
		'$end_of_table' -> ok
	end.

timestamp(Seconds) ->
	MS = Seconds * 1000000,
	timestamp() - MS.

timestamp() ->
	TS = {_,_, Micro} = os:timestamp(),
	Utc = calendar:now_to_universal_time(TS),
	Seconds = calendar:datetime_to_gregorian_seconds(Utc),
	((Seconds - 62167219200) * 1000000) + Micro. 

get_value(BI, Key, Version) ->
	PK = get_last_version(BI, Key, Version),
	get_value(BI, PK).

get_value(_, ?MDB_KEY_NOT_FOUND) -> {error, key_not_found};
get_value(#bucket{ets=TID}, PK=?MDB_PK_RECORD(_Key, _Version)) ->
	case ets:lookup(TID, PK) of
		[] -> {error, version_not_found};
		[?MDB_RECORD(_Key, _Version, ?MDB_RECORD_DELETED)] -> {error, deleted};
		[?MDB_RECORD(_Key, Version, Value)] -> {ok, Value, Version}
	end;
get_value(#bucket{ets=TID}, Key) ->
	get_value(#bucket{ets=TID}, Key, ?MDB_VERSION_LAST).

get_last_version(#bucket{ets=TID}, Key, Version) ->
	FixedVersion = fix_search_version(Version),
	case ets:prev(TID, ?MDB_PK_RECORD(Key, FixedVersion)) of
		'$end_of_table' -> ?MDB_KEY_NOT_FOUND;
		Last = ?MDB_PK_RECORD(Key, _) -> Last;
		_ -> ?MDB_KEY_NOT_FOUND
	end.

fix_search_version(?MDB_VERSION_LAST) -> ?MDB_VERSION_LAST;
fix_search_version(Version) -> Version + 0.1.

versions(#bucket{ets=TID}, Key) ->
	versions(TID, ?MDB_PK_RECORD(Key, ?MDB_VERSION_LAST), []).

versions(TID, PK=?MDB_PK_RECORD(Key, _), Acc) ->
	case ets:prev(TID, PK) of
		'$end_of_table' when length(Acc) =:= 0 -> ?MDB_KEY_NOT_FOUND;
		'$end_of_table' -> Acc;
		Prev = ?MDB_PK_RECORD(Key, Version) -> versions(TID, Prev, [Version|Acc]);
		_ when length(Acc) =:= 0 -> ?MDB_KEY_NOT_FOUND;
		_ -> Acc
	end.	

is_last_version(#bucket{ets=TID}, PK=?MDB_PK_RECORD(Key, _)) ->
	case ets:next(TID, PK) of
		'$end_of_table' -> true;
		?MDB_PK_RECORD(Key, _) -> false;
		_ -> true
	end.

write_key_value(BI=#bucket{ets=TID}, Key, ReadVersion, Value, WriteVersion) ->
	validate_read_version(BI, Key, ReadVersion),
	ets:insert(TID, ?MDB_RECORD(Key, WriteVersion, Value)),
	{ok, WriteVersion}.

validate_read_version(_BI, _Key, ?MDB_VERSION_LAST) -> ok;
validate_read_version(BI, Key, Version) ->
	case is_last_version(BI, ?MDB_PK_RECORD(Key, Version)) of
		true -> ok;
		false -> system_abort(not_last_version)
	end.

system_abort(Reason) -> throw({system_abort, Reason}).

do_fold(BI, Fun, Acc) ->
	ReadVersion = timestamp(),
	do_fold(BI, Fun, Acc, ReadVersion).

do_fold(#bucket{ets=TID}, Fun, Acc, Version) ->
	MatchSpec = [{?MDB_RECORD('$1', '$2', '$3'), [{'<', '$2', Version}], ['$_']}],
	case ets:select_reverse(TID, MatchSpec, 500) of
		{Matched, Continuation} -> do_fold(TID, Fun, Acc, Continuation, '$mdb_no_key', Matched);
		'$end_of_table' -> Acc
	end.

do_fold(TID, Fun, Acc, Continuation, LastKey, [?MDB_RECORD(LastKey, _, _)|T]) ->
	do_fold(TID, Fun, Acc, Continuation, LastKey, T);
do_fold(TID, Fun, Acc, Continuation, _LastKey, [?MDB_RECORD(Key, _, ?MDB_RECORD_DELETED)|T]) ->
	do_fold(TID, Fun, Acc, Continuation, Key, T);
do_fold(TID, Fun, Acc, Continuation, _LastKey, [?MDB_RECORD(Key, Version, Value)|T]) ->
	Acc1 = Fun(Key, Value, Version, Acc),
	do_fold(TID, Fun, Acc1, Continuation, Key, T);
do_fold(TID, Fun, Acc, Continuation, LastKey, []) ->
	case ets:select_reverse(Continuation) of
		{Matched, Continuation1} -> do_fold(TID, Fun, Acc, Continuation1, LastKey, Matched);
		'$end_of_table' -> Acc
	end.
