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

-module(mdb_mvcc).

-include("mdb.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([fold/3, fold/4]).
-export([clean/0, clean/1]).
-export([write_key_value/5]).
-export([get_value/2, get_value/3]).
-export([get_last_version/3, versions/2]).

fold(BI, Fun, Acc) ->
	ReadVersion = mdb_hlc:timestamp(),
	fold(BI, Fun, Acc, ReadVersion).

fold(#bucket{ets=TID}, Fun, Acc, Version) ->
	MatchSpec = [{?MDB_RECORD('$1', '$2', '$3'), [{'<', '$2', Version}], ['$_']}],
	case ets:select_reverse(TID, MatchSpec, 500) of
		{Matched, Continuation} -> fold(TID, Fun, Acc, Continuation, '$mdb_no_key', Matched);
		'$end_of_table' -> Acc
	end.

clean() ->
	{ok, Threshold} = application:get_env(obsolete_threshold),
	TS = mdb_hlc:timestamp(Threshold),
	mdb_storage:fold(fun(BI=#bucket{options=Options}, _Acc) ->
				case lists:member(keep_history, Options) of
					true -> ok;
					false -> clean(BI, TS) 
				end
		end, 0).

clean(BI) ->
	{ok, Threshold} = application:get_env(mdb, obsolete_threshold),
	TS = mdb_hlc:timestamp(Threshold),
	clean(BI, TS).

write_key_value(BI=#bucket{ets=TID}, Key, ReadVersion, Value, WriteVersion) ->
	validate_read_version(BI, Key, ReadVersion),
	ets:insert(TID, ?MDB_RECORD(Key, WriteVersion, Value)),
	{ok, WriteVersion}.

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

versions(#bucket{ets=TID}, Key) ->
	versions(TID, ?MDB_PK_RECORD(Key, ?MDB_VERSION_LAST), []).

%% ====================================================================
%% Internal functions
%% ====================================================================

fold(TID, Fun, Acc, Continuation, LastKey, [?MDB_RECORD(LastKey, _, _)|T]) ->
	fold(TID, Fun, Acc, Continuation, LastKey, T);
fold(TID, Fun, Acc, Continuation, _LastKey, [?MDB_RECORD(Key, _, ?MDB_RECORD_DELETED)|T]) ->
	fold(TID, Fun, Acc, Continuation, Key, T);
fold(TID, Fun, Acc, Continuation, _LastKey, [?MDB_RECORD(Key, Version, Value)|T]) ->
	Acc1 = Fun(Key, Value, Version, Acc),
	fold(TID, Fun, Acc1, Continuation, Key, T);
fold(TID, Fun, Acc, Continuation, LastKey, []) ->
	case ets:select_reverse(Continuation) of
		{Matched, Continuation1} -> fold(TID, Fun, Acc, Continuation1, LastKey, Matched);
		'$end_of_table' -> Acc
	end.

clean(BI=#bucket{ets=TID}, TS) ->
	MatchSpec = [{?MDB_RECORD('$1', '$2', '$3'), [{'<', '$2', TS}], ['$_']}],
	case ets:select_reverse(TID, MatchSpec, 500) of
		{Matched, Continuation} -> clean(BI, Continuation, '$mdb_no_key', Matched);
		'$end_of_table' -> ok
	end. 

clean(BI=#bucket{ets=TID}, Continuation, LastKey, [?MDB_RECORD(LastKey, Version, _)|T]) ->
	ets:delete(TID, ?MDB_PK_RECORD(LastKey, Version)),
	clean(BI, Continuation, LastKey, T);
clean(BI=#bucket{ets=TID}, Continuation, _LastKey, [?MDB_RECORD(Key, Version, ?MDB_RECORD_DELETED)|T]) ->
	ets:delete(TID, ?MDB_PK_RECORD(Key, Version)),
	clean(BI, Continuation, Key, T);
clean(BI=#bucket{ets=TID}, Continuation, _LastKey, [?MDB_RECORD(Key, Version, _)|T]) ->
	PK = ?MDB_PK_RECORD(Key, Version),
	case is_last_version(BI, PK) of
		true -> ok;
		false -> ets:delete(TID, PK)
	end,
	clean(BI, Continuation, Key, T);
clean(BI, Continuation, LastKey, []) ->
	case ets:select_reverse(Continuation) of
		{Matched, Continuation1} -> clean(BI, Continuation1, LastKey, Matched);
		'$end_of_table' -> ok
	end.

system_abort(Reason) -> throw({system_abort, Reason}).

fix_search_version(?MDB_VERSION_LAST) -> ?MDB_VERSION_LAST;
fix_search_version(Version) -> Version + 0.1.

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

validate_read_version(_BI, _Key, ?MDB_VERSION_LAST) -> ok;
validate_read_version(BI, Key, Version) ->
	case is_last_version(BI, ?MDB_PK_RECORD(Key, Version)) of
		true -> ok;
		false -> system_abort(not_last_version)
	end.