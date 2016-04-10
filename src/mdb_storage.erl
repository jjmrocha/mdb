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

-module(mdb_storage).

-include("mdb.hrl").

-define(MDB_STORAGE, mdb_schema).

%% ====================================================================
%% API functions
%% ====================================================================
-export([create/0, drop/0]).
-export([create/2, drop/1]).
-export([memory/0, memory/1]).
-export([with_bucket/2, fold/2]).

create() ->
	ets:new(?MDB_STORAGE, [set, public, named_table, {read_concurrency, true}, {keypos, 2}]).

drop() ->
	ets:foldl(fun(#bucket{ets=TID}, _Acc) -> 
				ets:delete(TID) 
		end, 0, ?MDB_STORAGE),
	ets:delete(?MDB_STORAGE).

create(Bucket, Options) -> 
	case get_bucket(Bucket) of
		{ok, _} -> {error, bucket_already_exists};
		{error, bucket_not_found} ->
			TID = ets:new(?MDB_STORAGE, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]),
			true = ets:insert(?MDB_STORAGE, #bucket{name=Bucket, ets=TID, options=Options}),
			ok
	end.

drop(Bucket) -> 
	case get_bucket(Bucket) of
		{ok, #bucket{ets=TID}} -> 
			ets:delete(TID),
			ets:delete(?MDB_STORAGE, Bucket),
			ok;
		Other -> Other
	end.

memory() -> 
	Master = ets:info(?MDB_STORAGE, memory),
	ets:foldl(fun(#bucket{ets=TID}, Acc) -> 
				Acc + ets:info(TID, memory) 
		end, Master, ?MDB_STORAGE).	

memory(Bucket) -> 
	case get_bucket(Bucket) of
		{ok, #bucket{ets=TID}} -> 
			Size = ets:info(TID, memory),
			{ok, Size};
		Other -> Other
	end.

with_bucket(Bucket, Fun) ->
	case get_bucket(Bucket) of
		{ok, BI} -> Fun(BI);
		Other -> Other
	end.

fold(Fun, Acc) ->
	ets:foldr(Fun, Acc, ?MDB_STORAGE).

%% ====================================================================
%% Internal functions
%% ====================================================================

get_bucket(Bucket) ->
	case ets:lookup(?MDB_STORAGE, Bucket) of
		[] -> {error, bucket_not_found};
		[BI] -> {ok, BI}
	end.
