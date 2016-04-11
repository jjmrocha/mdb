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

-record(bucket, {name, ets, options}).

-define(MDB_PK_RECORD(Key, Version), {p, Key, Version}).
-define(MDB_RECORD(Key, Version, Value), {?MDB_PK_RECORD(Key, Version), Value}).

-define(MDB_VERSION_FIRST, 0).
-define(MDB_VERSION_LAST, '$last').

-define(MDB_RECORD_DELETED, '$mdb_record_deleted').
-define(MDB_KEY_NOT_FOUND, '$mdb_key_not_found').

-define(catcher(Call), 
	try Call
	catch _:{system_abort, Reason} -> {error, Reason}
	end
).
