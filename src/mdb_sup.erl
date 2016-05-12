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

-module(mdb_sup).

-behaviour(supervisor).

-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).

start_link() ->
	supervisor:start_link(?MODULE, []).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% init/1
init([]) ->
	Server = {mdb, {mdb, start_link, []}, permanent, infinity, worker, [mdb]},
<<<<<<< HEAD
	Clock = {mdb_hlc, {mdb_hlc, start_link, []}, permanent, infinity, worker, [mdb_hlc]},
	Procs = [Server, Clock],
=======
	
	{ok, Multiplier} = application:get_env(async_processes_by_core),
	WorkerCount = erlang:system_info(schedulers) * Multiplier,
	Async = {mdb_async, {async, start_pool, [mdb_async, WorkerCount]}, permanent, 2000, supervisor, [worker_pool_sup, async]},
	
	Procs = [Server, Async],
>>>>>>> 9faa62c79653d44889be424709ada1e331bf7727
	{ok, {{one_for_one, 5, 60}, Procs}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
