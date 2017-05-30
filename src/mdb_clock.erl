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

-module(mdb_clock).

%% ====================================================================
%% API functions
%% ====================================================================
-export([timestamp/0]).
-export([timestamp/1]).
-export([update/1]).
	
timestamp() -> nomad_hlc:encode(nomad_hlc:timestamp()).

timestamp(Seconds) ->
	{Type, Logical, Counter} = nomad_hlc:timestamp(),
	MS = Seconds * 1000,
	nomad_hlc:encode({Type, Logical - MS, Counter}).

update(ExternalTime) -> nomad_hlc:update(ExternalTime).
