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

-module(mdb_hlc).

%% ====================================================================
%% API functions
%% ====================================================================
-export([timestamp/0, timestamp/1]).

timestamp(Seconds) ->
	MS = Seconds * 1000000,
	timestamp() - MS.

timestamp() ->
	TS = {_,_, Micro} = os:timestamp(),
	Utc = calendar:now_to_universal_time(TS),
	Seconds = calendar:datetime_to_gregorian_seconds(Utc),
	((Seconds - 62167219200) * 1000000) + Micro. 

%% ====================================================================
%% Internal functions
%% ====================================================================


