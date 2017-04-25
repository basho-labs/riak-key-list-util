%% -------------------------------------------------------------------
%%
%% key_list_util: utility console script for per-vnode key counting, siblings logging and more
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% TODOs
%% * replace foreach loops with foldl loops, taking errors into account and returning them

-module(key_list_util).
-compile(export_all).

% Describes the Contents of a Riak object. A "sibling" is an instance of this record.
% Duplicated from riak_kv/riak_object, since it's needed by compare_content_dates()
-record(r_content, {
		  metadata :: dict(),
		  value :: term()
		 }).

% Describes a Riak Object
% Duplicated from riak_kv/riak_object, since it's needed by compare_content_dates()
-record(r_object, {
		  bucket :: riak_object:bucket(),
		  key :: riak_object:key(),
		  contents :: [#r_content{}],
		  vclock = vclock:fresh() :: vclock:vclock(),
		  updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
		  updatevalue :: term()
		 }).

%% =================================================================================================
%% In the following functions, Bucket can be a Bucket or a {BucketType, Bucket} pair.
%% Including this parameter implies the operation will be run only upon the specified Bucket or pair.

count_all_keys(OutputDir) ->
	process_cluster_parallel(OutputDir, [count_keys, log_siblings]).

count_all_keys_for_bucket(OutputDir, Bucket) ->
	process_cluster_parallel(OutputDir, [count_keys, log_siblings, {bucket, Bucket}]).

count_all_keys_for_vnode(OutputDir, Vnode) ->
	process_cluster_parallel_for_vnode(OutputDir, [count_keys, log_siblings], Vnode).

log_all_keys(OutputDir) ->
	process_cluster_parallel(OutputDir, [log_keys]).

log_all_keys_for_bucket(OutputDir, Bucket) ->
	process_cluster_parallel(OutputDir, [log_keys, {bucket, Bucket}]).

log_all_keys_for_vnode(OutputDir, Vnode) ->
	process_cluster_parallel_for_vnode(OutputDir, [log_keys], Vnode).

% Find the size of data stored. Takes options to add object metadata and 
% count siblings. Default is to only count value sizes and only the first 
% returned sibling. Send Options as a list of atoms 
% e.g. [raw_size, ignore_siblings] to add metadata to results and count 
% all siblings
size_all_keys(OutputDir) ->
	process_cluster_parallel(OutputDir, [size_keys]).

size_all_keys(OutputDir, Options) ->
	process_cluster_parallel(OutputDir, [size_keys|Options]).

size_all_keys_for_bucket(OutputDir, Bucket) ->
	process_cluster_parallel(OutputDir, [size_keys, {bucket, Bucket}]).

size_all_keys_for_bucket(OutputDir, Bucket, Options) ->
	process_cluster_parallel(OutputDir, [size_keys, {bucket, Bucket}] ++ Options).

size_all_keys_for_vnode(OutputDir, Vnode) ->
	process_cluster_parallel_for_vnode(OutputDir, [size_keys], Vnode).

size_all_keys_for_vnode(OutputDir, Vnode, Options) ->
	process_cluster_parallel_for_vnode(OutputDir, [size_keys|Options], Vnode).

% SleepPeriod - optional amount of time to sleep between each key operation,
% in milliseconds
log_all_keys(OutputDir, SleepPeriod) ->
	process_cluster_parallel(OutputDir, [log_keys, {sleep_for, SleepPeriod}]).

log_all_keys_for_bucket(OutputDir, SleepPeriod, Bucket) ->
	process_cluster_parallel(OutputDir, [log_keys, {sleep_for, SleepPeriod}, {bucket, Bucket}]).

log_all_keys_for_vnode(OutputDir, SleepPeriod, Vnode) ->
	process_cluster_parallel_for_vnode(OutputDir, [log_keys, {sleep_for, SleepPeriod}], Vnode).

%% Call size_all_keys* functions like: 
%%    size_all_keys_(OutputDir, [{sleep_for, SleepPeriod}])
%% to sleep between key operations. Setting Options already creates a /2 function
%% Other options can also be added to the call list

resolve_all_siblings(OutputDir) ->
	process_cluster_serial(OutputDir, [log_siblings, resolve_siblings]).

resolve_all_siblings_for_bucket(OutputDir, Bucket) ->
	process_cluster_serial(OutputDir, [log_siblings, resolve_siblings, {bucket, Bucket}]).

resolve_all_siblings_for_vnode(OutputDir, Vnode) ->
	process_cluster_serial_for_vnode(OutputDir, [log_siblings, resolve_siblings], Vnode).

local_direct_delete(Index, Bucket, Key) when
	is_integer(Index),
	is_binary(Bucket),
	is_binary(Key) ->
	DeleteReq = {riak_kv_delete_req_v1, {Bucket, Key}, make_ref()},
	riak_core_vnode_master:sync_command({Index, node()}, DeleteReq, riak_kv_vnode_master).

get_preflist_for_key(Bucket, Key, NValue) when 
	is_binary(Bucket),
	is_binary(Key),
	is_integer(NValue) ->
	BKey = {Bucket,Key},
	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
	DocIdx = riak_core_util:chash_key(BKey),
	% BucketProps = riak_core_bucket:get_bucket(Bucket, Ring), 
	% [NValue] = [Y || {X1, Y} <- BucketProps, n_val == X1],
	UpNodes = riak_core_node_watcher:nodes(riak_kv),
	Preflist = riak_core_apl:get_apl_ann(DocIdx, NValue, Ring, UpNodes),
	[IndexNode || {IndexNode, _Type} <- Preflist].


%% =================================================================================================


% Used for sorting an object's siblings in modified timestamp order (most recently modified to least)
% Duplicated from riak_kv/riak_object (since it's not exported from that module)
compare_content_dates(C1, C2) ->
	D1 = dict:fetch(<<"X-Riak-Last-Modified">>, C1#r_content.metadata),
	D2 = dict:fetch(<<"X-Riak-Last-Modified">>, C2#r_content.metadata),
	%% true if C1 was modifed later than C2
	Cmp1 = riak_core_util:compare_dates(D1, D2),
	%% true if C2 was modifed later than C1
	Cmp2 = riak_core_util:compare_dates(D2, D1),
	%% check for deleted objects
	Del1 = dict:is_key(<<"X-Riak-Deleted">>, C1#r_content.metadata),
	Del2 = dict:is_key(<<"X-Riak-Deleted">>, C2#r_content.metadata),

	SameDate = (Cmp1 =:= Cmp2),
	case {SameDate, Del1, Del2} of
		{false, _, _} ->
			Cmp1;
		{true, true, false} ->
			false;
		{true, false, true} ->
			true;
		_ ->
			%% Dates equal and either both present or both deleted, compare
			%% by opaque contents.
			C1 < C2
	end.

get_vtag(Obj) ->
	dict:fetch(<<"X-Riak-VTag">>, Obj#r_content.metadata).

is_deleted(Obj) ->
	dict:is_key(<<"X-Riak-Deleted">>, Obj#r_content.metadata).

% Loads the contents of a module (this module, usually) on every node in the cluster,
% to parallelize and cut down on inter-node disterl chatter.
load_module_on_nodes(Module, Nodes) ->
	rpc:multicall(Nodes, code, purge, [Module]),
	case code:get_object_code(Module) of
		{Module, Bin, File} ->
			{_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]);
		error ->
			error(lists:flatten(io_lib:format("unable to get_object_code(~s)", [Module])))
	end.

% Log the vtag, value and deleted status of a given sibling object
log_sibling_contents(Obj, OutputFilename) ->
	DateModified = calendar:now_to_local_time(dict:fetch(<<"X-Riak-Last-Modified">>, Obj#r_content.metadata)),
	Deleted = is_deleted(Obj),
	Vtag = get_vtag(Obj),
	Msg = io_lib:format("~p~n", [{{vtag, Vtag}, {date_modified, DateModified}, {is_deleted, Deleted}}]),
	file:write_file(OutputFilename, Msg, [append]),
	Value = Obj#r_content.value,
	file:write_file(OutputFilename, io_lib:format("~p~n", [Value]), [append]).

% Returns the last (most recent) version of the object (by timestamp)
% If the last version is a tombstone, return the
% (The list of siblings is pre-sorted by timestamp, most recent to least)
last_valid_vtag([]) ->
	{error, "No valid (non-deleted) sibling found."};
last_valid_vtag([Sibling|Rest]) ->
	case is_deleted(Sibling) of
		false ->
			{ok, {get_vtag(Sibling), Sibling}};
			% {ok, {test, test}};
		true ->
			last_valid_vtag(Rest)
	end.

resolve_object_siblings(OutputFilename, Bucket, Key, SiblingsByDate) ->
	case last_valid_vtag(SiblingsByDate) of
		{ok, {CorrectVtag, CorrectSibling}} ->
			case force_reconcile(Bucket, Key, CorrectSibling) of
				ok ->
					Msg = io_lib:format("Resolved to Vtag: ~p~n", [CorrectVtag]);
				{error, Error} ->
					Msg = io_lib:format("Error resolving to Vtag ~p :: ~p~n", [CorrectVtag, Error])
			end;
		{error, Error} ->
			Msg = io_lib:format("Error resolving siblings: ~p~n", [{Error}])
	end,
	file:write_file(OutputFilename, Msg, [append]).

force_reconcile(Bucket, Key, CorrectSibling) ->
	{ok, C} = riak:local_client(),
	{ok, OldObj} = C:get(Bucket, Key),
	NewObj = riak_object:update_metadata(riak_object:update_value(OldObj, CorrectSibling#r_content.value), CorrectSibling#r_content.metadata),
	UpdatedObj = riak_object:apply_updates(NewObj),
	C:put(UpdatedObj, all, all).  % W=all, DW=all

% Convert a serialized binary into a riak_object() record
% The function riak_object:from_binary() was introduced in 1.4, so
% we need to check for its existence and use it if possible
unserialize(_B, _K, Val = #r_object{}) ->
	Val;
unserialize(_B, _K, <<131, _Rest/binary>>=Val) ->
	binary_to_term(Val);
unserialize(Bucket, Key, Val) ->
	try
		riak_object:from_binary(Bucket, Key, Val)
	catch _:_ ->
		{error, bad_object_format}
	end.

% Log a key/bucket pair to a file
log_key(OutputFilename, Bucket, Key, Options) ->
	Msg = io_lib:format("~p,~s~n", [Bucket, binary_to_list(Key)]),
	file:write_file(OutputFilename, Msg, [append]),
	case lists:keyfind(sleep_for, 1, Options) of
		{sleep_for, SleepPeriod} ->
			timer:sleep(SleepPeriod);
		_ -> ok
	end.

% Log a key/bucket pair to a file with object sizes
size_key(OutputFilename, Bucket, Key, ObjBinary, Options) ->
	case {lists:member(raw_size, Options), lists:member(ignore_siblings, Options)} of
		{true, _} ->  % raw_size, size of the object, including siblings
			ByteSize = byte_size(ObjBinary);
		{false, true} -> % ignore_siblings, size of first value returned
			[Value|_] = riak_object:get_values(unserialize(Bucket, Key, ObjBinary)),
			ByteSize = byte_size(Value);
		{false, false} -> % neither, aggregates values for all siblings
			% Get siblings
			ByteSize = lists:foldl(fun(X,Sum) -> byte_size(X) + Sum end, 0, riak_object:get_values(unserialize(Bucket, Key, ObjBinary)))
	end,
	Msg = io_lib:format("~p,~s,~p~n", [Bucket, binary_to_list(Key), ByteSize]),
	file:write_file(OutputFilename, Msg, [append]),
	case lists:keyfind(sleep_for, 1, Options) of
		{sleep_for, SleepPeriod} ->
			timer:sleep(SleepPeriod);
		_ -> ok
	end.

% Log all siblings for a riak object (if any exist).
log_or_resolve_siblings(OutputFilename, Bucket, Key, ObjBinary, Options) ->
	Obj = unserialize(Bucket, Key, ObjBinary),
	SiblingCount = case Obj of
		{error, _Error} ->
			1;  % Error unserializing, skip the logging of the sibling count, below
		_ ->
			riak_object:value_count(Obj)
	end,

	if SiblingCount > 1 ->
		Contents = Obj#r_object.contents,
		SiblingsByDate = lists:sort(fun compare_content_dates/2, Contents),
		Msg = io_lib:format("~n~p~n", [{Bucket, Key, SiblingCount}]),
		file:write_file(OutputFilename, Msg, [append]),

		lists:foreach(fun(Sibling) -> log_sibling_contents(Sibling, OutputFilename) end, SiblingsByDate),

		case lists:member(resolve_siblings, Options) of
			true ->
				resolve_object_siblings(OutputFilename, Bucket, Key, SiblingsByDate);
			_ -> ok
		end;
	true -> ok
	end.

member_nodes() ->
	{ok, Ring} = riak_core_ring_manager:get_raw_ring(),
	riak_core_ring:all_members(Ring).

% For each node in the cluster, in parallel, load this module,
% and invoke the process_node() function on its vnodes.
process_cluster_parallel(OutputDir, Options) ->
	io:format("Scanning all nodes in parallel...~n"),
	Members = member_nodes(),
	load_module_on_nodes(?MODULE, Members),
	rpc:multicall(Members, ?MODULE, process_node, [OutputDir, Options]),
	io:format("Done.~n").

% For each node in the cluster, load this module,
% and invoke the process_node() function on its vnodes.
process_cluster_serial(OutputDir, Options) ->
	io:format("Scanning all nodes serially...~n"),
	Members = member_nodes(),
	load_module_on_nodes(?MODULE, Members),
	NodeFun = fun(Node) ->
		io:format("Processing node ~p~n", [Node]),
		rpc:call(Node, ?MODULE, process_node, [OutputDir, Options])
	end,
	lists:foreach(NodeFun, Members),
	io:format("Done.~n").

%% For per vnode operations
process_cluster_parallel_for_vnode(OutputDir, Options, Vnode) ->
	io:format("Scanning all nodes in parallel...~n"),
	Members = member_nodes(),
	load_module_on_nodes(?MODULE, Members),
	rpc:multicall(Members, ?MODULE, process_vnode_on_node, [OutputDir, Options, Vnode]),
	io:format("Done.~n").

% For each node in the cluster, load this module,
% and invoke the process_node() function on its vnodes.
process_cluster_serial_for_vnode(OutputDir, Options, Vnode) ->
	io:format("Scanning all nodes serially...~n"),
	Members = member_nodes(),
	load_module_on_nodes(?MODULE, Members),
	NodeFun = fun(Node) ->
		io:format("Processing node ~p~n", [Node]),
		rpc:call(Node, ?MODULE, process_vnode_on_node, [OutputDir, Options, Vnode])
	end,
	lists:foreach(NodeFun, Members),
	io:format("Done.~n").

% Invoked on each member node in the ring
% Calls process_vnode() on each vnode local to this node.
process_node(OutputDir, Options) ->
	{ok, Ring} = riak_core_ring_manager:get_raw_ring(),
	Owners = riak_core_ring:all_owners(Ring),
	LocalVnodes = [IdxOwner || IdxOwner={_, Owner} <- Owners,
						  Owner =:= node()],
	lists:foreach(fun(Vnode) -> process_vnode(Vnode, OutputDir, Options) end, LocalVnodes). %% TODO foreach

% Performs a riak_kv_vnode:fold(), and invokes logging functions for each key in this partition
process_vnode(Vnode, OutputDir, Options) ->
	{Partition, Node} = Vnode,
	CountsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-counts.log", [Node, Partition])]),
	SiblingsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-siblings.log", [Node, Partition])]),
	KeysFilename = filename:join(OutputDir, [io_lib:format("~s-~p-keys.log", [Node, Partition])]),
	SizesFilename = filename:join(OutputDir, [io_lib:format("~s-~p-sizes.log", [Node, Partition])]),

	FoldOptions = case proplists:get_value(bucket, Options, undefined) of
		undefined ->
			[];
		SingleBucket ->
			[{bucket, SingleBucket}]
	end,

	InitialAccumulator = dict:store(<<"BucketKeyCounts">>, dict:new(), dict:new()),
	ProcessObj = fun(BKey, ObjBinary, AccDict) ->
		{Bucket, Key} = BKey,

		case lists:member(log_keys, Options) of
			true ->
				log_key(KeysFilename, Bucket, Key, Options);
			_ -> ok
		end,

		case lists:member(size_keys, Options) of
			true ->
				size_key(SizesFilename, Bucket, Key, ObjBinary, Options);
			_ -> ok
		end,

		case lists:member(log_siblings, Options) of
			true ->
				log_or_resolve_siblings(SiblingsFilename, Bucket, Key, ObjBinary, Options);
			_ -> ok
		end,

		case lists:member(count_keys, Options) of
			true ->
				% Update per-bucket key count
				CountDict = dict:update_counter(Bucket, 1, dict:fetch(<<"BucketKeyCounts">>, AccDict)),
				dict:store(<<"BucketKeyCounts">>, CountDict, AccDict);
			_ -> AccDict
		end
	end,
	Results = riak_kv_vnode:fold(Vnode, ProcessObj, InitialAccumulator, FoldOptions),
	write_vnode_totals(CountsFilename, Results).

match_vnode(Vnode, VnodeArg, OutputDir, Options) ->
	% 
	%io:format("Comparing ~p~n", [VnodeArg]),
	%io:format("With ~p~n", [Vnode]),
	case is_element_of_tuple(Vnode, VnodeArg) of
	 	true ->
	 		io:format("Processing vnode ~p~n", [VnodeArg]),
			process_vnode(Vnode, OutputDir, Options);
		_ -> ok
	end.


% Invoked on each member node in the ring
% Calls process_vnode() on each vnode local to this node.
process_vnode_on_node(OutputDir, Options, VnodeArg) ->
	{ok, Ring} = riak_core_ring_manager:get_raw_ring(),
	Owners = riak_core_ring:all_owners(Ring),
	LocalVnodes = [IdxOwner || IdxOwner={_, Owner} <- Owners, Owner =:= node()],
	lists:foreach(fun(Vnode) -> match_vnode(Vnode, VnodeArg, OutputDir, Options) end, LocalVnodes).

	

is_element_of_tuple(Tuple, Element) ->
    lists:member(Element, tuple_to_list(Tuple)).

write_vnode_totals(OutputFilename, Results) ->
	case dict:is_key(<<"BucketKeyCounts">>, Results) of
		true ->
			Counts = dict:to_list(dict:fetch(<<"BucketKeyCounts">>, Results)),
			WriteBucketFun = fun(BucketCount) ->
				{Bucket, Count} = BucketCount,
				file:write_file(OutputFilename, io_lib:format("~p,~B~n", [Bucket, Count]), [append])
			end,
			lists:foreach(WriteBucketFun, Counts); %% TODO foreach
		_ -> ok	%% TODO return a result
	end.
