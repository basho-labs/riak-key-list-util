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

-module(key_list_util).
-compile(export_all).

% Describes the Contents of a Riak object. A "sibling" is an instance of this record.
% Duplicated from riak_kv/riak_object (riak version 1.1.2), since it's needed by compare_content_dates()
-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

% Describes a Riak Object (as of 1.1.2)
% Duplicated from riak_kv/riak_object (riak version 1.1.2), since it's needed by compare_content_dates()
-record(r_object, {
          bucket :: riak_object:bucket(),
          key :: riak_object:key(),
          contents :: [#r_content{}],
          vclock = vclock:fresh() :: vclock:vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

count_all_keys(OutputDir) ->
	process_cluster(OutputDir).

% Used for sorting an object's siblings in modified timestamp order (most recently modified to least)
% Duplicated from riak_kv/riak_object (riak version 1.1.2) (since it's not exported from that module)
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

% Loads the contents of a module (this module, usually) on every node in the cluster,
% to parallelize and cut down on inter-node disterl chatter.
load_module_on_nodes(Module, Nodes) ->
	rpc:multicall(Nodes, code, purge, [Module]),
	case code:get_object_code(Module) of
		{Module, Bin, File} ->
			{_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]);
	error ->
		error(lists:flatten(io_lib:format("unable to get_object_code(~s)", [Module])))
	end,
	ok.

% log_large_objects(OutputFilename, Threshold) ->
% 	Scan = fun(BKey, Val, Acc) ->
% 		Size = byte_size(Val),
% 		if Size >= Threshold ->
% 				Msg = io_lib:format("~p :: ~p bytes~n", [BKey, Size]),
% 				file:write_file(OutputFilename, Msg, [append]);
% 		true -> ok
% 		end,
% 		Acc
% 	end,
% 	InitialAccumulator = [],
% 	{Scan, InitialAccumulator}.

% Log the vtag, value and deleted status of a given sibling object
log_sibling_contents(Obj, OutputFilename) ->
	Metadata = Obj#r_content.metadata,
	DateModified = calendar:now_to_local_time(dict:fetch(<<"X-Riak-Last-Modified">>, Metadata)),
	Deleted = dict:is_key(<<"X-Riak-Deleted">>, Metadata),
	Vtag = dict:fetch(<<"X-Riak-VTag">>, Metadata),
	Msg = io_lib:format("~p~n", [{{vtag, Vtag}, {date_modified, DateModified}, {is_deleted, Deleted}}]),
	% Msg = io_lib:format("~p~n", [Metadata]),
	file:write_file(OutputFilename, Msg, [append]),
	Value = Obj#r_content.value,
	file:write_file(OutputFilename, io_lib:format("~p~n", [Value]), [append]).

% Log all siblings for a riak object (if any exist)
log_siblings(OutputFilename, Bucket, Key, Val) ->
	Obj = binary_to_term(Val),
	SiblingCount = riak_object:value_count(Obj),

	if SiblingCount > 1 ->
		Contents = Obj#r_object.contents,
		SiblingsByDate = lists:sort(fun compare_content_dates/2, Contents),
		Msg = io_lib:format("~n~p~n", [{Bucket, Key, SiblingCount}]),
		file:write_file(OutputFilename, Msg, [append]),
		lists:foreach(fun(Sibling) -> log_sibling_contents(Sibling, OutputFilename) end, SiblingsByDate);
	true -> ok
	end,
	ok.

% For each node in the cluster, load this module, 
% and invoke the process_node() function on its vnodes.
process_cluster(OutputDir) ->
	io:format("Scanning cluster...~n"),
	{ok, Ring} = riak_core_ring_manager:get_raw_ring(),
	Members = riak_core_ring:all_members(Ring),
	load_module_on_nodes(?MODULE, Members),
	rpc:multicall(Members, ?MODULE, process_node, [OutputDir]),
	% process_node(OutputDir),
	Msg = "Done.~n",
	io:format(Msg),
	ok.

% Invoked on each member node in the ring
% Calls process_vnode() on each vnode local to this node.
process_node(OutputDir) ->
	{ok, Ring} = riak_core_ring_manager:get_raw_ring(),
	Owners = riak_core_ring:all_owners(Ring),
	LocalVnodes = [IdxOwner || IdxOwner={_, Owner} <- Owners,
	 					 Owner =:= node()],
	lists:foreach(fun(Vnode) -> process_vnode(Vnode, OutputDir) end, LocalVnodes),
	ok.

% Performs a riak_kv_vnode:fold(), and invokes logging functions for each key in this partition
process_vnode(Vnode, OutputDir) ->
	{Partition, Node} = Vnode,
	CountsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-counts.txt", [Node, Partition])]),
	SiblingsFilename = filename:join(OutputDir, [io_lib:format("~s-~p-siblings.txt", [Node, Partition])]),

	InitialAccumulator = dict:store(<<"BucketKeyCounts">>, dict:new(), dict:new()),
	ProcessObj = fun(BKey, Contents, AccDict) ->
		{Bucket, Key} = BKey,
		log_siblings(SiblingsFilename, Bucket, Key, Contents),
		% Update per-bucket key count
		CountDict = dict:update_counter(Bucket, 1, dict:fetch(<<"BucketKeyCounts">>, AccDict)),
		dict:store(<<"BucketKeyCounts">>, CountDict, AccDict)
	end,
	Results = riak_kv_vnode:fold(Vnode, ProcessObj, InitialAccumulator),
	write_vnode_totals(CountsFilename, Results),
	ok.

write_vnode_totals(OutputFilename, Results) ->
	case dict:is_key(<<"BucketKeyCounts">>, Results) of
		true ->
			Counts = dict:to_list(dict:fetch(<<"BucketKeyCounts">>, Results)),
			lists:foreach(fun(BucketCount) -> file:write_file(OutputFilename, io_lib:format("~p~n", [BucketCount]), [append]) end, Counts);
		_ -> ok
	end,
	ok.