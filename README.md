riak-key-list-util
==================

A Riak console utility script for per-vnode key counting, siblings logging and more. (For Riak 1.1.4 and above)

## Usage
This script needs to be run from a Riak console, on one of the Riak nodes. All nodes must be up and ready to accept requests or the script will fail. Every run of the script will append its output to the output of any previous run, so it’s a good idea to clear out the previous results.


1. Clone this repository

    ```
    git clone https://github.com/basho-labs/riak-key-list-util.git
    ```

2. Attach to a Riak console, of a running node. (Don't forget to detach via `CTRL+D`, when you're done). 
   (You can hit Enter so that a `>` prompt appears).

    ```
    riak attach
    ```

3. From the console, load and compile the `key_list_util.erl` script. 
   Be sure to use the full absolute path to the file. Avoid `~/` shortcuts for user home directories.
   For example, assuming that the repository above was cloned into `/root/`, do the following at the Riak console:

   ```erlang
   compile:file("/root/riak-key-list-util/key_list_util.erl").
   ```

4. You can now run the script functions. (See sections below)
   For example, `key_list_util:count_all_keys()` logs per-bucket key counts for each partition for a given node.

**Important:** Once launched from a Riak console, this script will be loaded on each node in the cluster,
*and run locally*. That is, the output files will be produced on *each node in the cluster*. They can then be 
gathered together in one location via `scp` or similar mechanisms.

## Counting keys (per bucket) and Logging Siblings

This script works by iterating over every object in every partition, on a single partition, or on a single bucket. In order to avoid doing this multiple times, the functionality for counting keys and logging objects that contain siblings is unified into the same function.  The three variations of the function are:

```erlang
key_list_util:count_all_keys(OutputDirectory).
key_list_util:count_all_keys_for_bucket(OutputDirectory, Bucket).
key_list_util:count_all_keys_for_vnode(OutputDirectory, Vnode).
```

Example usage:

```erlang
key_list_util:count_all_keys("/tmp/").
key_list_util:count_all_keys_for_bucket("/tmp/", test).
key_list_util:count_all_keys_for_vnode("/tmp/", 1438665674247607560106752257205091097473808596992).
```

On the Riak console, once the script has been compiled (for this example, the log files will be created in `/tmp/`), run:

```erlang
key_list_util:count_all_keys("/tmp/").
```

The output should be:

```
Scanning cluster...
Done.
ok
```

Outside of the Riak console, you should now be able to see the resulting log files, for each partition on a given node.

```
ls /tmp/
...
dev1@192.168.10.1-548063113999088594326381812268606132370974703616-counts.log
dev1@192.168.10.1-548063113999088594326381812268606132370974703616-siblings.log
...
```

#### Using Key Counts for Verification
The per-bucket key count logs discussed above can be used for data verification, for example to compare key counts before and after an upgrade.

The following commands have to be run on *each* Riak node. (The example assumes that the keys were logged to `/tmp/v1/counts/` directory before upgrade, and to `/tmp/v2/counts/` after upgrade):

1. Sort the log files in the output directories:

    ```bash
    for file in /tmp/v1/counts/*-counts.log; do sort -o $file $file; done
    for file in /tmp/v2/counts/*-counts.log; do sort -o $file $file; done
    ```

2. Use `diff` to compare the "before" and "after" directories. `diff` will return no results if the directories are the same.

    ```bash
    diff -r /tmp/v1/counts /tmp/v2/counts
    ```

The sorted files should be identical before and after an upgrade. If not, the `diff` command will list any differences in any of the log files.

### Bucket key counts files
These output files will be named in the form of `[node name]-[partition number]-counts.log`. Their contents will be in CSV format
`"bucket name",#keys`, one line per bucket.

For example:

```erlang
"test-bucket-one",100
"test-bucket-two",150
```

## Logging All Keys
While per-bucket key counts will suffice for data verification before and after Riak upgrades, some operations require more stringent 
(and more time-intensive) verification methods. For data verification before and after *ring resizing*, a full log of buckets and keys
is required.

To log all bucket/key combinations (this will generate a log of keys for each partition on each node):

```erlang
key_list_util:log_all_keys("/tmp/").
```

The output should be:

```
Scanning cluster...
Done.
ok
```

Outside of the Riak console, you should now be able to see the resulting log files, for each partition on a given node.

```
ls /tmp/
...
dev1@192.168.10.1-548063113999088594326381812268606132370974703616-keys.log
...
```

Their contents will be in the CSV format, `"bucket name","key"`, one line per key.

For example:

```erlang
"test-bucket-one","key2"
"test-bucket-one","key1"
"test-bucket-two","key100"
```

As with key counting, this function also has two alternatives which take two arguments.  One which executes on a single bucket: `key_list_util:log_all_keys_for_bucket(OutputDirectory, Bucket).` Bucket is either a bucket name binary, or a {BucketType, Bucket} pair of binaries. The second option is to run per vnode (partition): `key_list_util:log_all_keys_for_vnode(OutputDirectory, Vnode)`.  


#### Using Key Logs for Verification
The keylogs generated by `log_all_keys()` are most useful when sorted. The standard Unix `sort` command can help with sorting the key logs,
merging them all into one file, removing duplicates (so that only one entry will exist for each key/bucket combination, rather than `N` entries),
and also splitting the merged file into smaller files for easier processing.

The following commands have to be run on *each* Riak node. (The example assumes that the keys were logged to `/tmp/v1/keys/` directory before upgrade):

1. Sort the key log files in the output directories:

    ```bash
    for file in /tmp/v1/keys/*-keys.log; do sort -u -o $file $file; done
    ```

2. You can then merge the files (`sort -m`), remove duplicates (`-u`), and optionally `split` the resulting huge file into more manageable 
chunks (of 500,000 lines each). 

    ```bash
    sort -m -u /tmp/v1/keys/*-keys.log | split -l 500000 /tmp/v1/keys/node1-sorted-keys- 
    ```

3. The split chunked files will be named `..-aa`, `..-ab` and so on:

    ```bash
    ls /tmp/v1/keys/
    ...
    node1-sorted-keys-aa
    node1-sorted-keys-ab
    node1-sorted-keys-ac
    ...
    ```

    Note the use of `node1` in the split filenames template, this will help disambiguate the key files when they're copied from
    all nodes into a central location.

4. The sorted, de-duped and split files can now be copied from the various nodes to a central location, for processing, using `scp`:

    ```bash
    scp root@192.168.10.1:/tmp/v1/keys/*-sorted-keys* /tmp/whole-cluster-v1/
    # .. repeat for each riak node
    ```

#### Processing and Verification of Sorted Key Logs
Once the sorted key logs are copied to a central location via `scp` (see section above):

1. The files, copied from all of the nodes in the cluster, need to be merged and de-duplicated again, before they are useful for verification:

    ```bash
    sort -m -u /tmp/all-nodes-v1/*-sorted-keys* | split -l 500000 /tmp/cluster-v1/all-sorted-keys-
    ```

2. The resulting `cluster-v1` directory can then be used for data verification with the `diff` command:

    ```bash
    diff /tmp/cluster-v1/ /tmp/cluster-v2/
    ```

### Finding size of all keys
Finding the size of all keys works similarly to listing all keys. You can iterate over all partitions in the cluster, a single partition, or a single bucket. The six variations of the function are:

```
size_all_keys(OutputDir).
size_all_keys(OutputDir, Options).
size_all_keys_for_bucket(OutputDir, Bucket).
size_all_keys_for_bucket(OutputDir, Bucket, Options).
size_all_keys_for_vnode(OutputDir, Vnode).
size_all_keys_for_vnode(OutputDir, Vnode, Options).
```

The output will appear in the form of `[node name]-[partition number]-sizes.log`. These functions can accept options as a list. If no options are required, you may leave off the argument. The `raw_size` option controls whether to return the whole object with metadata, or just the size of values. The `ignore_siblings` option controls whether to consider siblings in the calculations, or return the size of the first value (or an estimate of all the objects divided by the number of siblings). These two options are mutually exclusive. If both are provided, `raw_size` will take precedence, and the script will silently drop `ignore_siblings`.

For example:

```erlang
key_list_util:size_all_keys("/tmp/").
```

Print out full object size, including any siblings:

```erlang
key_list_util:size_all_keys("/tmp/", [raw_size]).
```

The `size_all_keys()` function requires key duplication similar to `log_all_keys()` above. Its output adds a size of objects in bytes to the end of the `log_all_keys()` output.

### Logs of Objects with Siblings
These will be named in the form of `[node name]-[partition number]-siblings.log`. (If no objects with siblings are found for a particular partition,
no siblings log file will be created for that partition). The files consist of the following entries for each object, separated by a single blank line:

```erlang
{<<"bucket name">>,<<"object key">>,2}
{{vtag,"6oEMJzrPKX6gPgPYm9s2pt"},
 {date_modified,{{2014,5,14},{11,6,30}}},
 {is_deleted,false}}
<<"sibling value 2">>
{{vtag,"6ZPSo6ATuiQohGuaBYsdWq"},
 {date_modified,{{2014,5,14},{11,6,25}}},
 {is_deleted,false}}
<<"sibling value 1">>
```

The siblings (each with their own `vtag`), will be sorted by timestamp, most recent to least. (The `2` following the bucket name and key in the example above denotes the number of siblings for that object).

#### Note on Tombstones
In the sibling logs above, tombstones will be denoted by `{is_deleted,true}`.

## Sibling Resolution
To force-reconcile all siblings on the cluster, use the `resolve_all_siblings()` function. As with the key counting function above,
the script will run on every node in the cluster (sequentially, this time).

On the Riak console, once the script has been compiled (for this example, the log files will be created in `/tmp/`), run:

```erlang
key_list_util:resolve_all_siblings("/tmp/").
```

The output files this function produces will be similar to the `*-siblings.log` files above, with the addition of a 
`Resolved to Vtag:` line at the end of each object listing. For example:

```erlang
{<<"bucket name">>,<<"object key">>,2}
{{vtag,"6oEMJzrPKX6gPgPYm9s2pt"},
 {date_modified,{{2014,5,14},{11,6,30}}},
 {is_deleted,false}}
<<"sibling value 2">>
{{vtag,"6ZPSo6ATuiQohGuaBYsdWq"},
 {date_modified,{{2014,5,14},{11,6,25}}},
 {is_deleted,false}}
<<"sibling value 1">>
Resolved to Vtag: "6oEMJzrPKX6gPgPYm9s2pt"
```

Note: There will be fewer `*-siblings.log` entries than during key counting and sibling logging. Because the sibling resolution
runs sequentially, once a sibling is resolved for one node, this also resolves all 3 replicas, and so the resolved objects will not be
encountered on other nodes.

As with the other functions in this utility, this function also has two alternatives.  The first two-argument version resolves siblings for only a single bucket: `key_list_util:resolve_all_siblings_for_bucket(OutputDirectory, Bucket).` Here, Bucket is either a bucket name binary, or a {BucketType, Bucket} pair of binaries.  The second option is to run per vnode (partition): `key_list_util:resolve_all_keys_for_vnode(OutputDirectory, Vnode).` 

<br>

## Direct Deletes

Rarely, you will come upon an object that has been turned into a tombstone, but has not been reaped. This can happen due to the distributed nature of the database coupled with it being an AP, rather than CP store. Also, the erlang timers that schedule the reaps can occasionally fail for other reasons.

Whatever the case, the following function is there for use in such a case:

```
local_direct_delete(Index, Bucket, Key)
```

Here, **Index** is an integer representing the vnode or partition that the tombstone is on. **Bucket** and **Key** are binaries, as described above, and look like *\<\<"bucketOrKeyName"\>\>*.

<br>

## Preflists

It can be useful to get the preflist, up to a specified n-val, for a given object identifier. One example of use is to determine whether an object has been orphaned at an n-val higher than that in the bucket config. This might occur due to a previous lowering of n-val for that bucket.

```
get_preflist_for_key(Bucket, Key, NValue)
```

Here, **Bucket** and **Key** are binaries as above. **NValue** is an integer representing the length of the preflist you want returned.

***Please note*** that results will differ for different ring sizes. So make sure you test in the same cluster you are working with, or one with the same ring size. Also, NValue cannot be higher than the ringsize of the cluster you are attached to; if it is, the function will fail with an error.

<br>

\~
