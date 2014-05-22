riak-key-list-util
==================

A Riak console utility script for per-vnode key counting, siblings logging and more. (For Riak 1.1.4)

## Usage
This script needs to be run from a Riak console, on one of the Riak nodes. 


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

   ```erl
   compile:file("/root/riak-key-list-util/key_list_util.erl").
   ```

4. You can now run the script functions. (See sections below)
   For example, `key_list_util:count_all_keys()` logs per-bucket key counts for each partition for a given node.

**Important:** Once launched from a Riak console, this script will be loaded on each node in the cluster,
*and run locally*. That is, the output files will be produced on *each node in the cluster*. They can then be 
gathered together in one location via `scp` or similar mechanisms.

## Counting keys (per bucket) and Logging Siblings
This script works by iterating over every single object, on every partition in the cluster. 
In order to avoid doing this multiple times, the functionality for counting keys and logging objects that contain siblings
is unified into one function, `key_list_util:count_all_keys(OutputDirectory)`. 

It takes a single argument, a directory to write the log files to. If you are creating this directory, make sure to do this on every node.

On the Riak console, once the script has been compiled (for this example, the log files will be created in `/tmp/`), run:

```erl
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
dev1@192.168.10.1-548063113999088594326381812268606132370974703616-counts.txt
dev1@192.168.10.1-548063113999088594326381812268606132370974703616-siblings.txt
...
```

### Bucket key counts files
These output files will be named in the form of `[node name]-[partition number]-counts.txt`. Their contents will be of the form
`{<<"bucket name">>,#keys}`, one line per bucket.

For example:

```erl
{<<"test-bucket-one">>,100}
{<<"test-bucket-two">>,150}
```

### Logs of Objects with Siblings
These will be named in the form of `[node name]-[partition number]-siblings.txt`. (If no objects with siblings are found for a particular partition,
no siblings log file will be created for that partition). The files consist of the following entries for each object, separated by a single blank line:

```erl
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

