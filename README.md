# Nidus

Nidus is a distributed, durable, fault-tolerant, key value store based on the [Raft Consensus Algorithm](https://raft.github.io). Key value pairs are segmented into namespaces called buckets.

![](https://cdn.zappy.app/7739dd7e6e272b9dd71f8eb570482fb6.png)


## Quick Start

1. Update the config.json with your cluster configuration.
2. Start up each node in the cluster (different process or machine) `python -m nidus --config=config.json <node-name>`
   where `node-name` is the name of a node defined in your config.
3. Start sending commands to the leader `python -m nidus --leader=localhost:12000 SET <bucket> <key> <value>`.

For a full list of supported commands please see the command reference below.


## Supported Raft Features

- [x] Leader election
- [x] Log replication
- [x] Persistence to stable storage (disk)
- [ ] Log compaction (snapshots)
- [ ] Dynamic config changes


## Command Reference

Command | Arguments | Description | Example
--------|-----------|-------------|--------
`SET` | `<bucket>`, `<key>`, `<value>` | Sets a key with value in the bucket. This command will implictly create the bucket. Returns `OK` on success. | `SET fruits apples 3`
`GET` | `<bucket>`, `<key>` | Gets the value for key stored in the bucket. Returns the stored value on success or `None`. | `GET fruits apples`
`DEL` | `<bucket>`, `<key>` | Deletes a key and associated value from the bucket. Returns `OK` on success or `NO_KEY` if the key doesn't exist. | `DEL fruits apples`
`KEYS` | `<bucket>` | Lists all the keys in the bucket. | `KEYS fruits`
`DELBUCKET` | `<bucket>` | Deletes a bucket and all key value pairs associated with it. Returns `OK` on success or `NO_BUCKET` if the bucket doesn't exist. | `DELBUCKET fruits`
`BUCKETS` | | Lists all the buckets that exist. | `BUCKETS`

Providing an invalid command will result in a return value of `BAD_CMD`.
Providing an invalid number of arguments will result in a return value of `BAD_ARGS`.
