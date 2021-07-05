Nidus is a Python implementation of the [Raft Consensus Algorithm](https://raft.github.io) for a simple key value store


> This is a WIP

Usage:

1. Update the config.json with your cluster configuration.
2. Start up each node in the cluster (different process or machine) `python -m nidus --config=config.json <node-name>`
3. Send commands to the leader `python -m nidus --leader=localhost:12000 SET hello world`
