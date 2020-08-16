# Key-Value Store

This is an academic reference implementation of a distributed in-memory data
store. It was developed as an undergraduate project in distributed systems by
Spencer Peterson, Adam Askari, Vineet Ramareddi, and Will Asper.

Key features include
 - **High availability**: Designed to accept writes in the event of failed
   nodes.
 - **Eventual causal consistency**: Database interactions will never violate the
   happens-before relationship, and replicas gossip new writes.
 - **Sharded**: Data is partitioned among shards for scalability.
 - **Replicated**: Data is replicated within shards for durability.
 - **Proxying**: Nodes transparently proxy requests as needed to load balance
   traffic.

## Build and Run

The binary can be compiled with a Go toolchain and 
```
go install github.com/spencer-p/key-value-store
```

Alternatively, a Docker image can be built with
```
docker build path/to/key-value-store -t key-value-store:latest
```

To run, the binary must be supplied with a few environment variables:

1. `ADDRESS`. The IP address this node is running on.
1. `PORT`. The TCP port to bind to.
2. `VIEW`. A comma-separted list of addresses including in the cluster.
3. `REPL_FACTOR`. The number of replicas to assign per shard (integer).

## API

The key value store exposes a CRUD API over HTTP.

Every request may supply a `causal-context` which the key value store will use
to appropriately apply requests. Every response will return the most up-to-date
`causal-context` which should be supplied in subsequent requests. 

#### Create & Update

```
PUT /kv-store/keys/x HTTP/1.1
Host: 127.0.0.1
Content-type: application/json
Content-length: 36

{"value": "1", "causal-context": {}}
```

#### Delete

```
DELETE /kv-store/keys/x HTTP/1.1
Host: 127.0.0.1
Content-length: ???
{"causal-context": {insert-context-here}}
```

#### Read

```
GET /kv-store/keys/x HTTP/1.1
Host: 127.0.0.1
Content-length: ???
{"causal-context": {insert-context-here}}
```
