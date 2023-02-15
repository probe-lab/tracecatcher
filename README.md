# tracecatcher

TraceCatcher is a utility that listens to pubsub traces produced by [Lotus](https://github.com/filecoin-project/lotus) and records them in a Postgresql database. 

## Overview

TraceCatcher can be run alongside a Lotus node to capture pubsub traces. 
It emulates the ElasticSearch API expected by Lotus and writes each trace to a Postgresql database. 
Each event type is recorded in a separate table.

The following trace events are supported:

| Event type         | Database tables                    |
| ------------------ | ---------------------------------- |
| publish_message    | publish_message_event              |
| reject_message     | reject_message_event               |
| duplicate_message  | duplicate_message_event            |
| deliver_message    | deliver_message_event              |
| add_peer           | add_peer_event                     |
| remove_peer        | remove_peer_event                  |
| join               | join_event                         |
| leave              | leave_event                        |
| graft              | graft_event                        |
| prune              | prune_event                        |
| peer_score         | peer_score_event, peer_score_topic |   

The following event types are not yet supported:

 - recv_rpv
 - send_rpc
 - drop_rpc

## Getting Started

As of Go 1.19, install the latest tracecatcher executable using:

	go install github.com/iand/tracecatcher@latest

This will download and build an ipfsfiled binary in `$GOBIN`

Run the daemon by executing `$GOBIN/tracecatcher` and use command line options to configure its operation:

 - `--addr` - the address to listen for traces on (default: ":5151")
 - `--db-host` - hostname/address of the database server in which to write traces
 - `--db-port` - port number of the database server (default: 5432)
 - `--db-name` - name of the database to use
 - `--db-password` - password to use when connecting the the database
 - `--db-user`- user to use when connecting the the database
 - `--db-sslmode` - sslmode to use when connecting the the database
 - `--batch-size` - set the size of query batches to use when inserting into the database (default: 100)

Run `$GOBIN/tracecatcher --help` to see the full list of options. 
Each option may also be set using environment variables. These are shown in the help.

### Setting up Postgresql

Ensure that TraceCatcher is supplied with a user that has permissions to create tables and indexes.
When TraceCatcher runs it creates the necessary tables for each event type.

### Configuring Lotus

Lotus has two configuration settings that control the destination of pubsub traces. 
Set `ElasticSearchTracer` to the address of TraceCatcher. 
`ElasticSearchIndex` must be set to `traces`.

```toml
[Pubsub]
ElasticSearchTracer = "localhost:5151"
ElasticSearchIndex = "traces"
```

These settings can also be specified using environment variables:

```
LOTUS_PUBSUB_ELASTICSEARCHTRACER=localhost:5151
LOTUS_PUBSUB_ELASTICSEARCHINDEX=traces
```

## License

This software is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](https://github.com/iand/tracecatcher/blob/master/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](https://github.com/fiand/tracecatcher/blob/master/LICENSE-MIT) or http://opensource.org/licenses/MIT)





