# walbouncer-companion
A tool to perform a selective (leaving out specified databases or tablespaces) Postgres basebackup based on a Walbouncer config file

## What
Tool performs following sequence of actions:

- read the provided Walbouncer YAML config to get the master hostname/port and information on
 which tablespaces/tablespaces should not be copied over (as data changes on them will be filtered out by Walbouncer anyways)
- start to stream XLOGS's from the master using pg_receivexlog, into a temp folder. NB! pg_receivexlog needs to be on the $PATH.
- call pg_start_backup on the master
- copy data files from the master using "rsync"
- call pg_stop_backup on the master
- stop XLOG streaming (after fixed 10s delay) and move streamed XLOGs to pg_xlog folder

After that the "cloned" Postgres can be started in replication mode, following Walbouncer or actually also as a normal master
 (with errors being thrown if one accesses dabatases/tablespaces that were not copied over)

## How
Sample execution in "--dry-run" mode

```
./walbouncer_companion.py -c wbtest.yaml -r replica1 -D replica1 -u postgres -n
```

See "--help" for more
