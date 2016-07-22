# walbouncer-companion
A tool to perform a selective Postgres basebackup based on a Walbouncer config file

## What
Tool performs following sequence of actions:

- read the provided Walbouncer YAML config to get the master hostname/port and information which tablespaces/tablespaces should not be copied over (as data changes on them will be filtered out by Walbouncer anyways)
- start to stream XLOGS's from the master (pg_receivexlog)
- call pg_start_backup on the master
- copy data files from
- call pg_stop_backup on the master
- stop XLOG streaming and move streamed XLOGs to pg_xlog folder

 ## How
 Sample execution in "--dry-run" mode

 ```
    ./walbouncer_companion.py -c wbtest.yaml -r replica1 -D replica1 -u postgres -n
 ```

 See "--help" for more
