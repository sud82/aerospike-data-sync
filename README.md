# aerospike-cluster-synchronizer
In multi-DC HA setup there is a need to be able to find out if data in multiple
DC are in sync. Aerospike Synchronizer is a tool to find data which is not in
sync in multiple Aerospike Clusters running across multiple DC connected in
various topology using Aerospike XDR.

This tool starts scanning of data from one cluster (origin) and checks if it
matches with other cluster (destination).
if it doesn't match it dumps mismatched keys info in given file.

Two records are considered synchronized when their data (bins) are equivalent. 

```
options:
-sh                 Server host for source cluster (IP:Port)
-su                 Username for source cluster (Cluster username)
-sp                 Password for source cluster (Cluster password)
-dh                 Server host for destination cluster (IP:Port)
-du                 Username for destination cluster (Cluster username)
-dp                 Password for destination cluster (Cluster password)
-n                  Namespace
-s                  Set (Default: All sets in given Namespace)
-b                  Bin list: bin1,bin2,bin3... (Default: Full record)
-A                  Time after which records modified. eg: Jan 2, 2006 at 3:04pm (MST)
-B                  Time before which records modified. eg: Jan 2, 2006 at 3:04pm (MST)
-o                  Output File to log records to be synced.
-P                  The scan priority. 0 (auto), 1(low), 2 (medium), 3 (high). Default: 0.
-p                  Sample percentage. (Default: 10)
-ss                 Sample size. if sample percentage given, it won't work. Default: 1000
-r                  Remove existing record sync log files
-ll                 Set log level, DEBUG(0), INFO(1), WARNING(2), ERR(3), Default: INFO
-u                  Print usage.


-su,sp,du,dp        used only when cluster needs access credentials
```

Some sample arguments are:

```
./syncCluster -sh 127.0.0.1:3000 -dh 127.0.0.0:3009 -n test -s testset -P 2 p 10
-A "Mar 31, 2017 at 11:01pm (PDT)" -o rec_sync.log -r

\# Source cluster is 127.0.0.1:3000, Destination cluster is 127.0.0.0:3009 and 
\# Check 10% (p) of the records from Namespace test and Set testset. Scan with
priority(P 2)
\# Only check records updated after "Mar 31, 2017 at 11:01pm (PDT)"
\# Save digest info of unsync records in rec_sync.log file.

```

Note:  Only supported on Aerospike 3.X and above

