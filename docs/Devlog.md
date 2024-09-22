# Devlog

- **[23-2-4] Lab1: MapReduce Finished.**

- **[23-4-8] Lab2A: Raft Leader Election Finished**

  - Candidate first
  - Handle election timeout
  - RPC Caller, Handle both RPC single timeout and RPC batch timeout (enable RPC retry)
  - Parallel RPC calls to other servers (RPC call is not locked)
  - Pass all tests for over 100 times with 3, 5 and 7 servers
  - No real log entry or log constraint
  
- **[23-4-13] Lab2B: Raft Log Replication Finished**

  - Simple implementation based on Raft paper, especially on Figure 2
  - Slow Backup without follower's reply for nextIndex for AE
  - Reduce the number of RPC calls (partially)
  - Delay mechanism for AE Caller failure
  - Pass all tests for over 1500 times
  
- **[23-4-13] Lab2C: Raft Persistent States**

  - Unified method for modifying persistent states, which calls persist()
  - RPC number is pretty big
  - Pass all tests for over 600 times
  
- **[23-4-18] Lab3A: KV Raft without Log Compaction**

  - Increasing MsgId of client request to avoid duplicated apply
  - Wrapped Op struct to go through Raft module and maintain consistentency
  - Fix bugs for misdirected output channel due to same opId between different servers
  - Pass all tests
  
- **[23-4-20] Lab3B: KV Raft with Log Compaction**

  - Send InstallSnapshot RPC(parallel) only if leader tries to send deleted entries to a server when AE
  - A complex index mechanism: bias for starting from 1, bias for snapshot's lastIncludeIndex (0 means no snapshot)

    No useless empty placeholder entry at logEntries[0], good for AE
  - Fix the bug for non-stop InstallSnapshot RPC if the server is killed
  - Pass all tests, with potential bugs
  
- **[23-4-21]  Add single node KV and parser**

- **[23-4-22]  Add file storage for persister**

- **[24-8-6]  Lab4A: Shard Master**
  
  - Reconfiguration algorithm
  - Use raft module
  - Pass all tests
  
- **[24-9-22]  Lab4B: Sharded KV **

  - A consistent machanism for process reconfig one by one
  - Internal request for server state access
  - Check and fix bugs about restart
  - Resolve challenges: delete unused shards, unaffected access, partial access
  - Pass all tests (occasional bugs about inconsistency data in restart test)
