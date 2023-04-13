# Devlog

- **[23-2-4] Lab1: MapReduce finished.**

- **[23-4-8] Lab2A: Raft Leader Election Finished**
  - Candidate first
  - Handle election timeout
  - Handle both RPC single timeout and RPC batch timeout (enable RPC retry)
  - Parallel RPC calls to other servers (RPC call is not locked)
  - Pass all tests for over 100 times with 3, 5 and 7 servers
  - No real log entry or log constraint
  
- **[23-4-13] Lab2B: Raft Log Replication Finished**
  
  - Simple implementation based on Raft paper, especially on Figure 2
  
  - Slow Backup without follower's reply for nextIndex for AE
  
  - Reduce the number of RPC calls
  
  - Pass all tests for over 1500 times

