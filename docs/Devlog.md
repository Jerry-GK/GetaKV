- [23-2-4] Lab1: MapReduce finished.

- [23-4-8] Lab2A: Raft Leader Election Finished
  - Candidate first
  - Handle election timeout
  - Handle both RPC single timeout and RPC batch timeout (enable RPC retry)
  - Parallel RPC calls to other servers (RPC call is not locked)
  - Pass test for over 100 times with 3, 5 and 7 servers
  - No real log entry or log constraint