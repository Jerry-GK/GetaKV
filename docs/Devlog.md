- MapReduce finished.

- Lab2A: Raft Leader Election
  - Candidate first
  - Handle election timeout
  - Handle both RPC single timeout and RPC batch timeout
  - Internal lock for parallel goroutines
  - Pass test for over 100 times
  - No real log entry and log constraint