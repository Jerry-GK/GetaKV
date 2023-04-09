

# Issues

## lab1



## lab2

### Lab2A

场景P：一切正常，所有server工作正常、term都跟leader一样为1时，假如有一个server与leader通信出错或超时、没有对RPC-APPENDENTRIES响应(但跟其他大部分但不是全部follower通信正常)，于是它term变成了2，成为candidate。假设此时又跟leader恢复了通信。

- 问题1: P场景下，candidate向leader发起的RPC-REQUESTVOTE，leader接收到了term更大的请求，是否需要马上变成follower？注意此时leader才是跟所有server通信正常的，candidate只能跟一部分通信，理论上应该让leader继续当leader。

- 问题2: 如果问题1的答案是肯定的，那么leader(或者candidate)变为follower后是否马上需要给该candidate投票？还是reply false然后公平接受所有candidate的投票请求？

    答案: leader(或candidate)变成follower后，必须马上给让它变成follower的那个candidate投票。

- 问题3: P场景下，如果candidate刚完成状态转化，就马上收到了leader之前没发送成功的RPC-APPENDENTRIES，该RPC请求的term比自身term低，那么candidate应该认为存在leader、自己放弃选举、变成follower(term降低)，还是拒绝请求、reply false？

- 问题4: P场景下，在问题3中，candidate肯定会回复leader的RPC-APPENDENTRIES，那么leader在接收到后，如果发现reply的term比自己大，那么应不应该变成follower(这其实跟问题2类似)

    注意P场景问题1,3,4其实整体只有两种策略

    - 策略1: candidate优先。leader如果收到新candidate的RPC请求或者回复，都会马上变成follower、与candidate同步term。candidate收到旧leader的请求不会理会。
    - 策略2: leader优先。leader永远拒绝投票、拒绝被candidate的请求或回复给变成follower。leader给candidate发送的RPC-APPENDENTRIES即使term要低，也能让candidate强行认leader、变成follower。只有当该candidate竞选成功变成新leader、给旧leader发送RPC-APPENDENTRIES时，旧leader才会变成follower。

    最终决定策略1(这才是Raft的选择，但不知道leader优先策略有什么问题。candidate优先是否可能导致选举和leader更替太频繁？)

    

- 问题5: 如果leader接收到来自另一个leader的RPC-APPENDENTRIES，会发生什么？(比如一个leader经过一段时间从failure中回复，这段时间内生成的新leader给该leader发生了RPC-APPENDENTRIES)

    答案: 相对明显，无论是哪种策略，都是如果接受方发现自己term低，就同步term、变成follower。

- 问题6: 如果选举期间发生了split vote，candidate会变成follower，经过随机事件后重新发起选举。那么在candidate变成follower的时候，需不需要将自身term恢复(降低，减一)？还是保持原来的term？

    答案: 竞选失败，term不需要降低。？


- 问题7: 当Leader向其他server广播AppendEntries，或Candidate向其他server广播RequestVote时，是否需要等待这些广播全部返回结果？即使可能存在RPC超时？

    答案：不需要。否则严重影响性能。另外注意RPC在Call时不能锁住整个Raft结构，否则不会并行、性能极差。

- 问题8: 是否可能同时存在两个term相等的Leader？

    答案：Raft中不可能存在term相等的Leader。Leader确实可能有多个，但只有一个最新的、有效的Leader。如果Leader收到了另一个term相等的Leader发来的AppendEntries的RPC请求，则认为出现了异常。

- 问题9: 是否有必要对失败或超时的RPC尝试重新请求？这样同一server可能收到两次相同的rpc请求，会不会有问题？

    答案: 可以进行，采用single和batch两种timeout。接收方应该忽略重复的请求。

- 问题10: 什么时候会resetHeartBeatTimer?什么时候会resetElectionTimer?

    答案: 分别是以下情况。
    - resetHeartBeatTimer的情况
      1. server初始化时
      2. HeartBeatTimer触发时
      另外注意设置当server变成Leader时，马上尝试触发HeartBeatTimer(即设很短的间隔，方便马上发送心跳信息)
      
    - resetElectionTimer的情况
      1. server初始化时
      2. Follower在接收到term相等的Candidate的RequestVote RPC请求、并同意投票时
      3. 任何server在发生状态转化时。这包括：
      
        - Follower变成Candidate，发起新选举时（这里是否无所谓？）
        - Candidate竞选成功变成Leader时（这里其实不reset也无所谓）
        - Candidate竞选失败或收到更高term的RPC，变成Follower时（这里必须reset，否则将马上再去竞选）
        - Leader收到更高term的RPC，变成Follower时
        - Follower接收到term相等的来自Leader的RPC-AppendEntries时、“转变”为Follower时（这可能是最常见的情况）
        - Follower接收到term更高的RPC(比如来自新任Leader，或Candidate)、更新term并“转变”为Follower时
      
        