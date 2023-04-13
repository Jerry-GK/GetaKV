

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

    - 策略1: **candidate优先**。leader如果收到新candidate的RPC请求或者回复，都会马上变成follower、与candidate同步term。candidate收到旧leader的请求不会理会。
    - 策略2: leader优先。leader永远拒绝投票、拒绝被candidate的请求或回复给变成follower。leader给candidate发送的RPC-APPENDENTRIES即使term要低，也能让candidate强行认leader、变成follower。只有当该candidate竞选成功变成新leader、给旧leader发送RPC-APPENDENTRIES时，旧leader才会变成follower。

    最终决定策略1(这才是Raft的选择，这可能是整个Raft的一个重要基础。但能不能优化成leader优先？candidate优先是否可能导致选举和leader更替太频繁？)

    

- 问题5: 如果leader接收到来自另一个leader的RPC-APPENDENTRIES，会发生什么？(比如一个leader经过一段时间从failure中回复，这段时间内生成的新leader给该leader发生了RPC-APPENDENTRIES)

    答案: 相对明显，无论是哪种策略，都是如果接受方发现自己term低，就同步term、变成follower。

- 问题6: 如果选举期间发生了split vote，candidate会变成follower，经过随机事件后重新发起选举。那么在candidate变成follower的时候，需不需要将自身term恢复(降低，减一)？还是保持原来的term？

    答案: **竞选失败，term不需要降低、恢复到原值**。


- 问题7: 当Leader向其他server广播AppendEntries，或Candidate向其他server广播RequestVote时，是否需要等待这些广播全部返回结果？即使可能存在RPC超时？

    答案：不需要。否则严重影响性能。另外注意RPC在Call时不能锁住整个Raft结构，否则不会并行、性能极差。

- 问题8: 是否可能同时存在两个term相等的Leader？

    答案：**Raft中不可能存在term相等的Leader**。Leader确实可能有多个，但只有一个最新的、有效的Leader。如果Leader收到了另一个term相等的Leader发来的AppendEntries的RPC请求，则认为出现了异常。

- 问题9: 是否有必要对失败或超时的RPC尝试重新请求？这样同一server可能收到两次相同的rpc请求，会不会有问题？

    答案: 可以进行，采用single和batch两种timeout。接收方应该忽略重复的请求。这样可能增大RPC的数量。

- 问题10: voteFor字段有什么特点？

    答案：voteFor字段表示当前term下投出的票，如果没投过，则为-1(无效值)。**同一个term内voteFor只能有过最多一个有效值**，这是保证同一个term最多一个leader的基础。voteFor初始化为-1，在首次投票时设为RV发送者(Candidate)的ID，并且在同一term内不会再改变；voteFor只有在changeState的过程中**发现term增加时**才会重置为-1。不可随意重置。

- 问题11: 什么时候会resetHeartBeatTimer?什么时候会resetElectionTimer?

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
      
        

### Lab2B

- 问题1:  如何理解matchIndex和nexrIndex的关系？matchIndex[i] = nextIndex[i] - 1什么时候才成立？

    答案：（直接引用guide中的解答）

    One common source of confusion is the difference between `nextIndex` and `matchIndex`. In particular, you may observe that `matchIndex = nextIndex - 1`, and simply not implement `matchIndex`. This is not safe. While `nextIndex` and `matchIndex` are generally updated at the same time to a similar value (specifically, `nextIndex = matchIndex + 1`), the two serve quite different purposes. `nextIndex` is a *guess* as to what prefix the leader shares with a given follower. It is generally quite optimistic (we share everything), and is moved backwards only on negative responses. For example, when a leader has just been elected, `nextIndex` is set to be index index at the end of the log. In a way, `nextIndex` is used for performance – you only need to send these things to this peer.

    `matchIndex` is used for safety. It is a conservative *measurement* of what prefix of the log the leader shares with a given follower. `matchIndex` cannot ever be set to a value that is too high, as this may cause the `commitIndex` to be moved too far forward. This is why `matchIndex` is initialized to -1 (i.e., we agree on no prefix), and only updated when a follower *positively acknowledges* an `AppendEntries` RPC.

- 问题2：能否把心跳AE(以下AE代表RPC-APPENDENTRIES)当作一种“**特殊**”的AE，只重置接受者的electionTimer、进行term检查操作、返回succuss？

    答案：**不可以**！Leader必须对空AE也要做出响应。Follower接受AE并reply的过程，也是告诉Leader它们的日志匹配程度的过程。Leader据此可以判断哪些日志已被复制到多数server中，这样就可以确定哪些entry可以commit。这在程序中表现为Leader的UpdateLeaderCommitIndex函数，这个函数即使收到空AE的reply也要调用。
    
    否则，如果client不再发送请求，上一次已被复制到多数的log entry可能永远都不会commit。

- 问题3: Leader在接收到多数复制成功的信息后决定commit某个log entry，需不需要单独向follower发送RPC？

    答案：不需要。**commitIndex的信息被piggyback(捎带)在下一次AE中**。下一次AE(无论是否为空)，会告诉Follower是否commit之前的log entry。这除了节省RPC开销外，还有利于组织顺序、便于log entry的管理

- 问题4: UpdateLeaderCommitIndex中，N从大往小找还是从小往大找？

    答案：都可以，看具体情况下哪个好？该算法可能还有优化空间。注意如果是从小往大找，找到的N未必是当前能commit的最大的Index，可能需要下一次AE时再更新，好处是某些情况下可能找得快。

- 问题5: 什么时候去apply log？apply哪些log entries？

    答案：首先，apply的log entries是index在[ lastApplied+1, commitIndex ]内的log entry，即已提交但未应用的部分。

    可以选择用applyTimer定时尝试StartApply，也可以在SetCommitIndex时发现commitIndex > lastApplied时触发StartApply函数。其中SetCommitIndex在Follower收到AE的捎带信息、Leader收到AE的reply信息时可能调用。

    两者各有优劣，定时法可以一次apply较多entry减少频繁apply。触发法可以让client响应较快。也可以两者结合用？

    本实现目前使用SetCommitIndex可以HitApplyTimer的方法。
    
- 问题6: Term、Index从0还是从1开始计？

    答案：Term初始为0，第一任Leader的term肯定大于0。Log entry的index从1开始计。但并没有给log entries结构的0下标位设置空值，因为可能浪费空间。所以Log entry实际上下标从0开始计，因此GetLogEntryByIndex(index) == logEntries[index-1]。很多地方因此比较绕，需要特别注意。

    程序中通过提供根据index或index范围获取Raft类的log entry的接口来避免错误，但对于AE中传入的args.Entries，下标问题需留意

- 问题7: 如何正确对follower的log entries进行截断(truncarte)? 如果follower的entry的term比对应index的args中的entry term要大，也要truncate吗？

    答案：符合Figure2所说的条件才要截断。是的，只要term开始不符合就要截断。

    另外注意阶段过程也是判断重复log entry的过程，在与RPC传入的args.Entries逐个比较时，如果发现同一index的term不相等则马上截断并退出循环。如果发现同一index的term相等则更新max_i_InLog，据此在后面截掉args.Entries前面与log中重复的部分。如果index处没有日志记录则无视。这是这个算法比较复杂的部分，代码大致如下

    ```go
    //3. if an existing entry conflicts with a new one (same index but different terms)
    	//this is one of the most tricky part!
    	index := 0
    	max_i_InLog := -1
    	for i := 0; i < len(args.Entries); i++ {
    		index = args.PrevLogIndex + 1 + i
    		//condition index > 0 is not necessary
    		if index > 0 && rf.GetLastLogIndex() >= index && rf.GetLogEntryByIndex(index).Term != args.Entries[i].Term {
    			//delete all entries after index (truncate)
    			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Truncate logEntries of index range [1, " + strconv.Itoa(index) + "), last commit index = " + strconv.Itoa(rf.commitIndex))
    
    			//rf.logEntries = rf.logEntries[:index-1]
    			rf.logEntries = rf.GetLogEntriesByIndexRange(1, index)
    			break
    		} else if index > 0 && rf.GetLastLogIndex() >= index && rf.GetLogEntryByIndex(index).Term == args.Entries[i].Term {
    			max_i_InLog = i
    		}
    	}
    
    	//4. append any new entries <not already> in the log, don't forget to slice args.Entries!
    	rf.logEntries = append(rf.logEntries, args.Entries[max_i_InLog+1:len(args.Entries)]...)
    ```

    

- 问题8: 如果Leader发送AE后发现自己不再是当前Leader，还要不要对success reply进行响应，如更新commitIndex？

    答案：不需要。不是当前Leader就不应该再做任何Leader独有的响应。应该马上取消当前向server发送AE的过程，并退出StartHearBeat函数。

- 问题9: Leader应该用一个总定时器，到时同时给所有follower发AE；还是应该给每个follwer分别单独设置一个定时器，到时定点给那个follower发送AE？

    答案：后者可以减少同时发AE的压力，但goroutine数量较多。由于server数量通常不多，所以其实差别不大？本实验中采用的是总定时器、**向所有其他server一起发AE**的模式。
    
- 问题10: Leader在HearteBeat时对某个server进行AE失败，是否需要一直重新尝试？还是放弃AE在下次HearteBeat中再尝试？

    答案：如果一直重新尝试AE，会导致有连接异常的时候产生大量RPC Call(和goroutine)。如果不重复尝试，会不会有问题？(答案似乎是不会，论文中的 If followers crash or run slowly, or if network packets are lost, the leader retries Append- Entries RPCs indefinitely (even after it has responded to the client) until all followers eventually store all log en- tries. 意思似乎是在不断的HeartBeat中进行retry AE，而不是一次HeartBeat中都要不断重试AE)。经过实验测试，如果单HearteBeat内不断尝试AE，会导致某一测试集下的RPC Call总数从2000提升到接近80000，而不重试也不会出错。
    
    注意投票中，如果RV失败，直接认为没有争取到投票、不会重试。

- 问题11: Leader在用某个nextIndex[i]对Follower尝试AE失败(因为inconsistency)，需要降低nextIndex[i] (减一)再尝试。如果该Follower已经宕机很久、错过了很多log entries，那么可能需要循环尝试很多次、比较耗费时间和线程开销，有无优化方法？

    答案：原论文5.3节末部分有讲到(**Fast Backup**)。基本思想是让Follower给Leader reply false时，也尽量包含跟日志有关的信息，从而让Leader直接知道nextIndex应该设为多少。论文提到该优化可能意义并不太大，设计上会更复杂、会在Leader和Follower之间交换更多的信息。目前暂未使用Fast Backup。
    
    

### Lab2C







