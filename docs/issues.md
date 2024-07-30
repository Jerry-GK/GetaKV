

# Issues

## lab1



## lab2

- 问题1: 如何进行RPC调用？

    答案：对每种RPC(AE, RV), 设计了一个RPC Caller，是对RPC调用的包装。在这个函数中会对指定server发送RPC请求，接受reply(但不处理reply，交给上层)。Caller返回bool值表示是否RPC调用成功(千万注意这跟AE reply里success、RV reply里VoteGranted的区别)。

    Caller内可能存在多次RPC调用。设置了RPCSingleTimer和RPCBatchTimer(以下简称ST和BT)。Caller内的大致逻辑是：

    ```go
    func (rf *Raft)Caller(peerIdx, args, reply) bool{
      ResetTimer(BatchTimer, BTTimeout)
      
      for{
        ResetTimer(SingleTimer, STTimeout)
        go func{
          ok := rf.peers[peerIdx].Call("Callee", args, reply) //真正的RPC调用，可能很久才返回甚至不返回？
          ch <- ok //告诉Caller某个RPC调用返回了结果。
        }()
        
        select{
        case <-SingleTimer.C:
          //单次超时，重试。注意上一个goroutine的RPC调用可能还在传输中、可以被接收
          //经测试大多数RPC Call发生在disconnect发生后在此重试发送！
        	continue 
        case <-BatchTimer.C:
          return false //超过总时间BT，直接认为RPC Call失败，传输中的RPC调用全部作废
        case ok := <-ch
          if !ok{
            sleep(RPCInterval) //防止马上返回错误重试导致busy loop
            continue //重试。
          }else{
            return ok //某次RPC调用成功，第一个被接收的就是Caller返回的结果
          }
        }
      }
      //never reach here
    }
     
    ```

    这套逻辑有如下几个关键点

    - 对外部而言，Caller相当于一次有retry保证的稳定RPC调用，对其信任。如果Caller返回失败，外部调用者讲不再重新尝试。这意味着对于AE，如果AE Caller失败，将放弃AE、在下一次HeartBeat来临时再次尝试AE(延迟覆盖机制)；对于RV，RV Caller失败直接认为争取投票失败，没有任何重试。

        注意与延迟覆盖机制相反的是外部重试机制：Caller只真正调用最多一次，失败(RPC返回失败或ST超时)直接返回。外部进行重试。这样封装性不好，且难以处理长延迟的RPC返回。

    - Caller的超时时间RPCBatchTimeout决定了系统能承受的最长RPC延迟，一切长于BT的RPC响应都不可能被接收到。

        BT过长将导致上层goroutine不及时退出，另外可能在Caller内产生过多的RPC调用，削弱延迟覆盖机制的作用。

        BT过短会导致响应时间稍长的RPC全部被忽略，如果网络延迟高可能导致系统无法更新数据（比如2C的Figure8(not reliable)测试点）。

        BT的存在合理性是个问题，理论上可以让Caller无限尝试。

    - RPCSingleTimer的作用是在认为RPC单次调用可能失败的情况下再发起一个调用，这样，网络中会有统一逻辑RPC的多个调用，有一个成功被Caller收到成功的reply即可返回，增大成功的可能性。

        ST过长会导致重试频率低、Caller的成功率低。

        ST过短会导致RPC调用过多。

    - RPC调用返回给ok如果是false，将在Caller内重新尝试而不是直接返回false。这里要设置一个RPCInterval，否则如果RPC调用马上返回false，会导致busy loop。

    

- 问题2: 如何想办法减少RPC调用次数？

    答案：RPC调用次数是衡量系统开销的重要指标，可以采用这些办法减少RPC调用
    
    - AE采用延迟覆盖机制，不在外部重试RPC。
    - 减小RPCBatchTimeout
    - 增大RPCSingleTimeout
    - 采用Fast Backup，减少AE次数（也就是AE Caller调用的次数）。
    - 给每个server单独设置HeartBeat Timer + 非延迟覆盖机制 + 阻塞AE。能有效减少RPC，但会导致Leader发起AE频率降低，可能导致其他问题？
    
    实践中RPC数量仍然较多，有很大优化空间。
    
    

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

- 问题1:  如何理解matchIndex和nextIndex的关系？matchIndex[i] = nextIndex[i] - 1什么时候才成立？

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

    答案：首先，apply的log entries是index在[ lastAppliedIndex+1, commitIndex ]内的log entry，即已提交但未应用的部分。

    可以选择用applyTimer定时尝试StartApply，也可以在SetCommitIndex时发现commitIndex > lastAppliedIndex时触发StartApply函数。其中SetCommitIndex在Follower收到AE的捎带信息、Leader收到AE的reply信息时可能调用。

    两者各有优劣，定时法可以一次apply较多entry减少频繁apply。触发法可以让client响应较快。也可以两者结合用？

    本实现目前使用SetCommitIndex可以HitApplyTimer的方法。
    
- 问题6: Term、Index从0还是从1开始计？

    答案：Term初始为0，第一任Leader的term肯定大于0。Log entry的index从1开始计。但并没有给log entries结构的0下标位设置空值，因为可能浪费空间。所以Log entry实际上下标从0开始计，因此getLogEntryByIndex(index) == logEntries[index-1]。很多地方因此比较绕，需要特别注意。

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
    		if index > 0 && rf.getLastLogIndex() >= index && rf.getLogEntryByIndex(index).Term != args.Entries[i].Term {
    			//delete all entries after index (truncate)
    			labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Truncate logEntries of index range [1, " + fmt.Sprint(index) + "), last commit index = " + fmt.Sprint(rf.commitIndex))
    
    			//rf.logEntries = rf.logEntries[:index-1]
    			rf.logEntries = rf.getLogEntriesByIndexRange(1, index)
    			break
    		} else if index > 0 && rf.getLastLogIndex() >= index && rf.getLogEntryByIndex(index).Term == args.Entries[i].Term {
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

    所以本实验中，RPC Caller内部可能多次重试RPC请求，但不会超过RPCBatchTimeout；对于外部，一但Caller返回失败(超时)，就不再尝试，而是在下一次心跳产生的AE中再尝试。

    注意投票中，如果RV失败，也是直接认为没有争取到投票、不会重试。

- 问题11: Leader在用某个nextIndex[i]对Follower尝试AE失败(因为inconsistency)，需要降低nextIndex[i] (减一)再尝试。如果该Follower已经宕机很久、错过了很多log entries，那么可能需要循环尝试很多次、比较耗费时间和线程开销，有无优化方法？

    答案：原论文5.3节末部分有讲到(**Fast Backup**)。基本思想是让Follower给Leader reply false时，也尽量包含跟日志有关的信息，从而让Leader直接知道nextIndex应该设为多少。论文提到该优化可能意义并不太大，设计上会更复杂、会在Leader和Follower之间交换更多的信息。**目前暂未使用Fast Backup**。
    
    

### Lab2C

- 问题1: server的哪些属性需要persist？为什么？

    答案：目前persist的内容是term、voteFor、logEntries。这些内容与数据本身的内容、一致性有关，且影响选举。

    注意commitIndex可以不持久化，某些被提交的logEntry可能在崩溃发生后可能存在被recommit、reapply的情况？

- 问题1: 本实验persist是持久化到Persister类中，并未持久化到磁盘。有什么区别和影响？

    答案：Persist类应该是一种模拟持久化的封装类。具体不清楚，后续可能需要真正持久化到磁盘。

- 问题2: 哪些地方需要调用persist函数、进行持久化？

    答案：所有更改持久化属性的地方。lab中给持久化属性单独设立了Set接口，多数修改持久化属性的地方只能通过Set接口设置，Set内保证设置不同的值后马上persist。
    
    这样其实会导致persist很频繁，如果持久化到磁盘会效率低，可能有优化方法？


## Lab3
### Lab3A

- 问题1: 带有Raft模块的Server集群是如何工作的？

    答案：Raft系统中，client的输入并不被直接处理，而是被转化为Op，即server作为状态机，状态转化的操作过程。Op作为log Entry先写入Leader中，Leader将其复制到大多数服务器的log后，提交该op。提交后可以触发apply，apply会传递ApplyMsg(包含Op信息)给server主模块，然后真正执行该Op、修改主模块数据、得到输出并返回给client。

    非Leader也会随AE的接收而进行commit、触发apply。只不过非leader服务器的执行结果会被忽略(但对主模块数据有影响)。

    Op是非常关键的中间元素，每个server递增地生成opId，serverId + opId是唯一的。每当生成一个op时，也会在该server上生成一个监听其apply完成的管道(outPutCh[opId])。leader在apply op后会向管道发送返回结果、删除管道，这个结果再被返回给client。注意此时必须检查该op是否来产生于服务器自身，否则会导致错乱bug。非leader在apply后不会发送结果、删除管道。所以管道可能需要定期清理。

- 问题2: ApplyMsg中，CommandValid有什么用(可能设为false吗)？CommandIndex有什么用？

    答案：lab3B的snapshot中有用，可以提醒落后的follower读来自serevr的snapshot、更新lastAppliedIndex。

- 问题3: 对系统的读操作(比如KV中的Get)是否也要写Raft Log、经过Raft模块进行响应？

    答案：是？虽然读操作不修改数据内容，但也经过Raft模块可以保证操作的顺序性、一致性。



### Lab3B

- 问题1: Snapshot机制大致是怎样的？会引出什么麻烦？

    答案：为了避免serevr中raft模块内的log一直增长、太大，需要记录系统状态的快照snapshot，并可以删去快照前已经apply了的log entries。

    - 麻烦1：系统状态snapshot的持久化需要跟raft模块的persister绑定在一起，不能独立持久化。两者耦合性增强。

    - 麻烦2：logEntries的物理“数组”会被截断，但逻辑上需要假定有一份从头开始完整的日志、只不过不会访问lastIncluded前面的entry。也就是说逻辑index仍认为从0开始一直增长。这需要跟物理的下标进行转化、避免越界。

    - 麻烦3：对于单个server来说，lastAppliedIndex之前的entry对自己已经没用，可以做snapshot。但如果它是leader，它的某个log entry只需要复制到大多数server上就可以commit、apply了，然后应该继续尝试复制到其它小部分server上。**但如果apply后做了snapthot，那么这些lastAppliedIndex之前的entry已经被删掉、无法再复制到剩下的小部分“落后”的server上。**

        Raft论文的解决办法时：leader在这种情况下，需要给这些小部分serevr发送installSnapshot的RPC请求、强制同步snapshot状态，从而无需再发送lastAppliedIndex之前到entry。

    - 麻烦4：如何控制leader进行installSnapshot的RPC请求的时机？有以下几种可能的选择

        - 选择1：leader尝试AE时，如果发现需要发送的entries包括lastAppliedIndex之前的entry，那么直接对该follower发送installSnapshot、放弃本次AE。（我的做法）

        - 选择2：leader进行AE，结果多次尝试发现都未成功，那么leader认为该follower落后自己太多，发送installSnapshot、放弃本次AE。(我认为效率较低，没有采用)

        - 选择3：leader定时向所有follower发送installSnapshot(跟AE独立)。如果AE时发现需要发lastAppliedIndex之前的entry，那么直接放弃本次AE(或者说发空AE)。

            这个选择在论文中被提到过，但被否决了，因为数据开销太大且大多数时候没必要 (Follower大多数时候是跟leader同步的，不需要接受leader的snapshot)

    - 麻烦5：**不同server之间做snapshot(状态持久化)是独立的(无需询问leader意见)**，时机见’麻烦6‘。但是还可能接受来自leader的installSnapshot的RPC请求，这也是做snapshot的一个触发条件且数据来自外部。需要在两者生成一套进行选择、控制的机制。

    - 麻烦6：server状态，比如K/V系统的kv表，如果每次更新都马上持久化、做snapshot实在开销太大。应该控制持久化snapshot的时机和方法、减少I/O开销。也就是说，apply entry之后可以选择马上持久化，也可以一次持久化多个applied entries，比如发现log太大时才去做持久化（合理策略）；

        

- 问题2: 如何维护逻辑、物理两套index逻辑？

    答案：从逻辑上讲，每个server只有一份完整的逻辑日志，这个日志term从1开始计数。本实验中没有在日志数组logEntries的0

    位置设空值，所以term到下标的转换需要进行偏移。

    另外，引入snapshot机制后，lastIncludedIndex前面(不包括自身)的物理entries可能被删掉，是不能访问的范围，所以还要加一重转换。另外还要注意lastIncludedIndex=0表示无snapshot时的corner case。转换中必须严格防止访问到正常范围外的物理entry。

- 问题3: 当leader的某些log entry复制到大多数server并commit后，如果进行了snapshot、删掉这些entry后，它们还如何复制到剩下的其它server中？

    答案：它们不需要再复制。事实上snapshot时，如果对某个server[i]发送SN RPC成功，那么会修改对应leader的nextIndex[i]的值，使那些可能还没复制到其他serevr、但已被snapshot截断删去的那些log entries不用再被复制。
    
- 问题4: , lastLogIndex, commitIndex, lastAppliedIndex, lastIncludedIndex是什么含义、有什么关系？

    - 含义
    
        首先想象有一部完整的逻辑日志（包括想象前面已经被物理删去的entry）
    
        - lastLogIndex: 最后一条log entry的index。逻辑日志为空时此值为0。
    
        - commitIndex: 初始化为无效值0。1 -> commitIndex是已经被commit了的index范围，这部分entry可能有后一部分正在等待被apply。
    
        - lastAppliedIndex: 初始化为无效值0。1 -> lastAppliedIndex是已经被apply、随时可以被snapshot截断的范围。
    
        - lastIncludedIndedx: 如果没有进行任何快照，那么此值为0(初始化值)。否则，1 -> lastIncludeIndedx-1是上一次快照完成后已被删除的entry的范围(访问这部分entry会越界，是lab过程的一大重要问题)，lastIncludeIndedx -> lastLogIndex是当前情况下有物理日志对应的index范围，这部分的entry才能被访问。
    
            注意，虽然lastIncludedIndex没有被删，但是它对应的entry在快照中已经被apply，属于无用entry。
    
    - 递增性：
    
        这些属性都是**递增**的，不可能存在下降的情况，否则是严重的逻辑错误。
    
        但注意，非持久化的属性启动时会回归初始化值。但会被快速恢复？（见持久性）
    
    - 关系：
    
        一般情况下，**0 <= lastIncludedIndex <= lastAppliedIndex <= commitIndex <= lastLogIndex**
    
        因为commit需要log entry存在、apply需要已经commit、被快照删除需要已经apply。
    
        但是快照不仅可以来自于自身定时持久化，还可能来自leader的InstallSnapshot RPC，此时会传来强制同步的服务器snapshot数据和lastIncludedIndex，但并不会传来leader的logEntries，接收到IS的follower会删去args.lastIncludedIndex之前的entries（如果args.lastIncludeIndex比自身lastLogIndex要大，那么自身logEntries变为空）。
    
        在实验实现中，发现这种情况下不能马上在接受IS RPC的时候就修改kv serever的表、更改commitIndex、lastAppliedIndex，因为接受IS的模块属于Raft模块。所以解决办法是允许暂时可能的commitIndex、lastAppliedIndex比lastIncludedIndex小，然后在applyLog的时候发现时，向kv server发送commandInvalid的ApllyMsg，让其readPersist、真正读取snapshot的信息更新kv表，然后再修改lastAppliedIndex和commitIndex (其实commitIndex也可以在其它地方修改)。
    
        所以这个不等式是普遍情况，但不保证系统中任何时候都成立，这里是否体现设计有问题？
    
    - 持久性：
    
        这属性中，lastIncludedIndex, commitIndex需要持久化，lastAppliedIndex, lastLogIndex则不需要。
    
        注意lastLogIndex其实是实时根据logEntries得到的，logEntries是需要持久化的，所以其实也可以理解为lastLogIndex也是持久化的值。
    
        唯一不需要持久化的值是lastAppliedIndex，因为这个值会在发送ApplyMsg检测到异常时回复给其需要更新的最新值。当系统重启时, lastAppliedIndex是0，发现其小于lastIncludedIndex后，会发送invalid的ApplyMsg、读持久化、对lastAppliedIndex进行更新。
    
        实际中发现如果持久化lastAppliedIndex将出错，原因暂未知。















