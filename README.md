## MIT 6.824 分布式系统 [(2022)](http://nil.csail.mit.edu/6.824/2022/schedule.html)

### [Lab1](http://nil.csail.mit.edu/6.824/2022/labs/lab-mr.html)-MapReduce

#### /mr 实现思路

coordinator和多个worker通过RPC通信，worker以plugin方式加载/mrapps下的Map()和Reduce()函数实现

worker向coordinator发心跳要任务：
- 若是Map任务，收到fileName读取文件kv，mapf处理后输出成json格式的中间文件mr-fileId-reduceId
- 若是Reduce任务，将mr-XX-reduceId文件合并后输出mr-out-reduceId

coordinator管理整体任务状态：
- 在Map全部完成后才开始Reduce
- 给worker任务10s后检查完成情况，没完成就重新标记为待做

### [Lab2](http://nil.csail.mit.edu/6.824/2022/labs/lab-raft.html)-Raft

Raft共识算法的[论文][1]与[翻译][2]，Figure2总结了算法过程

#### Part2A 选举和心跳
- currentTerm作为逻辑时钟，空entries[]的AppendEntries RPC作为心跳
- 三个角色follower/candidate/leader的状态机转化
- 无论什么角色，收到的request或reply中对方的term比自己大，马上变成follower

粗略过程如下：
1. 刚开始都是follower，超时没有收到leader心跳变为candidate
2. candidate广播RequestVote RPC请求投票
3. RequestVote处理方对一个term只能投一张票
4. candidate收到">1/2多数"选票变为leader
5. leader广播AppendEntries RPC心跳阻止其他选举产生

#### Part2B 日志项的复制

粗略过程如下：
1. client用Start()发送需本地状态机执行的command
2. leader在本地日志log[]中添加LogEntry{需本地状态机执行的command, 创建时的term}
3. 通过AppendEntries心跳通知所有follower复制log[]
4. follower[i]回复成功后，leader更新matchIndex[i]，matchIndex[i]是已成功复制到follower[i]的log[]最大索引
5. 用matchIndex[]找出满足">1/2多数"的commitIndex，则log[:commitIndex]已成功复制到多数follower，可应用log[:commitIndex]到本地状态机
6. leader的commitIndex在心跳中传给follower，follower也可应用log[:commitIndex]到本地状态机

第3步通过心跳复制leader日志到follower[i]，关键是找到leader与follower[i]日志匹配的最大索引lastMatch，然后将follower[i]的log[lastMatch+1:]全部覆盖。leader用prevLogIndex从最后位置（nextIndex[i]-1）往前探查lastMatch。日志匹配，这里检查某index位置的term相同。

#### Part2C 持久化
TestFigure8Unreliable2C的超时问题：
- 加速查找lastMatch的[回退优化](https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations)

测试帮助： 
- [go-test-many](https://gist.github.com/smilingpoplar/793cb88ff29bff65bdb1a78d49f4cfdd)可多核运行测试
```
go-test-many.sh 20 8 2A  // 共20次用8核运行`go test -run 2A`
```
- pprof性能分析：
```
go test -race -run TestFigure8Unreliable2C -cpuprofile cpu.prof -blockprofile block.prof -mutexprofile mutex.prof

// 命令行
go tool pprof cpu.prof
top15
// web端
go tool pprof -http=":8080" cpu.prof
```
#### Part2D 日志压缩
[论文][1]Section7有介绍。

粗略过程如下：
- 上层应用调用leader的`Snapshot(lastIncludedIndex, snapshot)`传入快照。leader把log截断成[lastIncludedIndex:]，log[lastIncludeIndex]留作dummy头
- leader在发送心跳时若发现`nextIndex[i]<=lastIncludedIndex`，改成InstallSnapshot RPC向follower[i]发送快照
- follower收到快照时截断自己的log，再往applyCh应用快照

### [Lab3](http://nil.csail.mit.edu/6.824/2022/labs/lab-kvraft.html)-基于raft实现的容错KV服务
#### Part3A 无快照功能的KV服务

参考[Students' Guide to Raft][3]

- kvclient向leader-kvserser发Get/PutAppend请求，leader-kvserver转发请求给自己的leader-raft层，leader-raft层在后台与follower-raft同步日志，然后leader-kvserver从applyCh确认收到请求，执行请求并将结果返回kvclient。
- kvclient的请求要同步返回，每个请求要等在leader-kvserver一个channel上，而leader-kvserver直到从applyCh拿到请求才能执行，执行结果与等待的请求之间需要有一（applyCh）到多（等待在opResultCh）的分发。kvclient请求由RequestId{clientId,seqNum}唯一确定。
- kvclient写请求可能因网络乱序到达。leader-kvserver要记录各kvclient已执行写请求的lastSeqNum，只执行`seqNum > lastSeqNum`的写请求。


[1]: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
[2]: https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md
[3]: https://thesquareplanet.com/blog/students-guide-to-raft/#applying-client-operations/
