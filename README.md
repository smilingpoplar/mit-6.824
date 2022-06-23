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

Raft共识算法的[论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)与[翻译](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)，Figure2总结了算法过程

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
