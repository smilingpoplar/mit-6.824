## MIT6.824 分布式系统 [(2020)](https://pdos.csail.mit.edu/6.824/schedule.html)

### [Lab1](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)-MapReduce

#### /mr 实现思路

1个master和多个worker通过RPC通信，worker以plugin方式加载/mrapps下的Map()和Reduce()函数实现。

worker向master要任务：
* 若是MAP任务，收到filename读取文件，mapf处理后输出成json格式的中间文件mr-fileIdx-reduceIdx
* 若是REDUCE任务，将mr-XX-reduceIdx文件合并后输出mr-out-reduceIdx

master管理各任务状态：
* 在MAP全部完成后才进REDUCE阶段
* 给worker任务时就定时，10s内worker若没汇报完成，就把任务重新放进待做队列

### [Lab2](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)-Raft

Raft共识算法的[论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)与[翻译](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)，Figure2总结了算法过程

#### Part2A 选举和心跳

* 任期currentTerm作为逻辑时钟，日志项为空的AppendEntries RPC作为心跳
* 三个状态follower/candidate/leader的状态机转化
* 不管什么状态，只要收到的request和response中对方的任期比自己大，马上更新任期变成follower。
* 投票行为和日志同步行为需要双方在同一个任期中

粗略过程如下：
1. 刚开始都是follower
2. 当follower超时没有收到leader心跳，就变为candidate、currentTerm++，然后并行发送RequestVote，包含{任期，最新日志的索引}。
3. 其他服务器会比较{任期，最新日志的索引}，拒绝那些日志还没自己新的candidate。每个服务器对一个任期只有一张选票，先求先得。
4. 当candidate收到大多数服务器的选票，就当选为leader。新leader广播心跳，阻止其他选举产生。
5. candidate若收到其他服务器的、任期>=自己任期的心跳，就承认新leader。

#### Part2B 日志项的复制

1. client发起请求包含{需状态机执行的指令}
2. leader往日志中添加日志项{需状态机执行的指令, 创建时的任期}。如果最新日志的索引>=准备发往follower[i]的日志索引nextIndex[i]，就发送AppendEntries请求，带上从nextIndex[i]开始的日志项。
   * 若成功就更新nextIndex[i]和表明已复制进度的matchIndex[i]。
   * 若失败说明leader和follower的日志项不一致，follower从不一致项往后都被删除，nextIndex[i]--后重试
3. 当日志项被复制到大多数服务器后，leader应用日志项中的指令到它的状态机中（提交日志项），然后把结果返回给client
4. 一旦follower知道日志项已被提交，它也会把这条日志项按照日志顺序应用到本地状态机中

#### /raft 实现思路
