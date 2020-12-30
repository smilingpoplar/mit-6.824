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
* 不管处于什么状态，只要收到的request和reply中对方的任期比自己大，马上更新任期变成follower
* 投票和日志复制行为需要双方在同一个任期中

粗略过程如下：
1. 刚开始都是follower
2. 当follower超时没有收到leader心跳，就currentTerm++变为candidate，然后并行发送RequestVote。
3. 服务器会拒绝RequestVote{lastLogTerm，lastLogIndex}还没自己新的投票请求。对一个任期只有一张票，先求先得。
4. 当candidate收到"多数"服务器的选票，就当选为leader。新leader广播心跳，阻止其他选举产生。
5. candidate若收到其他服务器的、任期>=自己任期的心跳，就变成follower承认新leader。

#### Part2B 日志项的复制

粗略过程如下：
1. 收到client请求，包含{需状态机执行的指令}
2. leader在本地日志log[]中添加日志项{需状态机执行的指令, 创建时的任期}
3. 通过AppendEntries心跳通知所有follower复制日志项。
   * leader是强势leader，要将follower中从最后匹配索引位置往后的项全部覆盖。
   * 最后匹配索引是通过prevLogIndex从最后往前试探出来的，用{term,logIndex}比较follower和leader的日志项是否匹配。
   * 加速查找匹配日志项的[回退优化](https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations)
   * follower回复成功时，leader更新matchIndex[i]，用matchIndex[]找出满足"多数"条件的commitIndex，然后应用日志项到本地状态机中。
4. commitIndex是从matchIndex[]中选出来的，matchIndex[i]是已成功复制到follower[i]中的日志项索引，所以"已提交"日志项一定"已复制"到follower中。
5. leader的commitIndex会在心跳中传给follower，follower也就可以应用日志项到本地状态机中。

#### /raft调bug
[go-test-many](https://gist.github.com/jonhoo/f686cacb4b9fe716d5aa)可并行运行测试

##### TestFigure8Unreliable2C超时
在设置心跳50ms、选举超时150-300ms后，跑100次还是没通过。照[这里](https://github.com/springfieldking/mit-6.824-golabs-2018/issues/3)的提示，不要每次commit时立即apply，专门一个applyLoop等在条件变量通知它apply。

#### TestFigure82C apply error: commit index=20 server=2 ...
1. 原因：leader的commitIndex更新逻辑
2. 情景：leader带着旧term=t53挂掉，新leader在term=t54接受了些cmd后挂掉，旧leader起来变为t54重新当选（违反了每个term只能选一次的规定）。在旧leader接受cmd后，还没来得及传播自己的log链就又挂了，这时候同一个term=t54有新旧两条log链分叉。
   原因：在处理相同term的心跳时重置votedFor=-1，从而相同term的旧leader可以再次拉到票。