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
- 死锁分析：
```
// 用deadlock.Mutex换掉sync.Mutex声明
go get github.com/sasha-s/go-deadlock
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

#### Part3B 带快照功能的KV服务
kvserver加上快照的保存和恢复功能
### [Lab4](http://nil.csail.mit.edu/6.824/2022/labs/lab-shard.html)-分片的KV服务

概念：
- 分片shard即kv子集
- 一个raft组（replica set）是一台逻辑主机，对指令线性一致排序，负责某些分片数据。leader接收指令，follower通过applyCh更新本地状态
- 配置config保存：分片在哪个raft组，组内有哪些服务器
- shardctrler管理配置，本身也是个raft组
#### Part4A 配置管理shardctrler

实现类似lab3B，支持操作：JOIN（增加raft组）、LEAVE（去掉raft组）、MOVE（将分片分配给某raft组）、QUERY（获取某版本的配置）

rebalance：分片再平衡按尽量平均的原则计算各组应有多少分片，多的先拿出分片存到数组unalloc，少的再从unalloc拿

#### Part4B 分片服务器shardkv

- 在lab3B的基础上修改
- kvserver修改本地状态机的操作都要通过raft同步

kvclient的过程如下：
1. kvclient将key哈希成shard号，查shardctrler配置得到replica组gid，向gid的多个服务器轮发，直到发往组leader
2. kvserver收到请求，若请求的分片数据不是自己负责的了，就返回ErrWrongGroup
3. kvclient收到ErrWrongGroup，向shardctrler获取配置后重试

kvserver的分片迁移：
1. 某分片迁移时不应该影响其他分片的服务
    - 迁移状态是关于分片的，不是关于kvserver的
    - 分片迁出迁入期间拒绝服务，迁出方返回ErrWrongGroup，迁入方返回ErrShardMigrating让kvclient稍后重试
2. 怎么开始分片迁移？
    - 由kvserver-leader定时拉取下一配置，并与kvserver-follower同步。若当前正在分片迁入或迁出，则等迁移完成再拉取。
    - 若分片配置有变动，设置shardState[i]为迁入或迁出状态，把要迁出分片参数用通道发往migrateLoop做迁移。
    - 迁移双方的配置版本相同才能迁移。如果迁出方版本>迁入方，稍后重试，等待迁入方版本追上来；如果迁出方版本<迁入方，迁入方已收到过分片。
    - 宕机重启后shardState[]可以从currentConfig和lastConfig比较得到，再重发一遍迁移。
3. 选push还是pull？我选了push实现
    - push：config更新时kvserver-leader向对方raft组的leader推送对方缺少的分片数据
    - pull：config更新时kvserver-leader向对方raft组的leader拉取自己缺少的分片数据
4. 迁出RPC成功后，向raft记入GC操作，在applyLoop处理GC时删除迁出分片中的key

##### 改/shardkv的bug
> Bug：在Append进raft日志、Append日志处理之间插入了分片迁移，导致Append结果没随着迁移
> 
> 协程1：== 1.1 判断分片没在迁移 ======= 1.2 Append进raft日志 ================== 1.2 Append日志处理<br />
> 协程2：2.1 判断分片没在迁移 ==== 2.2 Config进raft日志 ==== 2.3 Config日志处理，分片迁移

把条件判断移到处理applyCh消息时，Append不负责分片就返回ErrWrongGroup，让kvclient重试

> Bug：follower有时丢了日志没执行，shardState有时飘忽不定

kvserver要加个lastApplied字段，防止load了旧快照或load快照后执行了旧日志

> Bug：TestConcurrent1重复Append了
> 
> applyPull()刚迁入分片，马上遇到kvclient之前轮询的Append

分片迁移时一起带上dedup map

> Bug：TestConcurrent2在Append某key时分叉了
> 
> raft组重新选举，新主重放日志时重发push()，发来旧状态

applyPull()要根据gid去重

> Bug：TestConcurrent3有一定几率卡住
>
> 有些调用由leader发起：shardState状态变更时向外push()，迁入切片后`go donePull()`通知迁出方。但此调用如果刚好发生在raft换主的短暂间隔中，将没人调用，流程卡住。

若不是leader，重试调用几次


[1]: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
[2]: https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md
[3]: https://thesquareplanet.com/blog/students-guide-to-raft/#applying-client-operations/
