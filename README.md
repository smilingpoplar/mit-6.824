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
