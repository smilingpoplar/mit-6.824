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
