package tools.redis.load

/**
 * Created by tsingfu on 15/6/11.
 */
class LoadStatus(var numTotal: Long = 0, var numScanned: Long = 0, var numBatches: Long = 0,
                      var numProcessed: Long = 0, var numBatchesProcessed: Long = 0,
                      var startTimeMs: Long = 0, var numProcessedlastMonitored: Long = 0,
                      var loadFinished: Boolean = false)


//进度信息监控
//
//var numTotal: Long = 0 //记录总记录数
//var numScanned: Long = 0 //记录已经扫描的记录数
//var numBatches: Long = 0
//
//val taskMap = scala.collection.mutable.HashMap[Int, FutureTask[FutureTaskResult]]()
//var numProcessed: Long = 0 //记录已经处理的记录数
//var numBatchesProcessed: Long = 0 //记录已经处理的批次数量
//
////  var startTimeMs: Long = -1 //记录导入开始时间，单位Ms
//var runningTimeMs: Long = -1 //记录导入运行时间，单位Ms
//
//var loadSpeedPerSec: BigDecimal = -1 //记录导入运行期间导入平均速度
//var numProcessedlastMonitored: Long = 0 //记录最近一次输出进度信息时已导入的记录数
//var loadSpeedPerSecLastMonitored: BigDecimal = -1 //记录最近一次输出进度信息的周期对应的导入平均速度
//
//val timer = new Timer() //用于调度reporter任务，定期输出进度信息
