import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DeptCount {
  def main(args: Array[String]):Unit={
    val conf = new SparkConf().setAppName("StreamingDeptCount").setMaster(args(0))
    val ssc = new StreamingContext(conf,Seconds(30))
    val msg = ssc.socketTextStream(args(1),args(2).toInt)
    val deptMsg = msg.filter(msg=>{
        val endpoint = msg.split(" ")(6)
        endpoint.split("/")(1) == "department"
      })
    val deps = deptMsg.map(rec=>{
       val endpoint = rec.split(" ")(6)
       (endpoint.split("/")(2),1)
    })
    val departmentTraffic = deps.reduceByKey(_+_)
    departmentTraffic.saveAsTextFiles("/user/satya123zx/deptWiseTraffic/",".cnt")
    ssc.start()
    ssc.awaitTermination()
  }
}
