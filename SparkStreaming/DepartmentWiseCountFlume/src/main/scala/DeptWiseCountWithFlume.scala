import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._

object DeptWiseCountWithFlume {
  def main(args :Array[String]):Unit={
    val conf = new SparkConf()
      .setAppName("DeptWiseCountWithFlume")
      .setMaster(args(0))
    val ssc = new StreamingContext(conf,Seconds(30))
    val flumeStream = FlumeUtils.createPollingStream(ssc,args(1),args(2).toInt)
    val messages = flumeStream.map(rec=> new String(rec.event.getBody.array()))
    //filter department
    val deptMsg = messages.filter(msg=>{
      val endpoint = msg.split(" ")(6)
      endpoint.split("/")(1)=="department"
    })
    //create a map from department type
    val dept = deptMsg.map(rec=>{
      val endpoint = rec.split(" ")(6)
      (endpoint.split("/")(2),1)
    })
    //count
    val deptTraffice = dept.reduceByKey(_+_)
    deptTraffice.saveAsTextFiles("/user/satya123zx/DeptWiseCountWithFlume/cnt")

    ssc.start()
    ssc.awaitTermination()
  }
}
