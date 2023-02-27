package cn.flinkcore.java.scala

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * 通过socket数据源，去请求一个socket服务（doit01:9999）得到数据流
 * 然后统计数据流中出现的单词及其个数 scala 语言实现
 */
object _01_StreamWordCountScala {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.socketTextStream("192.168.43.132", 9999)

    // sourceStream.flatMap(s=>s.split("\\s+")).map(w=>(w,1))

    sourceStream
      .flatMap(s => {
        s.split("\\s+").map(w => (w, 1))
      })
      .keyBy(tp => tp._1)
      .sum(1)
      .print("_01_入门程序WordCount")

    env.execute("我的job"); // 提交job

  }

}
