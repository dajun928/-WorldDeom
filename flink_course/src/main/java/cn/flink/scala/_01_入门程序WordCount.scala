package cn.flink.java.scala

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object _01_���ų���WordCount {
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
      .print("_01_���ų���WordCount")

    env.execute("�ҵ�job"); // �ύjob

  }

}
