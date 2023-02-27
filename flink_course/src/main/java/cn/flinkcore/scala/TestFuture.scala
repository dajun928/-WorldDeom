package cn.flinkcore.scala

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TestFuture {
  def main(args: Array[String]): Unit = {
    val future = Future {  // Future[String] 类型的
      Thread.sleep(1000)
      val tid = Thread.currentThread().getName
      println(s"future finished in $tid")
      "hello future" // 这里新增返回值
    }

    // 增加回调函数
    future.foreach(s => {
      val tid = Thread.currentThread().getName
      println(s"callback from $tid get content $s")
    })

    while (!future.isCompleted) {
      println("main thread wait for future")
      Thread.sleep(200)
    }
  }
}
