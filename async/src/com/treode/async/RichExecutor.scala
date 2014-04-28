package com.treode.async

import java.util.concurrent.Executor
import scala.util.{Failure, Success}

import com.treode.async.implicits._

import Async.async
import Scheduler.toRunnable

private class RichExecutor (executor: Executor) {

  private def execute (task: => Any) =
    executor.execute (toRunnable (task))

  private def pass [A] (cb: Callback [A], v: A) =
    executor.execute (toRunnable (cb, Success (v)))

  private def fail [A] (cb: Callback [A], t: Throwable) =
    executor.execute (toRunnable (cb, Failure (t)))

  def whilst [A] (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    async { cb =>
      val loop = Callback.fix [Unit] { loop => {
        case Success (v) =>
           execute {
             try {
               if (p)
                 f run loop
               else
                 pass (cb, ())
             } catch {
               case t: Throwable => fail (cb, t)
             }
           }
        case Failure (t) => fail (cb, t)
      }}
      loop.pass()
    }}
