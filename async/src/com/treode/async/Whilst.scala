package com.treode.async

import java.util.concurrent.Executor
import scala.util.{Failure, Success}

import Async.async
import AsyncImplicits._
import Scheduler.toRunnable

class Whilst (executor: Executor) {

  private def execute (task: => Any) =
    executor.execute (toRunnable (task))

  private def pass [A] (cb: Callback [A], v: A) =
    executor.execute (toRunnable (cb, Success (v)))

  private def fail [A] (cb: Callback [A], t: Throwable) =
    executor.execute (toRunnable (cb, Failure (t)))

  def cb (p: => Boolean) (f: Callback [Unit] => Any): Async [Unit] =
    async { cb =>
      val loop = Callback.fix [Unit] { loop => {
        case Success (v) =>
           execute {
             try {
               if (p)
                 f (loop)
               else
                 pass (cb, ())
             } catch {
               case t: Throwable => fail (cb, t)
             }
           }
        case Failure (t) => fail (cb, t)
      }}
      loop.pass()
    }

  def f (p: => Boolean) (f: => Any): Async [Unit] =
    cb (p) {cb => f; cb.pass()}

  def apply [A] (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    cb (p) {cb => f run cb}
}
