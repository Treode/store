package com.treode.async

import java.util.concurrent.Executor

import Scheduler.toRunnable

class Whilst (executor: Executor) {

  private def execute (task: Runnable) =
    executor.execute (task)

  private def execute (task: => Any) =
    executor.execute (toRunnable (task))

  def cb (p: => Boolean) (f: Callback [Unit] => Any): Async [Unit] =
    new Async [Unit] {
      private var ran = false
      def run (cb: Callback [Unit]) {
        require (!ran, "Async was already run.")
        ran = true
        val loop = new Callback [Unit] {
          def pass (v: Unit): Unit = execute {
            try {
              if (p)
                f (this)
              else
                execute (toRunnable (cb, ()))
            } catch {
              case t: Throwable => execute (toRunnable (cb, t))
            }}
          def fail (t: Throwable): Unit = execute (toRunnable (cb, t))
        }
        loop.pass()
      }}

  def f (p: => Boolean) (f: => Any): Async [Unit] =
    cb (p) {cb => f; cb.pass()}

  def apply [A] (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    cb (p) {cb => f run cb}
}
