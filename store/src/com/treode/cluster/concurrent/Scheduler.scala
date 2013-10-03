package com.treode.cluster.concurrent

import java.util.concurrent.{TimeUnit, ScheduledExecutorService}

trait Scheduler {

  def execute (task: Runnable)

  def execute (task: => Any)

  def delay (millis: Long) (task: => Any)

  def at (millis: Long) (task: => Any)

  def spawn (task: => Any)
}

object Scheduler {

  private def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }

  def apply (executor: ScheduledExecutorService): Scheduler =
    new Scheduler {

      def execute (task: Runnable) =
        executor.execute (task)

      def execute (task: => Any) =
        executor.execute (toRunnable (task))

      def delay (millis: Long) (task: => Any) =
        executor.schedule (toRunnable (task), millis, TimeUnit.MILLISECONDS)

      def at (millis: Long) (task: => Any) {
        val t = System.currentTimeMillis
        if (millis < t)
          executor.execute (toRunnable (task))
        else
          executor.schedule (toRunnable (task), millis - t, TimeUnit.MILLISECONDS)
      }

      def spawn (task: => Any): Unit =
        executor.execute (toRunnable (task))
    }}
