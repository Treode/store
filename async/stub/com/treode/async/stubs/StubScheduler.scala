package com.treode.async.stubs

import java.util.concurrent.ScheduledExecutorService
import scala.util.Random

import com.treode.async.Scheduler

trait StubScheduler extends Scheduler {

  def runTasks (timers: Boolean = false, count: Int = Int.MaxValue): Int
  def shutdown (timeout: Long)
}

object StubScheduler {

  def random(): StubScheduler =
    new RandomScheduler (new Random (0))

  def random (random: Random): StubScheduler =
    new RandomScheduler (random)

  def multithreaded (executor: ScheduledExecutorService): StubScheduler =
    new StubExecutorAdaptor (executor)
}
