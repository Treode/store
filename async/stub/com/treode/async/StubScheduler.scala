package com.treode.async

import java.util.concurrent.ScheduledExecutorService
import scala.util.Random

trait StubScheduler extends Scheduler {

  def runTasks (timers: Boolean = false)
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
