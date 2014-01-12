package com.treode.store.disk2

import java.util.ArrayList
import com.treode.async.{Callback, Scheduler}
import com.treode.pickle.Pickler

private class PageDispatcher (scheduler: Scheduler) {

  private val dsp = new Dispatcher [PickledPage] (scheduler)

  def write [P] (p: Pickler [P], page: P, cb: Callback [(Int, Long, Int)]): Unit =
    dsp.send (PickledPage (p, page, cb))

  def engage (writer: PageWriter): Unit =
    dsp.receive (writer.receiver)

  def replace (rejects: ArrayList [PickledPage]): Unit =
    dsp.replace (rejects)
}
