package com.treode.store.disk2

import com.treode.async._
import com.treode.async.io.StubFile
import org.scalatest.FlatSpec

import LogEntry.{Envelope, Update}

class LogSpec extends FlatSpec {

  val config = new DiskDriveConfig (6, 3, 2, 1<<20)

  "It" should "work" in {
    val scheduler = StubScheduler.random()
    val file = new StubFile (scheduler)
    val alloc = new SegmentAllocator (config)
    val dispatcher = new LogDispatcher (scheduler)
    val writer = new LogWriter (file, alloc, scheduler, dispatcher)

    alloc.init()
    writer.init (Callback.ignore)
    scheduler.runTasks()
    dispatcher.engage (writer)

    for (i <- 0 until 9)
      dispatcher.record (Update ("apple"), Callback.ignore)
    scheduler.runTasks()

    val cb1 = new CallbackCaptor [LogIterator]
    LogIterator (file, writer.head, alloc, cb1)
    scheduler.runTasks()
    val cb2 = new CallbackCaptor [Seq [Envelope]]
    AsyncIterator.scan (cb1.passed, cb2)
    scheduler.runTasks()
    val entries = cb2.passed
    expectResult (9) {
      entries count { e =>
        e.body match {
          case body: Update => body.s == "apple"
          case _ => false
        }}}
    var time = 0L
    println (entries)
    for (e <- entries) {
      assert (e.hdr.time >= time)
      time = e.hdr.time
    }}}
