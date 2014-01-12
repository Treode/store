package com.treode.store.disk2

import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.cluster.events.StubEvents
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class LogSpec extends FlatSpec {

  val config = DiskDriveConfig (6, 2, 1<<20)

  val update = new RecordDescriptor (0x0E4F8ABF, Picklers.string)

  "It" should "work" in {
    val scheduler = StubScheduler.random()
    val file = new StubFile (scheduler)
    val alloc = new SegmentAllocator (config)
    val dispatcher = new LogDispatcher (scheduler)
    val writer = new LogWriter (file, alloc, scheduler, dispatcher)
    val registry = new RecordRegistry (StubEvents)

    alloc.init()
    writer.init (Callback.ignore)
    scheduler.runTasks()
    dispatcher.engage (writer)

    var replayed = Seq.empty [String]
    update.register (registry) (replayed :+= _)

    for (i <- 0 until 9)
      update (dispatcher) ("apple") (Callback.ignore)
    scheduler.runTasks()

    val cb1 = new CallbackCaptor [LogIterator]
    LogIterator (file, writer.head, alloc, registry, cb1)
    scheduler.runTasks()
    val cb2 = new CallbackCaptor [Seq [Replay]]
    AsyncIterator.scan (cb1.passed, cb2)
    scheduler.runTasks()
    val entries = cb2.passed
    entries foreach (_.replay())
    expectResult (9) (replayed.size)
    assert (replayed forall (_ == "apple"))
    var time = 0L
    for (e <- entries) {
      assert (e.time >= time)
      time = e.time
    }}}
