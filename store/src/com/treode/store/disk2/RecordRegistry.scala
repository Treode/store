package com.treode.store.disk2

import java.util.concurrent.ConcurrentHashMap
import com.treode.cluster.events.Events
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

private class RecordRegistry (events: Events) {

  private val types = new ConcurrentHashMap [Int, PickledReplay]

  def prepare (time: Long, id: TypeId, buf: PagedBuffer, len: Int): Replay = {
    if (len == 0)
      return Replay.noop (time)

    val end = buf.readPos + len
    val pr = types.get (id.id)
    if (pr == null) {
      events.recordNotRecognized (id, len)
      buf.readPos = end
      buf.discard (end)
      return Replay.noop (time)
    }

    try {
      val replay = pr.prepare (time, buf)
      if (buf.readPos != end) {
        events.unpicklingRecordConsumedWrongNumberOfBytes (id)
        buf.readPos = end
        buf.discard (end)
      }
      replay
    } catch {
      case e: Throwable =>
        events.exceptionFromRecordHandler (e)
        buf.readPos = end
        buf.discard (end)
        Replay.noop (time)
    }}

  def register [R] (p: Pickler [R], id: TypeId) (f: R => Any) {
    val pf = PickledReplay (p, id, f)
    require (types.putIfAbsent (id.id, pf) == null, "Record type already registered: " + id)
  }}
