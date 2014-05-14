package com.treode.disk.stubs

import java.util.{ArrayDeque, ArrayList, HashMap}
import scala.collection.JavaConversions
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Callback}
import com.treode.async.implicits._
import com.treode.buffer.ArrayBuffer
import com.treode.disk.RecordRegistry

import Async.async
import JavaConversions._

class StubDiskDrive (implicit random: Random) {

  private var records = new ArrayList [Seq [StubRecord]]
  private var pages = Map.empty [Long, StubPage]
  private var stack = new ArrayDeque [Callback [Unit]]

  /** If true, the next call to `flush` or `fill` will be captured and push on a stack. */
  var stop: Boolean = false

  /** If true, a call to `flush` or `fill` was captured. */
  def hasLast: Boolean = !stack.isEmpty

  /** Pop the most recent call to `flush` or `fill` and return a callback which you can
    * `pass` or `fail`.
    */
  def last: Callback [Unit] = stack.pop()

  private def _stop [A] (f: Callback [A] => Any): Async [A] = {
    async { cb =>
      synchronized {
        if (stop) {
          stack.push {
            case Success (_) =>
              synchronized (f (cb))
            case Failure (t) =>
              cb.fail (t)
          }
        } else {
          f (cb)
        }}}}

  private [stubs] def replay (registry: RecordRegistry) {
    for (rs <- this.records; r <- rs)
      registry.read (r.typ, r.data) ()
  }

  private [stubs] def log (records: Seq [StubRecord]): Async [Unit] =
    _stop { cb =>
      this.records.add (records)
      cb.pass()
    }

  private [stubs] def write (page: StubPage): Async [Long] =
    _stop { cb =>
      var pos = random.nextLong
      while (pages contains pos)
        pos = random.nextLong
      pages += pos -> page
      cb.pass (pos)
    }

  private [stubs] def read (pos: Long): Async [StubPage] =
    _stop { cb =>
      cb.pass (pages (pos))
    }}
