/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk.stubs

import java.util.{ArrayDeque, ArrayList, HashMap}
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Callback, Scheduler}, Async.{async, supply}
import com.treode.async.implicits._
import com.treode.async.misc.RichOption
import com.treode.buffer.ArrayBuffer
import com.treode.disk.{GenerationDocket, ObjectId, RecordRegistry, TypeId}
import org.scalatest.Assertions, Assertions.fail

class StubDiskDrive (implicit random: Random) {

  private val stack = new ArrayDeque [Callback [Unit]]
  private var records = new ArrayList [Seq [StubRecord]]
  private var pages = Map.empty [Long, StubPage]

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

  private [stubs] def replay (registry: RecordRegistry) (implicit scheduler: Scheduler): Async [Unit] =
    for (rs <- records.async; r <- rs.latch)
      async [Unit] { cb =>
        scheduler.execute {
          registry.read (r.typ, r.data) (())
          cb.pass (())
        }}

  private [stubs] def claim (crashed: Boolean, claims: GenerationDocket): Unit =
    synchronized {
      for ((pos, page) <- pages)
        if (!claims.contains (page.typ, page.obj, page.gen))
          if (crashed)
            pages -= pos
          else
            fail ("Disk leak detected.")
    }

  private [stubs] def mark(): Int =
    synchronized {
      records.size
    }

  private [stubs] def checkpoint (mark: Int): Unit =
    synchronized {
      val _records = new ArrayList [Seq [StubRecord]] (records.size - mark)
      _records.addAll (records.subList (mark, records.size))
      records = _records
    }

  private [stubs] def log (records: Seq [StubRecord]): Async [Unit] =
    _stop { cb =>
      synchronized {
        this.records.add (records)
      }
      cb.pass (())
    }

  private [stubs] def write (page: StubPage): Async [Long] =
    _stop { cb =>
      val pos = synchronized {
        var pos = random.nextLong & 0x7FFFFFFFFFFFFFFFL
        while (pages contains pos)
          pos = random.nextLong
        pages += pos -> page
        pos
      }
      cb.pass (pos)
    }

  private [stubs] def read (pos: Long): Async [StubPage] =
    _stop { cb =>
      val page = synchronized {
        pages.get (pos)
      }
      page match {
        case Some (page) => cb.pass (page)
        case None => cb.fail (new Exception (s"Page $pos not found"))
      }}

  private [stubs] def drainable: GenerationDocket = {
    val ledger = new GenerationDocket
    for ((pos, page) <- pages)
      ledger.add (page.typ, page.obj, page.gen)
    val drains = new GenerationDocket
    for ((id, gens) <- ledger; gen <- gens)
      if (random.nextInt (2) == 0)
        drains.add (id, gen)
    drains
  }

  private [stubs] def release (typ: TypeId, obj: ObjectId, gens: Set [Long]): Unit =
    synchronized {
      for ((pos, page) <- pages)
        if (page.typ == typ && page.obj == obj && (gens contains page.gen))
          pages -= pos
    }

  private [stubs] def cleanable(): Iterable [(Long, StubPage)] =
    synchronized {
      pages
    }

  private [stubs] def free (pos: Seq [Long]): Unit =
    synchronized {
      pages --= pos
    }}
