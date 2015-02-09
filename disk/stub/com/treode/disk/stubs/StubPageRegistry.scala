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

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.guard
import PageLedger.{Groups, Merger}

private class StubPageRegistry (releaser: EpochReleaser) (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubDisk.Config
) extends AbstractPageRegistry {

  import config.compactionProbability

  def probe (iter: Iterable [(Long, StubPage)]): Async [(Groups, Seq [Long])] = {
    val merger = new Merger
    val segments = Seq.newBuilder [Long]
    iter.async.foreach { case (offset, page) =>
      val groups = Set (PageGroup (page.group))
      for {
        live <- probe (page.typ, page.obj, groups)
      } yield {
        if (live._2.isEmpty) {
          releaser.release (disk.free (Seq (offset)))
        } else if (compactionProbability > 0.0 && random.nextDouble < compactionProbability) {
          merger.add (Map ((page.typ, page.obj) -> groups))
          segments += offset
        }
      }
    } .map { _ =>
      (merger.result, segments.result)
    }}}
