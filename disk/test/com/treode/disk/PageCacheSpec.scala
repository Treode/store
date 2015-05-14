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

package com.treode.disk

import com.treode.async.Async, Async.supply
import com.treode.async.stubs.{AsyncCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class PageCacheSpec extends FlatSpec {

  implicit val config = DiskTestConfig()
  implicit val events = new StubDiskEvents
  val geom = DriveGeometry (8, 6, 1 << 18)
  val desc = Stuff.pager
  val stuff = Stuff (0xEDD66AFF181DED3FL, 7)

  private def setup () (implicit files: FileSystem, scheduler: StubScheduler): DiskAgent = {
    implicit val recovery = new RecoveryAgent
    val launch = recovery.reattach().expectPass()
    launch.launch()
    val agent = launch.controller.asInstanceOf [DiskAgent]
    agent.attach (geom, "d1") .expectPass (Notification.unit)
    agent
  }

  private def recover () (implicit files: FileSystem, scheduler: StubScheduler): DiskAgent = {
    implicit val recovery = new RecoveryAgent
    val launch = recovery.reattach ("d1") .expectPass()
    launch.launch()
    scheduler.run()
    launch.controller.asInstanceOf [DiskAgent]
  }

  /** Obliterate the data on disk to ensure that it was read from cache. */
  private def obliterate (pos: Position) (implicit files: FileSystem, scheduler: StubScheduler) {
    val file = files.open ("d1")
    val buf = PagedBuffer (12)
    buf.writePos = (pos.length)
    file.flush (buf, pos.offset) .expectPass()
  }

  "The PageCache" should "read from cache a page that was just written" in {
    implicit val files = new StubFileSystem
    files.create ("d1", geom.diskBytes.toInt, geom.blockBits)
    implicit val scheduler = StubScheduler.random()

    val controller = setup()
    import controller.disk
    val pos = disk.write (desc, 0, 0, stuff) .expectPass()
    obliterate (pos)
    disk.read (desc, pos) .expectPass (stuff)
  }

  it should "read a page that was just read" in {
    implicit val files = new StubFileSystem
    files.create ("d1", geom.diskBytes.toInt, geom.blockBits)

    val pos = {
      implicit val scheduler = StubScheduler.random()
      val controller = setup()
      import controller.disk
      disk.write (desc, 0, 0, stuff) .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      val controller = setup()
      import controller.disk
      disk.read (desc, pos) .expectPass (stuff)
      obliterate (pos)
      disk.read (desc, pos) .expectPass (stuff)
    }}}
