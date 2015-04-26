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

package com.treode.disk.edit

import java.util.ArrayList

import com.treode.async.{Async, Callback}, Async.async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class DetacherSpec extends FlatSpec {

  "The Detacher" should "detach immediately when having never flushed" in {
    implicit val scheduler = StubScheduler.random()
    val dtch = new Detacher (false)
    dtch.startDraining().expectPass()
    assert (dtch.startFlush() == false)
    intercept [IllegalStateException] (dtch.finishFlush())
  }

  it should "detach immediately after finishing a flush" in {
    implicit val scheduler = StubScheduler.random()
    val dtch = new Detacher (false)
    assert (dtch.startFlush() == true)
    assert (dtch.finishFlush() == true)
    dtch.startDraining().expectPass()
    assert (dtch.startFlush() == false)
    intercept [IllegalStateException] (dtch.finishFlush())
  }

  it should "delay detaching while flushing" in {
    implicit val scheduler = StubScheduler.random()
    val dtch = new Detacher (false)
    assert (dtch.startFlush() == true)
    val cb = dtch.startDraining().capture()
    scheduler.run()
    cb.assertNotInvoked()
    assert (dtch.finishFlush() == false)
    cb.expectPass()
    assert (dtch.startFlush() == false)
    intercept [IllegalStateException] (dtch.finishFlush())
  }}
