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

import java.nio.file.{Path, Paths}
import scala.collection.mutable.UnrolledBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.notify.{Message, Notification}
import org.scalatest.Assertions

import Assertions.assertResult

private object DiskTestTools {

  val sysid = SystemId (0, 0)

  def assertEqNotification (expected: Message*) (actual: Notification [Unit]) {
    assert (expected == actual.errors)
  }

  implicit def stringToPath (path: String): Path =
    Paths.get (path)
}
