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

import java.util.{ArrayDeque}
import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag
import com.treode.async.{Fiber, Scheduler}

private class Dispatcher [M] (
    private var counter: Long
) (implicit
    scheduler: Scheduler,
    mtag: ClassTag [M]
) {

  private type R = (Long, UnrolledBuffer [M]) => Any

  private val fiber = new Fiber
  private var engaged = false
  private var messages = new UnrolledBuffer [M]

  val receivers = new ArrayDeque [R]

  private def singleton (m: M): UnrolledBuffer [M] =
    UnrolledBuffer (m)

  private def drain(): UnrolledBuffer [M] = {
    val t = messages
    messages = new UnrolledBuffer
    t
  }

  private def engage (receiver: R, messages: UnrolledBuffer [M]) {
    counter += 1
    engaged = true
    scheduler.execute (receiver (counter, messages))
  }

  def send (message: M): Unit = fiber.execute {
    if (engaged || receivers.isEmpty)
      messages += message
    else
      engage (receivers.remove(), singleton (message))
  }

  def receive (receiver: R): Unit = fiber.execute {
    if (engaged || messages.isEmpty)
      receivers.add (receiver)
    else
      engage (receiver, drain())
  }

  def replace (rejects: UnrolledBuffer [M]): Unit = fiber.execute {
    rejects.concat (messages)
    messages = rejects
    if (!messages.isEmpty && !receivers.isEmpty)
      engage (receivers.remove(), drain())
    else
      engaged = false
  }}
