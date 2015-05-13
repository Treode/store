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

import java.util.{ArrayDeque, HashMap}

import com.treode.async.{Async, AsyncQueue, Fiber, Scheduler}, Async.supply

import GenerationDocket.DocketId

private class Compactor (implicit scheduler: Scheduler, events: DiskEvents) {

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)
  private val docket = new GenerationDocket
  private val arrival = new ArrayDeque [DocketId]
  private var compactors: Compactors = null

  private def reengage() {
    if (compactors == null)
      return
    if (!arrival.isEmpty)
      _compact()
    else
      assert (docket.isEmpty)
  }

  private def _compact() {
    queue.begin {
      val id = arrival.remove()
      val gens = docket.remove (id)
      val cmpctr = compactors.get (id.typ)
      if (cmpctr == null)
        supply (events.noCompactorFor (id.typ))
      else
        cmpctr (Compaction (id.obj, gens))
    }}

  private def add (id: DocketId, gens: Set [Long]) {
    if (!(docket contains id))
      arrival.add (id)
    docket.add (id, gens)
  }

  def launch (compactors: Compactors): Unit =
    fiber.execute {
      this.compactors = compactors
      queue.engage()
    }

  def compact (typ: TypeId, obj: ObjectId): Unit =
    fiber.execute {
      add (DocketId (typ, obj), Set.empty)
      queue.engage()
    }

  def compact (drains: GenerationDocket): Unit =
    fiber.execute {
      for ((id, gens) <- drains)
        add (id, gens)
      queue.engage()
    }}
