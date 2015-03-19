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

import java.util.HashMap

import com.treode.async.Async, Async.{guard, supply}

/** A convenient facade for an immutable map of compactor methods. */
private class CompactorRegistry (
  compactors: HashMap [Long, Compaction => Async [Unit]]
) (implicit
  events: DiskEvents
) {

  def compact (typ: TypeId, obj: ObjectId, gens: Set [Long]): Async [Unit] =
    guard {
      val compactor = compactors.get (typ.id)
      if (compactor != null)
        compactor (Compaction (obj, gens))
      else
        supply (events.noCompactorFor (typ))
    }}

private object CompactorRegistry {

  /** Collect compactor methods to build a registry; not thread safe. */
  class Builder (implicit events: DiskEvents) {

    private val compactors = new HashMap [Long, Compaction => Async [Unit]]

    def add (desc: PageDescriptor [_]) (f: Compaction => Async [Unit]) {
      val typ = desc.id
      require (!(compactors containsKey typ), s"Compactor already registered for $typ")
      compactors.put (typ.id, f)
    }

    def result: CompactorRegistry =
      new CompactorRegistry (compactors)
  }}
