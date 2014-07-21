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

package com.treode.store.catalog

import com.treode.async.Async
import com.treode.store.CatalogId

import Async.async
import Proposer.{accept, chosen, grant, refuse}

private class Proposers (kit: CatalogKit) {
  import kit.cluster
  import kit.library.releaser

  private val proposers = newProposersMap

  def get (key: CatalogId, version: Int): Proposer = {
    var p0 = proposers.get ((key, version))
    if (p0 != null)
      return p0
    val p1 = new Proposer (key, version, kit)
    p0 = proposers.putIfAbsent ((key, version), p1)
    if (p0 != null)
      return p0
    p1
  }

  def remove (key: CatalogId, version: Int, p: Proposer): Unit =
    proposers.remove ((key, version), p)

  def propose (ballot: Long, key: CatalogId, patch: Patch): Async [Patch] =
    releaser.join {
      async { cb =>
        val p = get (key, patch.version)
        p.open (ballot, patch)
        p.learn (cb)
      }}

  def attach() {

    refuse.listen { case ((key, version, ballot), c) =>
      get (key, version) refuse (ballot)
    }

    grant.listen { case ((key, version, ballot, proposal), c) =>
      get (key, version) grant (c, ballot, proposal)
    }

    accept.listen { case ((key, version, ballot), c) =>
      get (key, version) accept (c, ballot)
    }

    chosen.listen { case ((key, version, v), _) =>
      get (key, version) chosen (v)
    }}}
