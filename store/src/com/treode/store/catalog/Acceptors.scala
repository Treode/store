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

import com.treode.store.CatalogId

private class Acceptors (kit: CatalogKit) {
  import kit.{cluster, disk}
  import kit.library.atlas

  val acceptors = newAcceptorsMap

  def get (key: CatalogId, version: Int): Acceptor = {
    var a0 = acceptors.get ((key, version))
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, version, kit)
    a1.state = new a1.Opening
    a0 = acceptors.putIfAbsent ((key, version), a1)
    if (a0 != null) {
      a1.dispose()
      return a0
    }
    a1
  }

  def remove (key: CatalogId, version: Int, a: Acceptor): Unit =
    acceptors.remove ((key, version), a)

  def attach() {
    import Acceptor.{ask, choose, propose}

    ask.listen { case ((av, key, cv, ballot, default), c) =>
      if (atlas.version - 1 <= av && av <= atlas.version + 1)
        get (key, cv) ask (c, ballot, default)
    }

    propose.listen { case ((av, key, cv, ballot, value), c) =>
      if (atlas.version - 1 <= av && av <= atlas.version + 1)
        get (key, cv) propose (c, ballot, value)
    }

    choose.listen { case ((key, version, chosen), c) =>
      get (key, version) choose (chosen)
    }}}
