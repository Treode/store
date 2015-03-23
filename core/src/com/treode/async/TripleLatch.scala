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

package com.treode.async

import scala.util.{Failure, Success}

import com.treode.async.implicits._

private class TripleLatch [A, B, C] (cb: Callback [(A, B, C)]) {

  private var count = 3
  private var thrown = List.empty [Throwable]
  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]
  private var vc: C = null.asInstanceOf [C]

  private [this] def release() {
    count -= 1
    if (count > 0)
      return
    if (!thrown.isEmpty)
      cb.fail (MultiException.fit (thrown))
    else
      cb.pass ((va, vb, vc))
  }

  private [this] def failure (t: Throwable): Unit = synchronized {
    thrown ::= t
    release()
  }

  val cbA: Callback [A] = {
    case Success (v) => synchronized {
      require (va == null, "Value 'a' was already set.")
      va = v
      release()
    }
    case Failure (t) =>
      failure (t)
  }

  val cbB: Callback [B] = {
    case Success (v) => synchronized {
      require (vb == null, "Value 'b'  was already set.")
      vb = v
      release()
    }
    case Failure (t) =>
      failure (t)
  }

  val cbC: Callback [C] = {
    case Success (v) => synchronized {
      require (vc == null, "Value 'c' was already set.")
      vc = v
      release()
    }
    case Failure (t) =>
      failure (t)
  }}
