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

private class PairLatch [A, B] (cb: Callback [(A, B)])
extends AbstractLatch [(A, B)] (2, cb) {

  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]

  def value: (A, B) = (va, vb)

  init()

  val cbA: Callback [A] = {
    case Success (v) => synchronized {
        require (va == null, "Value 'a' was already set.")
        va = v
        release()
    }
    case Failure (t) => synchronized {
      failure (t)
    }}

  val cbB: Callback [B] = {
    case Success (v) => synchronized {
      require (vb == null, "Value 'b'  was already set.")
      vb = v
      release()
    }
    case Failure (t) => synchronized {
      failure (t)
    }}}
