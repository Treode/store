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

import com.treode.async.implicits._

private abstract class AbstractLatch [A] (private var count: Int, cb: Callback [A]) {

  private var thrown = List.empty [Throwable]

  def value: A

  def init() {
    if (count == 0)
      cb.pass (value)
  }

  def release() {
    require (count > 0, "Latch was already released.")
    count -= 1
    if (count > 0)
      return
    if (!thrown.isEmpty)
      cb.fail (MultiException.fit (thrown))
    else
      cb.pass (value)
  }

  def failure (t: Throwable) {
    thrown ::= t
    release()
  }}
