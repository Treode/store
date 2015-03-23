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

import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

import Async.async

/** An asynchronous future. */
class Future [A] extends Async [A] with Callback [A] {

  private var callbacks = new ArrayList [Callback [A]]
  private var value: Try [A] = null

  def apply (v: Try [A]): Unit = synchronized {
    require (value == null, "Future was already set.")
    value = v
    val callbacks = this.callbacks
    this.callbacks = null
    callbacks foreach (_ (v))
  }

  def run (cb: Callback [A]): Unit = synchronized {
    if (value != null)
      cb (value)
    else
      callbacks.add (cb)
  }}
