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

import java.util.concurrent.TimeoutException

/** Collects multiple exceptions into one.  The collected exceptions can be retrieved from
  * `getSuppressed`.
  */
class MultiException extends Exception

object MultiException {

  /** Create a MultiException, even if the sequence has zero or one exceptions. */
  def apply (ts: Seq [Throwable]): MultiException = {
    val e = new MultiException
    for (t <- ts)
      e.addSuppressed (t)
    e
  }

  /** Create a MultiException unless the sequence has one exception. */
  def fit (ts: Seq [Throwable]): Throwable = {
    if (ts.size == 0)
      new NoSuchElementException
    else if (ts.size == 1)
      ts.head
    else if (ts forall (_.isInstanceOf [TimeoutException]))
      ts.head
    else
      apply (ts)
  }}
