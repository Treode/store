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

package com.treode

import scala.util.{Try => ScalaTry, Failure, Success}

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.store.{Slice, TxClock, TxId}
import com.twitter.finatra.Request
import com.twitter.util.{Try => TwitterTry, Future => TwitterFuture, Promise, Return, Throw}

package finatra {

  class BadRequestException (val message: String) extends Exception {

    override def getMessage(): String = message
  }}

package object finatra {

  implicit class RichAsync [A] (async: Async [A]) {

    def toTwitterFuture: TwitterFuture [A] = {
      val promise = new Promise [A]
      async run {
        case Success (v) => promise.setValue (v)
        case Failure (t) => promise.setException (t)
      }
      promise
    }}

  implicit class RichTwitterTry [A] (v: TwitterTry [A]) {

    def toAsync: Async [A] =
      Async.async (_ (toScalaTry))

    def toTwitterFuture: TwitterFuture [A] =
      TwitterFuture.const (v)

    def toScalaTry: ScalaTry [A] =
      v match {
        case Return (v) => Success (v)
        case Throw (t) => Failure (t)
      }}}
