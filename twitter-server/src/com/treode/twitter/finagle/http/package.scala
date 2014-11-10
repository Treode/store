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

package com.treode.twitter.finagle

import java.lang.Long.highestOneBit

import com.treode.async.misc.{RichOption, parseInt}
import com.treode.cluster.HostId
import com.treode.store.{Slice, TxClock, TxId}
import com.twitter.finagle.http.Request

package object http {

  class BadRequestException (val message: String) extends Exception {

    override def getMessage(): String = message
  }

  implicit class RichRequest (request: Request) {

    private def optIntParam (name: String): Option [Int] =
      request.params.get (name) .map { value =>
        parseInt (value) .getOrThrow (new BadRequestException (s"Bad integer for $name: $value"))
      }

    private def optTxClockHeader (name: String): Option [TxClock] =
      request.headerMap.get (name) .map { value =>
        TxClock.parse (value) .getOrThrow (new BadRequestException (s"Bad time for $name: $value"))
      }

    def getIfModifiedSince: TxClock =
      optTxClockHeader ("If-Modified-Since") getOrElse (TxClock.MinValue)

    def getIfUnmodifiedSince: TxClock =
      optTxClockHeader ("If-Unmodified-Since") getOrElse (TxClock.now)

    def getLastModificationBefore: TxClock =
      optTxClockHeader ("Last-Modification-Before") getOrElse (TxClock.now)

    def getSlice: Slice = {
      val slice = optIntParam ("slice")
      val nslices = optIntParam ("nslices")
      if (slice.isDefined && nslices.isDefined) {
        if (nslices.get < 1 || highestOneBit (nslices.get) != nslices.get)
          throw new BadRequestException ("Number of slices must be a power of two and at least one.")
        if (slice.get < 0 || nslices.get <= slice.get)
          throw new BadRequestException ("The slice must be between 0 (inclusive) and the number of slices (exclusive).")
        Slice (slice.get, nslices.get)
      } else if (slice.isDefined || nslices.isDefined) {
        throw new BadRequestException ("Both slice and nslices are needed together")
      } else {
        Slice.all
      }}

    def getTransactionId (host: HostId): TxId =
      request.headerMap.get ("Transaction-ID") match {
        case Some (tx) =>
          TxId
            .parse (tx)
            .getOrElse (throw new BadRequestException (s"Bad Transaction-ID value: $tx"))
        case None =>
          TxId.random (host)
      }}
}
