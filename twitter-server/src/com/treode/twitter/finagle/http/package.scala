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
import scala.util.{Failure, Success}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.AsyncIterator
import com.treode.async.misc.{RichOption, parseInt}
import com.treode.cluster.HostId
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Slice, TxClock, TxId}
import com.treode.twitter.finagle.http.{BadRequestException, RichRequest}
import com.treode.twitter.util.RichTwitterFuture
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.netty3.ChannelBufferBuf
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object http {

  /** A [[com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper ScalaObjectMapper]]
    * with the [[com.fasterxml.jackson.module.scala.DefaultScalaModule DefaultScalaModule]]
    * and the [[com.treode.jackson.DefaultTreodeModule DefaultTreodeModule]].
    */
  implicit val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule (DefaultScalaModule)
  mapper.registerModule (DefaultTreodeModule)

  implicit class RichResponse (rsp: Response) {

    /** Do not use; necessary for Scala style setter. */
    def etag: TxClock =
      throw new UnsupportedOperationException

    def etag_= (time: TxClock): Unit =
      rsp.headerMap.add ("ETag", time.toString)

    /** Do not use; necessary for Scala style setter. */
    def json: Any =
      throw new UnsupportedOperationException

    def json_= [A] (value: A) (implicit mapper: ObjectMapper) {
      rsp.mediaType = MediaType.Json
      rsp.write (mapper.writeValueAsString (value))
      rsp.close()
    }

    private def buf (msg: String): ChannelBufferBuf =
      ChannelBufferBuf (ChannelBuffers.wrappedBuffer (msg.getBytes ("UTF-8")))

    def json_= [A] (iter: AsyncIterator [A]) (implicit mapper: ObjectMapper) {
      rsp.mediaType = MediaType.Json
      rsp.setChunked (true)
      var first = true
      iter.foreach { v =>
        if (first) {
          first = false
          rsp.writer.write (buf ("[" + mapper.writeValueAsString (v))) .toAsync
        } else {
          rsp.writer.write (buf ("," + mapper.writeValueAsString (v))) .toAsync
        }
      } .flatMap { _ =>
        if (first)
          rsp.writer.write (buf ("[]")) .toAsync
        else
          rsp.writer.write (buf ("]")) .toAsync
      } .run {
        case Success (_) =>
          rsp.close()
        case Failure (t) =>
          rsp.close()
          throw t
      }}

    def plain_= (value: String) {
      rsp.mediaType = "text/plain"
      rsp.write (value)
      rsp.close()
    }}

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

    def ifModifiedSince: TxClock =
      optTxClockHeader ("If-Modified-Since") getOrElse (TxClock.MinValue)

    def ifUnmodifiedSince: TxClock =
      optTxClockHeader ("If-Unmodified-Since") getOrElse (TxClock.now)

    def lastModificationBefore: TxClock =
      optTxClockHeader ("Last-Modification-Before") getOrElse (TxClock.now)

    def slice: Slice = {
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

    def transactionId (host: HostId): TxId =
      request.headerMap.get ("Transaction-ID") match {
        case Some (tx) =>
          TxId
            .parse (tx)
            .getOrElse (throw new BadRequestException (s"Bad Transaction-ID: $tx"))
        case None =>
          TxId.random (host)
      }

    def readJson [A: Manifest] (implicit mapper: ScalaObjectMapper): A =
      request.withReader (mapper.readValue [A] (_))
  }}
