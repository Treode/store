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
import com.treode.async.BatchIterator
import com.treode.async.misc.{RichOption, parseInt}
import com.treode.cluster.HostId
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Slice, TxClock, TxId, Window}
import com.treode.twitter.util.RichTwitterFuture
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.netty3.ChannelBufferBuf
import org.jboss.netty.buffer.{ChannelBufferOutputStream, ChannelBuffers}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.joda.time.format.DateTimeFormat

package object http {

  /** A [[com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper ScalaObjectMapper]]
    * with the [[com.fasterxml.jackson.module.scala.DefaultScalaModule DefaultScalaModule]]
    * and the [[com.treode.jackson.DefaultTreodeModule DefaultTreodeModule]].
    */
  implicit val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule (DefaultScalaModule)
  mapper.registerModule (DefaultTreodeModule)

  private val httpDate = DateTimeFormat.forPattern ("EEE, d MMM HH:mm:ss y zzz");

  implicit class RichResponse (rsp: Response) {

    /** Do not use; necessary for Scala style setter. */
    def json: Any =
      throw new UnsupportedOperationException

    def json_= [A] (value: A) (implicit mapper: ObjectMapper) {
      rsp.mediaType = MediaType.Json
      rsp.write (mapper.writeValueAsString (value))
      rsp.close()
    }

    def json_= [A] (iter: BatchIterator [A]) (implicit mapper: ObjectMapper) {
      rsp.mediaType = MediaType.Json
      rsp.setChunked (true)
      val writer = mapper.writer()
      var first = true
      iter.batch { vs =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val stream = new ChannelBufferOutputStream (buffer)
        for (v <- vs) {
          if (first) {
            first = false
            stream.writeByte ('[')
          } else {
            stream.writeByte (',')
          }
          writer.writeValue (stream, v)
        }
        stream.flush()
        rsp.writer.write (ChannelBufferBuf.Owned (buffer)) .toAsync
      } .flatMap { _ =>
        val buffer = ChannelBuffers.dynamicBuffer()
        val stream = new ChannelBufferOutputStream (buffer)
        if (first)
          stream.writeByte ('[')
        stream.writeByte (']')
        stream.flush()
        rsp.writer.write (ChannelBufferBuf.Owned (buffer)) .toAsync
      } .run {
        case Success (_) =>
          rsp.close()
        case Failure (t) =>
          rsp.close()
          throw t
      }}

    /** Do not use; necessary for Scala style setter. */
    def date: TxClock =
        throw new UnsupportedOperationException

    def date_= (time: TxClock): Unit =
      rsp.headerMap.add ("Date", httpDate.print (time.toDateTime))

    /** Do not use; necessary for Scala style setter. */
    def lastModified: TxClock =
      throw new UnsupportedOperationException

    def lastModified_= (time: TxClock): Unit =
      rsp.headerMap.add ("Last-Modified", httpDate.print (time.toDateTime))

    /** Do not use; necessary for Scala style setter. */
    def readTxClock: TxClock =
      throw new UnsupportedOperationException

    def readTxClock_= (time: TxClock): Unit =
      rsp.headerMap.add ("Read-TxClock", time.time.toString)

    /** Do not use; necessary for Scala style setter. */
    def valueTxClock: TxClock = 
      throw new UnsupportedOperationException

    def valueTxClock_= (time: TxClock): Unit =
      rsp.headerMap.add ("Value-TxClock", time.time.toString)

    /** Do not use; necessary for Scala style setter. */
    def vary: String =
      throw new UnsupportedOperationException

    def vary_= (vary: String): Unit =
      rsp.headerMap.add ("Vary", vary)

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

    private def optTxClockParam (name: String): Option [TxClock] =
      request.params.get (name) .map { value =>
        TxClock.parse (value) .getOrThrow (new BadRequestException (s"Bad time for $name: $value"))
      }

    def conditionTxClock(default: TxClock): TxClock = 
      optTxClockHeader ("Condition-TxClock") getOrElse (default)

    def requestTxClock: TxClock =                    
      optTxClockHeader ("Request-TxClock") getOrElse (TxClock.now)

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

    /** Get window from `since` and `until` query parameters. Window will be `Latest` from `since`
      * exclusive to `until` inclusive. If the query contains no window parameters, the default is
      * `Latest` from 0 to now inclusive.
      */
    def window: Window = {
      val since = optTxClockParam ("since")
      val until = optTxClockParam ("until")
      if (since.isDefined && until.isDefined) {
        if (since.get > until.get)
          throw new BadRequestException ("Since must preceed until.")
        Window.Latest (until.get, true, since.get, false)
      } else if (since.isDefined || until.isDefined) {
        throw new BadRequestException ("Both since and until are needed together")
      } else {
        Window.Latest (TxClock.now, true, TxClock.MinValue, true)
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
