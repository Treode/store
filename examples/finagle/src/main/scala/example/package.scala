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

import java.lang.Integer.highestOneBit
import scala.util.{Failure, Success}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JavaType, JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.{Async, AsyncIterator}, Async.async
import com.treode.async.implicits._
import com.treode.async.misc.{RichOption, parseInt, parseUnsignedLong}
import com.treode.cluster.{CellId, HostId}
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Bytes, Slice, TableId, TxClock, TxId}
import com.twitter.app.Flaggable
import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpResponseStatus}

package object example {

  implicit val flaggableCellId: Flaggable [CellId] =
    Flaggable.mandatory (s => CellId (parseUnsignedLong (s) .get))

  implicit val flaggableHostId: Flaggable [HostId] =
    Flaggable.mandatory (s => HostId (parseUnsignedLong (s) .get))

  val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (AppModule)

  val binaryJson = new ObjectMapper (new SmileFactory)

  val IfModifiedSince = "If-Modified-Since"
  val IfUnmodifiedSince = "If-Unmodified-Since"
  val LastModificationBefore = "Last-Modification-Before"
  val TransactionId = "Transaction-ID"

  object respond {

    def apply (req: Request, status: HttpResponseStatus) = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def clear (req: Request, status: HttpResponseStatus) = {
      val rsp = req.response
      rsp.status = status
      rsp.clearContent()
      rsp.contentLength = 0
      rsp
    }

    def json (req: Request, status: HttpResponseStatus, value: Any) = {
      val rsp = req.response
      rsp.status = status
      rsp.mediaType = MediaType.Json
      rsp.write (value.toJsonText)
      rsp.close()
      rsp
    }

    def json (req: Request, status: HttpResponseStatus, time: TxClock, value: Any) = {
      val rsp = req.response
      rsp.status = status
      rsp.mediaType = MediaType.Json
      rsp.headerMap.add ("ETag", time.toString)
      rsp.write (value.toJsonText)
      rsp.close()
      rsp
    }

    private def buf (msg: String): ChannelBufferBuf =
      ChannelBufferBuf (ChannelBuffers.wrappedBuffer (msg.getBytes ("UTF-8")))

    def json [A] (req: Request, status: HttpResponseStatus, iter: AsyncIterator [A]) = {
      val rsp = req.response
      rsp.status = status
      rsp.mediaType = MediaType.Json
      rsp.setChunked (true)
      var first = true
      iter.foreach { v =>
        if (first) {
          first = false
          rsp.writer.write (buf ("[" + v.toJsonText)) .toAsync
        } else {
          rsp.writer.write (buf ("," + v.toJsonText)) .toAsync
        }
      } .flatMap { _ =>
        rsp.writer.write (buf ("]")) .toAsync
      } .run {
        case Success (_) =>
          rsp.close()
        case Failure (t) =>
          rsp.close()
          throw t
      }
      rsp
    }

    def plain (req: Request, status: HttpResponseStatus, msg: String) = {
      val rsp = req.response
      rsp.status = status
      rsp.mediaType = "text/plain"
      rsp.write (msg)
      rsp.close()
      rsp
    }}

  object BadRequestFilter extends SimpleFilter [Request, Response] {

    def apply (req: Request, service: Service [Request, Response]): Future [Response] =
      Future.monitored {
        service (req)
      } .rescue {
        case e: BadRequestException =>
          Future.value (respond.plain (req, Status.BadRequest, e.getMessage))
      }}

  class BadRequestException (val message: String) extends Exception {

    override def getMessage(): String = message
  }

  object NettyToFinagle extends Filter [HttpRequest, HttpResponse, Request, Response] {

    def apply (req: HttpRequest, service: Service [Request, Response]): Future [HttpResponse] =
      service (req.asInstanceOf [Request])
  }

  implicit class RichAsync [A] (async: Async [A]) {

    def toTwitterFuture: Future [A] = {
      val promise = new Promise [A]
      async run {
        case Success (v) => promise.setValue (v)
        case Failure (t) => promise.setException (t)
      }
      promise
    }}

  implicit class RichAny (v: Any) {

    def toJsonText: String =
      textJson.writeValueAsString (v)
  }

  implicit class RichBytes (bytes: Bytes) {

    def toJsonNode: JsonNode =
      binaryJson.readValue (bytes.bytes, classOf [JsonNode])
  }

  implicit class RichJsonNode (node: JsonNode) {

    def toBytes: Bytes =
      Bytes (binaryJson.writeValueAsBytes (node))
  }

  implicit class RichRequest (request: Request) {

    def optIntParam (name: String): Option [Int] =
      request.params.get (name) .map { value =>
        parseInt (value) .getOrThrow (new BadRequestException (s"Bad value for $name: $value"))
      }

    def getIfModifiedSince: TxClock =
      request.headerMap.get (IfModifiedSince) match {
        case Some (ct) =>
          TxClock
            .parse (ct)
            .getOrElse (throw new BadRequestException (s"Bad If-Modified-Since value: $ct"))
        case None =>
          TxClock.MinValue
      }

    def getIfUnmodifiedSince: TxClock =
      request.headerMap.get (IfUnmodifiedSince) match {
        case Some (ct) =>
          TxClock
            .parse (ct)
            .getOrElse (throw new BadRequestException (s"Bad If-Unmodified-Since value: $ct"))
        case None =>
          TxClock.now
      }

    def getLastModificationBefore: TxClock =
      request.headerMap.get (LastModificationBefore) match {
        case Some (rt) =>
          TxClock
            .parse (rt)
            .getOrElse (throw new BadRequestException (s"Bad Last-Modification-Before value: $rt"))
        case None =>
          TxClock.now
      }

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
      request.headerMap.get (TransactionId) match {
        case Some (tx) =>
          TxId
            .parse (tx)
            .getOrElse (throw new BadRequestException (s"Bad Transaction-ID value: $tx"))
        case None =>
          TxId.random (host)
      }

    def readJson [A: Manifest]: A =
      try {
        request.withReader (textJson.readValue [A] (_))
      } catch {
        case e: JsonProcessingException =>
          throw new BadRequestException (e.getMessage)
      }}

  implicit class RichTwitterFuture [A] (fut: Future [A]) {

    def toAsync: Async [A] =
      async { cb =>
        fut.respond {
          case Return (v) => cb.pass (v)
          case Throw (t) => cb.fail (t)
        }}
  }

  implicit class RichString (s: String) {

    def getTableId: TableId =
      TableId (
        parseUnsignedLong (s)
        .getOrThrow (new BadRequestException (s"Bad table ID: $s")))

    def fromJson [A: Manifest]: A =
      textJson.readValue [A] (s)
  }}
