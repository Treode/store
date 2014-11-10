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
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.{Async, AsyncIterator}, Async.async
import com.treode.async.misc.RichOption
import com.treode.store.{Bytes, TableId, TxClock}
import com.treode.twitter.finagle.http.BadRequestException
import com.treode.twitter.util._
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.netty3.ChannelBufferBuf
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object example {

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

  implicit class ExtraRichRequest (request: Request) {

    def readJson [A: Manifest]: A =
      try {
        request.withReader (textJson.readValue [A] (_))
      } catch {
        case e: JsonProcessingException =>
          throw new BadRequestException (e.getMessage)
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

  implicit class RichString (s: String) {

    def getTableId: TableId =
      TableId.parse (s) .getOrThrow (new BadRequestException (s"Bad table ID: $s"))

    def fromJson [A: Manifest]: A =
      textJson.readValue [A] (s)
  }}
