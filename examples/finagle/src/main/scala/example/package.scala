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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.BatchIterator
import com.treode.async.misc.RichOption
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Bytes, TableId, TxClock}
import com.treode.twitter.finagle.http.{RichResponse, BadRequestException, RichRequest}
import com.twitter.finagle.http.{Request, Response, Status}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import com.twitter.finagle.http.filter.{CommonLogFormatter, LoggingFilter}
import com.twitter.logging.Logger


package object example {

  implicit val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (DefaultTreodeModule)
  textJson.registerModule (AppModule)

  val binaryJson = new ObjectMapper (new SmileFactory) with ScalaObjectMapper
  binaryJson.registerModule (DefaultScalaModule)

  object respond {

    def apply (req: Request, status: HttpResponseStatus = Status.Ok): Response = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def clear (req: Request, status: HttpResponseStatus = Status.Ok): Response  = {
      val rsp = req.response
      rsp.status = status
      rsp.clearContent()
      rsp.contentLength = 0
      rsp
    }

    def json (req: Request, value: Any): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = value
      rsp
    }

    def json (req: Request, time: TxClock, value: Any): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.date = req.requestTxClock
      rsp.lastModified = time
      rsp.readTxClock = req.requestTxClock
      rsp.valueTxClock = time
      rsp.vary = "Request-TxClock"
      rsp.json = value
      rsp
    }

    def json [A] (req: Request, iter: BatchIterator [A]): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = iter
      rsp
    }}

  object LoggingFilter extends LoggingFilter (Logger ("access"), new CommonLogFormatter)

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
