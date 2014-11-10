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

package example

import scala.util.{Failure, Success}

import com.treode.async.Async, Async.guard
import com.treode.twitter.finagle.http.BadRequestException
import io.netty.handler.codec.http.HttpResponse
import unfiltered.Async.Responder
import unfiltered.netty.{ReceivedMessage, ServerErrorResponse}
import unfiltered.netty.async.{Plan => AsyncPlan, RequestPlan}
import unfiltered.request.HttpRequest
import unfiltered.response._

trait Plan extends RequestPlan with ServerErrorResponse {

  def intent: Plan.Intent

  val requestIntent =
    new AsyncPlan.Intent {

      def apply (req: HttpRequest [ReceivedMessage] with Responder [HttpResponse]): Any =
        guard (intent (req))
        .recover {
          case exc: BadRequestException =>
            BadRequest ~> PlainTextContent ~> ResponseString (exc.getMessage)
        }
        .run {
          case Success (resp) => req.respond (resp)
          case Failure (exc) => throw exc
        }

      def isDefinedAt (req: HttpRequest [ReceivedMessage] with Responder [HttpResponse]): Boolean =
        intent.isDefinedAt (req)
  }}

object Plan {

  type Intent = PartialFunction [
    HttpRequest [ReceivedMessage],
    Async [ResponseFunction [HttpResponse]]]
}
