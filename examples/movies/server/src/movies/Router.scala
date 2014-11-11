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

package movies

import com.treode.async.Async
import com.treode.twitter.util._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{HttpMuxer, Request, Response}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

class Router {

  private val services = Seq.newBuilder [(String, Service [HttpRequest, HttpResponse])]

  private def make (service: Request => Async [Response]): Service [HttpRequest, HttpResponse] =
    new Service [HttpRequest, HttpResponse] {
      def apply (request: HttpRequest): Future [HttpResponse] =
        service (request.asInstanceOf [Request]) .toTwitterFuture
    }

  def register (path: String) (service: Request => Async [Response]): Unit =
    services += path -> make (service)

  def result: Service [Request, Response] =
    new Service [Request, Response] {
      val muxer = new HttpMuxer (services.result)
      def apply (request: Request): Future [Response] =
        muxer (request) .map (_.asInstanceOf [Response])
    }}
