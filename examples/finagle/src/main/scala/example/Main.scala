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

import com.treode.twitter.app.StoreKit
import com.twitter.app.App
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.util.Await

class Serve extends App with StoreKit {

  def main() {

    val resource = new Resource (controller.hostId, controller.store)

    val server = Http.serve (
      ":8080",
      NettyToFinagle andThen
      ExceptionFilter andThen
      BadRequestFilter andThen
      resource)

    onExit {
      server.close()
      controller.shutdown().await()
    }

    Await.ready (server)
  }}
