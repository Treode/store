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

package com.treode.twitter.finagle.http.filter

import scala.util.Random

import com.jayway.restassured.RestAssured.given
import com.treode.cluster.{Peer, RumorDescriptor}
import com.treode.store.{Store, StoreController}
import com.twitter.finagle.Http
import com.twitter.finagle.http.service.NullService
import org.hamcrest.Matchers, Matchers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class PeersFilterSpec extends FlatSpec with MockFactory {

  def served (test: (Int, StoreController) => Any) {
    val store = mock [Store]

    val controller = mock [StoreController]
    (controller.listen (_: RumorDescriptor [Any]) (_: (Any, Peer) => Any))
        .expects(*, *)
        .anyNumberOfTimes()
    (controller.store _)
        .expects()
        .returning (store)
        .anyNumberOfTimes()

    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      PeersFilter ("/peers", controller) andThen
      NullService)
    try {
      test (port, controller)
    } finally {
      server.close()
    }}

  "The PeersFilter" should "handle GET" in
    served { case (port, controller) =>
      (controller.hosts _) .expects (*) .returning (Seq.empty)
      given
        .port (port)
      .expect
        .statusCode (200)
        .body (equalTo ("[]"))
      .when
        .get ("/peers")
    }

  it should "reject other methods" in
    served { case (port, controller) =>
      given
        .port (port)
      .expect
        .statusCode (405)
      .when
        .put ("/peers")
    }


  it should "pass unrecognized paths through" in
    served { case (port, controller) =>
      given
        .port (port)
      .expect
        .statusCode (200)
      .when
        .put ("/path")
    }}
