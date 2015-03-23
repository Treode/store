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

package com.treode.twitter.server.handler

import com.jayway.restassured.RestAssured.given
import com.treode.async.Async, Async.supply
import com.treode.store.{Store, StoreController}
import org.scalatest.FlatSpec

class DrivesHandlerSpec extends FlatSpec with SpecTools {

  def handler (controller: StoreController) =
    new DrivesHandler (controller)

  "The DrivesHandler" should "handle GET" in
    served { case (port, controller) =>
      (controller.drives _) .expects() .returning (supply (Seq.empty))
      given
        .port (port)
      .expect
        .statusCode (200)
        .body (matchesJson ("[]"))
      .when
        .get ("/")
    }

  it should "reject other methods" in
    served { case (port, controller) =>
      given
        .port (port)
      .expect
        .statusCode (405)
      .when
        .post ("/")
    }}
