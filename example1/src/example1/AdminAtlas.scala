package example1

import com.fasterxml.jackson.databind.JsonNode
import com.treode.store.{Cohort, Store}
import com.twitter.finatra.{Controller => FinatraController}

import Cohort.{Issuing, Moving, Settled}
import Store.Controller
import scala.util.parsing.json.JSONObject

import com.treode.cluster.HostId

class AdminAtlas (controller: Controller) extends FinatraController {

  get ("/atlas") { request =>
    val json = textJson.convertValue (controller.cohorts.toArray, classOf [JsonNode])
    render.json (json) .toFuture
  }

  put ("/atlas") { request =>
    val cohorts = textJson.convertValue (request.readJson(), classOf [Array [Cohort]])
    controller.cohorts = cohorts
    render.ok.toFuture
  }}
