package example1

import com.fasterxml.jackson.databind.JsonNode
import com.treode.store.{Bytes, Cell, TxClock}
import com.twitter.finatra.test.MockResult

trait SpecTools {

  def cell (key: String, time: TxClock): Cell =
    Cell (Bytes (key), time, None)

  def cell (key: String, time: TxClock, json: String): Cell =
    Cell (Bytes (key), time, Some (json.readJson.toBytes))

  implicit class RichMockResult (result: MockResult) {

    def etag: TxClock = {
      val string = result.getHeaders.get (ETag)
      assert (string.isDefined, "Expected response to have an ETag.")
      val parse = TxClock.parse (string.get)
      assert (parse.isDefined, s"""Could not parse ETag "${string.get}" as a TxClock""")
      parse.get
    }}}
