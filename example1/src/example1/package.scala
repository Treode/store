
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.twitter.finatra.Request
import com.twitter.util.{Future, Try}

package example1 {

  class BadRequestException (val message: String) extends Exception
}

package object example1 {

  private val mapper = new ObjectMapper()

  implicit class RichRequest (request: Request) {

    def readJson(): Try [JsonNode] =
      Try {
        if (request.contentType != Some ("application/json"))
          throw new BadRequestException ("Expected JSON entity.")
        request.withReader (mapper.readTree _)
      } rescue {
        case e: JsonParseException =>
          throw new BadRequestException (e.getMessage)
      }}

  implicit class RichTry [A] (v: Try [A]) {

    def toFuture: Future [A] =
      Future.const (v)
  }}
