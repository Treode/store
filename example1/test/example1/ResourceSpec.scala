package example1

import com.treode.store.{Bytes, Cell, StubStore, TxClock, WriteOp, WriteResult}
import com.twitter.finagle.http.MediaType
import com.twitter.finatra.test.{MockApp, MockResult}
import org.scalatest.{FreeSpec, Matchers}

import WriteOp._
import WriteResult._

class ResourceSpec extends FreeSpec with Matchers with SpecTools {

  def newMock (store: StubStore) = {
    val resource = new Resource (store)
    MockApp (resource.delegate)
  }

  "When the database is empty" - {

    "GET /table/123?key=abc should respond Not Found" in {
      val store = StubStore()
      val mock = newMock (store)
      val response = mock.get ("/table/123?key=abc")
      response.code should equal (NotFound)
    }

    "PUT /table/123?key=abc with a condition should respond Ok with an etag" in {
      val body = "\"i have been stored\""
      val store = StubStore()
      val mock = newMock (store)
      val response = mock.put (
          "/table/123?key=abc",
          headers = Map (ContentType -> MediaType.Json),
          body = body)
      response.code should equal (Ok)
      val etag = response.etag
      store.scan (123) should be (Seq (cell ("abc", etag, body)))
    }}

  "When the database has an entry" - {

    val entity = "\"you found me\""

    def setup() = {
      val store = StubStore()
      val mock = newMock (store)
      val WriteResult.Written (ts) =
        store.write (1, TxClock.zero, Seq (Create (123, Bytes ("abc"), entity.readJson.toBytes))) .await
      (store, mock, ts)
    }

    "GET /table/123?key=abc should respond Ok" in {
      val (store, mock, ts) = setup()
      val response = mock.get ("/table/123?key=abc")
      response.code should equal (Ok)
      response.etag should be (ts)
      response.body should be (entity)
    }

    "GET /table/123?key=abc with If-Modified-Since:0 should respond Ok" in {
      val (store, mock, ts) = setup()
      val response = mock.get (
          "/table/123?key=abc",
          headers = Map (IfModifiedSince -> "0"))
      response.code should equal (Ok)
      response.etag should be (ts)
      response.body should be (entity)
    }

    "GET /table/123?key=abc with If-Modified-Since:1 should respond Not Modified" in {
      val (store, mock, ts) = setup()
      val response = mock.get (
          "/table/123?key=abc",
          headers = Map (IfModifiedSince -> "1"))
      response.code should equal (NotModified)
      response.body should be ("")
    }

    "GET /table/123?key=abc with Last-Modification-Before:0 should respond Not Found" in {
      val (store, mock, ts) = setup()
      val response = mock.get (
          "/table/123?key=abc",
          headers = Map (LastModificationBefore -> (ts-1).toString))
      response.code should equal (NotFound)
    }

    "GET /table/123?key=abc with Last-Modification-Before:1 should respond Ok" in {
      val (store, mock, ts) = setup()
      val response = mock.get (
          "/table/123?key=abc",
          headers = Map (LastModificationBefore -> "1"))
      response.code should equal (Ok)
      response.etag should be (ts)
      response.body should be (entity)
    }

    "PUT /table/123?key=abc with should respond Ok with an etag" in {
      val (store, mock, ts) = setup()
      val body2 = "\"i have been stored\""
      val response = mock.put (
          "/table/123?key=abc",
          headers = Map (ContentType -> MediaType.Json),
          body = body2)
      response.code should equal (Ok)
      val etag = response.etag
      store.scan (123) should be (Seq (cell ("abc", etag, body2), cell ("abc", ts, entity)))
    }

    "PUT /table/123?key=abc with a If-Unmodified-Since:1 should respond Ok with an etag" in {
      val (store, mock, ts) = setup()
      val body2 = "\"i have been stored\""
      val response = mock.put (
          "/table/123?key=abc",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> "1"),
          body = body2)
      response.code should equal (Ok)
      val etag = response.etag
      store.scan (123) should be (Seq (cell ("abc", etag, body2), cell ("abc", ts, entity)))
    }

    "PUT /table/123?key=abc with a If-Unmodified-Since:0 should respond Precondition Failed" in {
      val (store, mock, ts) = setup()
      val body2 = "\"i have been stored\""
      val response = mock.put (
          "/table/123?key=abc",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> "0"),
          body = body2)
      response.code should equal (PreconditionFailed)
      store.scan (123) should be (Seq (cell ("abc", ts, entity)))
    }}}
