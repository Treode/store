package com.treode.server

import org.scalatest._
import scala.collection.mutable.{HashMap}
import SchemaParser._

class ServerParserSpec extends FreeSpec with Matchers {

  def assertSuccess (expected: Schema, s: String): Unit = {
    assertResult (CompilerSuccess (expected)) { parse (s) }
  }

  def assertFailure (expected: Seq [Message], s: String): Unit = {
    assertResult (CompilerFailure (expected)) { parse (s) }
  }

  "When inputs are correct" - {
    "The parse should parse table names successfully" in {
      var map = new HashMap [String, Long] 
      map += ("table_1-v" -> 1)
      assertSuccess (new Schema (map) , "table table_1-v { id : \t 1 \n }")
    }
    "The parse should parse octal, hexadecimal and decimal id successfully" in {
      var map = new HashMap [String, Long] 
      map += ("table1" -> 1)
      map += ("table2" -> 31)
      map += ("table3" -> 25)
      assertSuccess (new Schema (map) , "table table1 { id : \t 1 \n } table table2 {id : 0x1F} \n table table3 {id : 031} \t ")
    }
  }

  /*
  "When inputs are incorrect" - {
    "The parser should report one error" in {
      assertFailure (Seq.empty [Message], "table top {id : }")
    }
    "The parser should report lexical error" in {
      assertFailure (Seq.empty [Message], "1table top {id : 0z56}")
    }
    "The parser should report two errors" in {
      assertFailure (Seq.empty [Message], "table top {id : } top2 {id: 1}")
    }
    "The parser should report three errors" in {
      assertFailure (Seq.empty [Message], "table top id : 5} table top1 {id: 09} table top2 {id 5}")
    }
    "The parser should report four errors" in {
      assertFailure (Seq.empty [Message], "table top {id : 5} table top1 id: 34 tabe top2 {id : 5}")
    }}
  */
  }
