package com.treode.server

import org.scalatest._
import scala.collection.mutable.{HashMap}
import scala.util.parsing.input.OffsetPosition
import SchemaParser._

class SchemaParserSpec extends FreeSpec with Matchers {

  def assertSuccess (s: String, expected: Schema): Unit = {
    assertResult (CompilerSuccess (expected)) { parse (s) }
  }

  def assertFailure (s: String, expected: List [Message]): Unit = {
    assertResult (CompilerFailure (expected)) { parse (s) }
  }

  "When inputs are correct" - {

    "The parse should parse table names successfully" in {
      var map = new HashMap [String, Long]
      map += ("table_1v" -> 1)
      assertSuccess ("table table_1v { id : \t 1 \n; };", Schema (map))
    }

    "The parse should parse octal, hexadecimal and decimal id successfully" in {
      var map = new HashMap [String, Long]
      map += ("table1" -> 1)
      map += ("table2" -> 31)
      map += ("table3" -> 25)
      assertSuccess ("table table1 { id : \t 1 \n ;} ; table table2 {id : 0x1F;} ;\n table table3 {id : 031;} ;\t ", Schema (map))
    }}

  "When inputs are incorrect" - {

    "The parser should report one error" in {
      val input = "table top-1\n{  id : 9 ; };"
      assertFailure (input, List (ParseError ("Bad definition of clause, expected\ntable <tablename> {\n  Field1;Field2;..\n};\nstarting", OffsetPosition (input,0))))
    }

    "The parser should report two errors" in {
      val input = "tabe top2\n{\n  id: 10 }; table top3 { id: 10 ; ;"
      assertFailure (input, List (ParseError ("Bad definition of clause, expected\ntable <tablename> {\n  Field1;Field2;..\n};\nstarting", OffsetPosition (input,0)), ParseError ("Bad definition of clause, expected\ntable <tablename> {\n  Field1;Field2;..\n};\nstarting", OffsetPosition (input,24)) ))
    }

    "The parser should report three errors" in {
      val input = "table top\n{\n  i:9;\n; \ntable top2\n{\n  id:g;\n};\ntable top3 {\n  id  \t 12 ;\n}\n;"
      assertFailure (input, List (ParseError ("Bad definition of clause, expected\ntable <tablename> {\n  Field1;Field2;..\n};\nstarting", OffsetPosition (input,0)), ParseError ("Expected long", OffsetPosition (input,40)), ParseError ("Id field improper, expected\nid : <long> ;\nbut found", OffsetPosition (input,61)) ))
    }}}
