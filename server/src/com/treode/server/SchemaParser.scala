package com.treode.server
import scala.util.{Success, Failure}
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.Position
import scala.collection.Iterator
import scala.collection.mutable.{HashMap, MutableList}
import scala.util.parsing.combinator._
import scala.collection.mutable.Builder
import scala.util.parsing.input.Positional
import com.treode.async.misc.parseUnsignedLong

object SchemaParser extends RegexParsers {

  trait CompilerResult

  case class CompilerSuccess (schema: Schema) extends CompilerResult

  case class CompilerFailure (errors: List [Message]) extends CompilerResult

  sealed abstract class Message {
    def message: String
    def position: Position
    override def toString = message + " at line " + position.line + "\n" + position.longString
  }

	case class ParseError (message: String, position: Position) extends Message

  case class Ident (ident: Option [String]) extends Positional {
    override def toString = ident getOrElse ("Non-identifier")
  }

  case class Number (number: Option [String]) extends Positional {
    override def toString = number getOrElse ("Non-Number")
  }

  sealed abstract class TableClause extends Positional
  sealed abstract class ResourceClause extends Positional
  sealed abstract class TopClause extends Positional

  case class Table (ident: Ident, clauses: List [TableClause]) extends TopClause {
    override def toString = s"table $ident"
  }

  case class Resource (regex: String, clauses: List [ResourceClause]) extends TopClause {
    override def toString = s"resource $regex"
  }

  case class SkippedTopClause (name: String) extends TopClause {
    override def toString = name
    def templateDefinition: String = "table <tablename> {\n  Field1;Field2;..\n};\n"
  }

  case class SkippedTableDirective (what: String) extends TableClause {
    override def toString = what
  }

  case class IdDirective (id: Number) extends TableClause {
    override def toString = s"idDirective $id"
  }

  def idDirective: Parser [TableClause] = positioned [TableClause] {
    ("id" ~> ( ":" ~> number <~ ";" ) ) ^^ {
      case num => {
        IdDirective (num)
      }
    } |
    "[^;^}]+[;]?".r ^^ (v => { SkippedTableDirective ("Id field improper, expected\nid : <long> ;\nbut found")})
  }

  def number: Parser [Number] = positioned [Number] {
    """((0[xX][0-9a-fA-F]+)|([0-9]+))""".r ^^ { v => Number (Some (v)) } |
    rep ("""[\p{Alnum}]+""".r) ^^ (_ => {Number (None)}) |
    success (Number (None))
  }

  def ident: Parser [Ident] = positioned [Ident] {
    """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*""".r ^^ (ident => { (Ident (Some (ident)))}) |
    rep ("""[\p{Alnum}]+""".r) ^^ (v => { Ident (None)}) |
    rep ("[^\\t\\n {};]".r) ^^ (v => { Ident (None)} )
  }

  def string: Parser [String] =
    ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"\"").r

  def topClause: Parser [TopClause] = positioned [TopClause] {
    ("table" ~> ident ) ~ ("{" ~> idDirective <~ "}") <~ ";" ^^ {case i~v => {Table (i, List (v))}} |
    ("resource" ~> string <~ ";") ^^ (Resource (_, List.empty)) |
    "([^;}]+[;} \\n\\t]*)|([^;}]*[;} \\n\\t]+)".r ^^ (v => { SkippedTopClause ("table definition")})
  }

  def collectErrors (builder: Builder [Message, _], clauses: List [TopClause]): Unit = {
    for (clause <- clauses) {
      clause match {
        case sk: SkippedTopClause => {
          builder += ParseError ("Bad definition of clause, expected\n" + sk.templateDefinition + "starting", sk.pos)
        }
        case Table (name, clauses) => {
          for (td <- clauses) {
            td match {
              case SkippedTableDirective (message) => builder += ParseError (message, td.pos)
              case IdDirective (id) => {
                id match {
                  case Number (None) => builder += ParseError ("Expected long", id.pos)
                  case _ => ()
                }}
              case _ => ()
            }}}
        case _ => ()
      }}}

  trait ParserOutput

  case class ParserSuccess (clauses: List [TopClause]) extends ParserOutput

  case class ParserFailure (messages: List [Message]) extends ParserOutput

  def buildParseTree (input: String): ParserOutput = {
    val clauses = parseAll (rep (topClause), input)
    clauses match {
      case Error (message, next) =>
        { ParserFailure (List (ParseError (message, next.pos))) }
      case Failure (message, next) =>
        { ParserFailure (List (ParseError (message, next.pos))) }
      case Success (clausesList, _) => {
        val builder = List.newBuilder [Message]
        collectErrors (builder, clausesList)
        val errors = builder.result
        if (errors.size > 0) {
          ParserFailure (errors)
        } else {
          ParserSuccess (clausesList)
        }}}}

  trait SemanticAnalysisResult

  case class SemanticAnalysisSuccess (map: HashMap [String, Long]) extends SemanticAnalysisResult

  case class SemanticAnalysisFailure (errors: List [Message]) extends SemanticAnalysisResult

  def semanticAnalysis (clauses: List [TopClause]): SemanticAnalysisResult = {
    val builder = List.newBuilder [Message]
    val map = HashMap [String, Long] ()
    for (clause <- clauses) {
      clause match {
        case Table (tableName, directives) => {
          val name = (tableName.ident match {
            case Some (name) => name
            case None => ""
          })
          val dupName = map.keySet.contains (name)
          if (dupName) {
            builder += ParseError ("Duplicate table name " + name, clause.pos)
          }
          for (directive <- directives) {
            directive match {
              case IdDirective (num) => {
                val id = (num match {
                  case Number (Some (nu)) => nu
                  case _ => ""
                })
                parseUnsignedLong (id) match {
                  case Some (v) => {
                    val dupId = map.exists (_._2 == v)
                    if (dupId) {
                      builder += ParseError ("Duplicate id ", directive.pos)
                    } else if (!dupName) {
                      map += (name -> v)
                    }
                  }
                  case _ => {
                    builder += ParseError ("This should be a valid octal (starting with 0), decimal number or hexadecimal number (starting with 0x)", directive.pos)
                  }}}
              case _ => {
                builder += ParseError ("Expected long, not found", directive.pos)
              }}}}
        case _ => ()
      }}
    val errors = builder.result
    if (errors.size > 0) {
      SemanticAnalysisFailure (errors)
    } else {
      SemanticAnalysisSuccess (map)
    }}

  def parse (input: String): CompilerResult = {

    buildParseTree (input) match {
      case ParserSuccess (clauses) => {
        semanticAnalysis (clauses) match {
          case SemanticAnalysisSuccess (map) => {
            CompilerSuccess (Schema (map))
          }
          case SemanticAnalysisFailure (errors) => {
            CompilerFailure (errors)
          }}}
      case ParserFailure (parseErrors) => {
        CompilerFailure (parseErrors)
      }}}
}
