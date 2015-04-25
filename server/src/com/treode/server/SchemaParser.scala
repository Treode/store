package com.treode.server
import com.treode.twitter.finagle.http.{BadRequestException}
import com.treode.async.misc.parseUnsignedLong
import scala.util.{ Success, Failure }
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.Position
import scala.collection.Iterator
import scala.collection.mutable.{HashMap, MutableList, Set}


object SchemaParser extends LCSSyntaxAnalyzer {

  case class DuplicateMessage (pos: Position, message: String) extends Message
  case class Table (name: String, id: Long)

  trait SemanticAnalyzerOutput
  case class SemanticAnalyzerSuccess (output: Any) extends SemanticAnalyzerOutput
  case class SemanticAnalyzerFailure (errors: Queue [Message]) extends SemanticAnalyzerOutput

  sealed abstract class CompilerResult
  case class CompilerSuccess (schema: Schema) extends CompilerResult
  case class CompilerFailure (errors: Seq [Message]) extends CompilerResult

  class SemanticAnalyzer {
    val idList = Set [Long] ()
    val tableNameList = Set [String] ()
    def apply (ast: ASTNode): SemanticAnalyzerOutput = {
      ast match {
        case ASTId (lng) => {
          val errors = Queue [Message] ()
          parseUnsignedLong (lng.token.chars) match {
            case Some (v) => { 
              if (idList.contains (v)) {
                SemanticAnalyzerFailure (Queue (DuplicateMessage (lng.pos, "Duplicate Id field")))
              } else {
                idList += v; SemanticAnalyzerSuccess (v)
              }
            }
            case None => SemanticAnalyzerFailure ((errors :+ DuplicateMessage (lng.pos, "Expected Long, but not found")))
          }
        }
        case ASTTable (tableName, id) => {
          val errors = Queue [Message] ()
          apply (id) match {
            case SemanticAnalyzerSuccess (id: Long) => {
              if (tableNameList.contains (tableName.token.chars)) {
                SemanticAnalyzerFailure ((errors :+ DuplicateMessage (tableName.pos, "Duplicate table name \"" + tableName.token.chars + "\"")))
              } else {
                tableNameList += tableName.token.chars
                SemanticAnalyzerSuccess (Table (tableName.token.chars, id))
              }
            }
            case SemanticAnalyzerFailure (err) => {
              if (tableNameList.contains (tableName.token.chars)) {
                SemanticAnalyzerFailure ((errors :+ DuplicateMessage (tableName.pos, "Duplicate table name \"" + tableName.token.chars + "\"")) ++ err)
              } else {
                SemanticAnalyzerFailure (errors ++ err)
              }
            }
          }
        }
        case ASTTables (tables) => {
          val tableList = MutableList [Table] ()
          val errors = Queue [Message] ()
          for (table <- tables.asInstanceOf [Queue [ASTTable]]) {
              apply (table) match {
                case SemanticAnalyzerSuccess (tab: Table) => tableList += tab
                case SemanticAnalyzerFailure (err) => errors ++= err
                case _ => {}
              }
          }
          if (errors.size > 0) {
            SemanticAnalyzerFailure (errors)
          } else {
            val map = HashMap [String, Long]()
            for (table <- tableList) {
              table match {
                case Table (name, id) => {
                  map += (name -> id)
                }
                case _ => {}
              }
            }
            SemanticAnalyzerSuccess (map)
          }
        }
      }
    }
  }

  trait ASTNode
  case class ASTId (long: TokenPosition) extends ASTNode
  case class ASTTable (tableName: TokenPosition, idField: ASTId) extends ASTNode
  case class ASTTables (tables: Queue [ASTTable]) extends ASTNode

  def getASTId (it: Iterator [TokenPosition]): ASTId = {
    val idKeyword = it.next
    val colon = it.next
    val long = it.next
    ASTId (long)
  }

  def getASTTable (it: Iterator [TokenPosition]): ASTTable = {
    val tableKeyword = it.next
    val tableName = it.next
    val lBrace = it.next
    val idField = getASTId (it)
    val rBrace = it.next
    ASTTable (tableName, idField)
  }

  def getASTTables (it: Iterator [TokenPosition]): ASTTables = {
    val result = Queue [ASTTable] ()
    while (it.hasNext) {
      result += getASTTable (it)
    }
    ASTTables (result)
  }

  def getAST (tokens: ArrayBuffer [TokenPosition]): ASTNode = {
    val it = tokens.iterator
    getASTTables (it)
  }
  
  def parse (input: String): CompilerResult = {
    
    reserved ++= Set("table","id")
    val semanticAnalyzer = new SemanticAnalyzer
    syntaxAnalyze (input) match {
      case SyntaxAnalyzerSuccess (tokenPositions) => {
        val ast = getAST (tokenPositions)
        semanticAnalyzer (ast) match {
          case SemanticAnalyzerSuccess (map: HashMap [String, Long]) => { CompilerSuccess(new Schema (map)) }
          case SemanticAnalyzerFailure (errors) => { 
            CompilerFailure (errors.toSeq)
          }
        }
      }
      case SyntaxAnalyzerFailure (errors) => {
        CompilerFailure (errors.toSeq)
      }
    }
  }
}
