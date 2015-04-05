package com.treode.server

import com.treode.twitter.finagle.http.{BadRequestException}
import com.treode.async.misc.parseUnsignedLong
import scala.util.parsing.combinator._
import scala.collection.mutable.HashMap

object SchemaParser extends RegexParsers {

  case class Table (name: String, id: Long)

  private def toId (s: String): Long = {
    parseUnsignedLong (s) match {
      case Some (v) => v
      case None => throw new BadRequestException (s"Bad table ID: $s")
    }}
  
  def identifier: Parser [String] = """[a-zA-Z][a-zA-Z0-9_-]*""".r ^^ { _.toString }                       
  def number: Parser [Long] = """((0[xX][0-9a-fA-F]+)|([0-9]+))""".r ^^ { toId(_) }
  
  def idField = identifier ~ (":" ~> number) ^^ { 
    case "id" ~ num => num 
    }
  
  def tableDefinition: Parser [Table] = {
    (identifier ~ identifier <~ "{") ~ (idField <~ "}") ^^ { 
  	  case "table" ~ id ~ fi => Table(id, fi)
    }}

  def tableDefinitions = tableDefinition*

  def getSchema (schema: String): Schema = {
  	
    val tableList = parseAll (tableDefinitions, schema) match {
      case Success (matched, _) => matched
      case Failure (msg, _) => throw new BadRequestException (msg)
      case Error (msg, _) => throw new BadRequestException (msg)
    }
    val map = new HashMap [String, Long]()

    for (table <- tableList) {
      table match {
  	    case Table (name, id) =>  map += (name -> id)
  	  }}
  	
  	new Schema(map)

  }}


