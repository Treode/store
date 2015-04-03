package com.treode.server

import com.treode.twitter.finagle.http.{BadRequestException}
import scala.util.parsing.combinator._
import scala.collection.mutable.HashMap

object SchemaParser extends RegexParsers {

  class Fields () {
  	var idField: Option [Long] = None
    def id: Option [Long] = idField
    def id_= (idVal: Long): Unit = { idField = Some(idVal) }
  }
  case class Table (name: String, id: Long)
  
  def keywordTable: Parser [String] = """table""".r ^^ { _.toString }
  def keywordId: Parser [String] = """id""".r ^^ { _.toString }                          
  def colon: Parser [String] = """[:]""".r ^^ { _.toString }

  def identifier: Parser [String] = """[a-zA-Z][a-zA-Z0-9]*""".r ^^ { _.toString }                       
  def number: Parser [Long] = """((0[xX][0-9])|[0-9])[0-9]*""".r ^^ { _.toId }
  def openBrace: Parser [String] = """[{]""".r ^^ { _.toString }
  def closeBrace: Parser [String] = """[}]""".r ^^ { _.toString }

  def idField (tableFields: Fields) = keywordId ~ colon ~ number ^^ { 
  	case id ~ co ~ num => tableFields.id = num 
    case _ => throw new BadRequestException ("Bad table ID")
  }
  def field (tableFields: Fields) = idField(tableFields) 
  def fields (tableFields: Fields) = field(tableFields)*

  def tableDefinition: Parser [Table] = {
    var tableFields = new Fields ()
  	identifier ~ openBrace ~ fields (tableFields) ~ closeBrace ^^ { 
  	  case id ~ ob ~ fi ~ cb => Table(id, tableFields.id match { 
  		case Some(v) => v; 
  	  	case _ => throw new BadRequestException ("Bad table ID")
  	  })
  	  case _ => throw new BadRequestException ("Bad table ID")
    }
  }

  def tableDefinitions = tableDefinition* 

  def getSchema (schema: String): Schema = {
  	
  	val tableList = parse (tableDefinitions, schema).get
  	val map = new HashMap [String, Long]()

  	for (table <- tableList) {
  		table match {
  			case Table (name, id) =>  map += (name -> id)

  		}
  	}
  	new Schema(map)

  }}


