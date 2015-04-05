package com.treode.server

import org.scalatest._
import com.treode.server.SchemaParser._
import com.treode.twitter.finagle.http.{BadRequestException}

class ServerParserSpec extends FreeSpec with Matchers {
  
  "When inputs are proper" - {
    "The parser should return the string tableV" in {
  	  parse (identifier, "tableV~").get should be ("tableV")
    }
    "The parser should return the string table-V_i" in {
  	  parse (identifier, "table-V_i").get should be ("table-V_i")
    }
    "The parser should return the string table_V-671994" in {
  	  parse (identifier, "table_V-671994").get should be ("table_V-671994")
    }
    "The parser should return the long 108 from decimal" in {
  	  parse (number, "108").get should be (108)
    }
    "The parser should return the long 1008 from hexadecimal" in {
  	  parse (number, "0x3F0!").get should be (1008)
    }
    "The parser should return the long 10008 from octal" in {
  	  parse (number, "023430").get should be (10008)
    }
    "The parser should return the proper table object" in {
      parse (tableDefinition, "table tableV { id : 108 }").get should be (Table("tableV",108)) 	
    }
  }

  "When inputs are not proper" - {
    "The parser should throw BadRequestException if table name is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table 4tableV { id : 1 }")
  	  }}
    "The parser should throw BadRequestException if table id is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table tableV { id : 1 } table tableU { id : op }")	    
  	  }} 
  	"The parser should throw BadRequestException if syntax is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("tableU { id : 1 }")
  	  }}
  	"The parser should throw BadRequestException if colon is missing" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table table2 { id 1 }")
  	  }}  
  }}
    
