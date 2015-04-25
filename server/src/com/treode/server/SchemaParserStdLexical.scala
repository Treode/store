package com.treode.server
import com.treode.twitter.finagle.http.{BadRequestException}
import com.treode.async.misc.parseUnsignedLong
import scala.util.{ Success, Failure }
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.Position
import scala.collection.Iterator
import scala.collection.mutable.{HashMap, MutableList}



class SchemaParserStdLexical extends StdLexical {
  def hexaDigit = elem("hexadecimal digit", _.isHexaDigit)
  def octaDigit = elem("octadecimal digit", _.isOctaDigit)
  def posiDigit = elem("positive digit", _.isPosiDigit)
  def deciDigit = elem("decimal digit", _.isDeciDigit)

  case class SpecialChar (chars: String) extends Token {
    override def toString = "\'" + chars + "\'"
  }

  override def token: Parser[Token] = 
    ( '0' ~ 'x' ~ hexaDigit ~ rep (hexaDigit) ^^ { case '0' ~ 'x' ~ first ~ rest => NumericLit("0x" + (first :: rest mkString "")) }
    | ('0' ~ rep( octaDigit )) ^^ { case '0' ~ rest => NumericLit("0" + (rest mkString "")) }
    | identChar ~ rep( identChar | digit | '-') ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
    | posiDigit ~ rep (deciDigit) ^^ { case first ~ rest => NumericLit(first :: rest mkString "") }
    | '{' ^^^ SpecialChar ("{")
    | '}' ^^^ SpecialChar ("}")
    | ':' ^^^ SpecialChar (":")
    | EofCh ^^^ EOF
    | delim
    | failure ("Illegal character")
    )
  implicit class RichChar (ch: Char) {
    def isHexaDigit = 
      ((ch >= '0' && ch <= '9') ||
       (ch >= 'a' && ch <= 'f') ||
       (ch >= 'A' && ch <= 'F'))
    def isOctaDigit = 
      (ch >= '0' && ch <= '7')
    def isDeciDigit = 
      (ch >= '0' && ch <= '9')
    def isPosiDigit = 
      (ch >= '1' && ch <= '9')
  }
}
