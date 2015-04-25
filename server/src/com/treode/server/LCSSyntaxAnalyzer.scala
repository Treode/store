package com.treode.server
import com.treode.twitter.finagle.http.{BadRequestException}
import com.treode.async.misc.parseUnsignedLong
import scala.util.{Success, Failure}
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.Position
import scala.collection.Iterator
import scala.collection.mutable.{HashMap, MutableList}



class LCSSyntaxAnalyzer extends SchemaParserStdLexical {

  trait Message {
    def pos: Position
    def message: String
    override def toString = (message + "\n" + "at line " + pos.line + ", column " + pos.column + "\n" + pos.longString)
  }
  case class NotFoundMessage (pos: Position, message: String) extends Message
  case class FoundAlternateMessage (pos: Position, message: String) extends Message
  case class UnexpectedFoundMessage (pos: Position, message: String) extends Message

  trait SyntaxAnalyzerResult
  case class SyntaxAnalyzerSuccess (tokenPositions: ArrayBuffer [TokenPosition]) extends SyntaxAnalyzerResult
  case class SyntaxAnalyzerFailure (errors: ArrayBuffer [Message]) extends SyntaxAnalyzerResult
  
  case class TokenPosition (token: Token, pos: Position)


  //Only for expected tokens						  
  private def mapSequnceNumber (n : Int): String = {
    n match {
      case 1 => "table"
      case 2 => "Identifier"
      case 3 => "{"
      case 4 => "id"
      case 5 => ":"
      case 6 => "Long"
      case 7 => "}"
      case 8 => "eof"
      case _ => "Unexpected token"
    }}

  private def findIndices (tokenPositions: ArrayBuffer [TokenPosition]): ArrayBuffer [Int] = {
    val indices = ArrayBuffer [Int] ()
    for (tokenPos <- tokenPositions) {
      indices += 
        (tokenPos.token match {
          case NumericLit (s) => 6
          case Identifier (s) => 2
          case Keyword (s) => {
            s match {
              case "table" => 1
              case "id" => 4
            }
          }
          case SpecialChar (s) => {
            s match {
              case "{" => 3
              case ":" => 5
              case "}" => 7
              case "eof" => 8
            }
          }
          case ErrorToken (s) => 8
        })
    }
    indices
  }

  private def findLCS (actual: ArrayBuffer [Int], expected: ArrayBuffer [Int]): Queue [(Int, Int)] = {
    val lcs = Array.ofDim [Int] (actual.length + 1, expected.length + 1)

    for (i <- 0 to actual.length){
      for (j <- 0 to expected.length){
        if (i==0 || j==0)
          lcs (i) (j) = 0
        else if (actual (i - 1) == expected (j - 1))
          lcs (i) (j) = lcs (i - 1) (j - 1) + 1
        else
          lcs (i) (j) = math.max (lcs (i) (j - 1), lcs (i - 1) (j))

      }}
    
    
    val matches = Queue [(Int, Int)] ()
    var index1 = actual.length
    var index2 = expected.length

    while (index1 > 0 && index2 > 0){
      
      if (actual (index1 - 1) == expected (index2 - 1)) {
        (index1 - 1, index2 - 1) +=: matches
        index1 = index1 - 1
        index2 = index2 - 1 
      }
      else if( lcs (index1 - 1) (index2) > lcs (index1) (index2 - 1)){
        index1 = index1 - 1
      }
      else{
        index2 = index2 - 1
      }}
    
    matches
  }

  private def generateErrors (matches: Queue [(Int, Int)], expected: ArrayBuffer [Int], actual: ArrayBuffer [Int], tokenPositions: ArrayBuffer [TokenPosition]): ArrayBuffer [Message] = {
    
    var errors = ArrayBuffer [Message] ()
    var index1 = 0
    var index2 = 0
    
    while (index1 < actual.length || index2 < expected.length) {
      
      var endIndex1 = actual.length
      var endIndex2 = expected.length
      
      if (matches.length > 0){
        var matchIndex = matches.dequeue	
        endIndex1 = matchIndex._1
        endIndex2 = matchIndex._2
      }
      
      while (index2 < endIndex2 && index1 < endIndex1){
        val expToken = mapSequnceNumber (expected (index2))
        val gotToken = mapSequnceNumber (actual (index1))

        val tok = tokenPositions (index1).token
        val pos = tokenPositions (index1).pos
        val errorMsg = "Expected \"" + expToken + "\", but found " + tok
        FoundAlternateMessage (pos, errorMsg) +=: errors
        index2 = index2 + 1
        index1 = index1 + 1
      }
      while (index2 < endIndex2) {
        val expToken = mapSequnceNumber (expected (index2))
        val errorMsg = "Expected " + expToken + " but not found" 
        val pos = tokenPositions (index1).pos
        NotFoundMessage (pos, errorMsg) +=: errors
        index2 = index2 + 1
      }
      while (index1 < endIndex1) {
        val gotToken = mapSequnceNumber (actual (index1))
        val tok = tokenPositions (index1).token
        val pos = tokenPositions (index1).pos
        val errorMsg = "Unexpected \"" + tok + "\" found"
        UnexpectedFoundMessage (pos, errorMsg) +=: errors
        index1 = index1 + 1
      }
      index1 = index1 + 1
      index2 = index2 + 1
    }
    for (v <- errors.reverse) {
      println (v)
    }
    errors.reverse
  }

  private def findTokenInfo (scanner: Scanner): ArrayBuffer [TokenPosition] = {
    if (scanner.atEnd) {
      ArrayBuffer [TokenPosition] (TokenPosition (SpecialChar ("eof"), scanner.pos))
    } else {
      val tokenPosition = findTokenInfo (scanner.rest)
      TokenPosition (scanner.first ,scanner.pos) +: tokenPosition
    }
  }

  def syntaxAnalyze (input: String): SyntaxAnalyzerResult = {
    val scanner = new Scanner (input)
    val actTokenPositions = findTokenInfo (scanner)
    val actTokenIndices = findIndices (actTokenPositions)
    val expected = ArrayBuffer (1,2,3,4,5,6,7)
    val expTokenIndices = ArrayBuffer [Int] ()
    val expNumTables = (actTokenIndices.size + expected.size/2) / expected.size
    for (i <- 1 to expNumTables) {
      expTokenIndices ++= expected
    }
    expTokenIndices += 8
    val bestResult = findLCS (actTokenIndices, expTokenIndices)
    val errors = generateErrors (bestResult, expTokenIndices, actTokenIndices, actTokenPositions)
    if (errors.size > 0) {
      SyntaxAnalyzerFailure (errors)
    } else {
      actTokenPositions.remove (actTokenPositions.size-1)
      SyntaxAnalyzerSuccess (actTokenPositions)
    }
  }
}
