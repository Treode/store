package com.treode.store

import java.io.IOException
import java.nio.file.{Files, FileVisitor, FileVisitResult, Path}
import java.nio.file.attribute.{BasicFileAttributes, FileAttribute}

object TestFiles {

  def createDirectories (path: Path, attrs: FileAttribute [_]*): Path =
    Files.createDirectories (path, attrs: _*)

  def createTempDirectory (path: String, attrs: FileAttribute [_]*): Path =
    Files .createTempDirectory (path, attrs: _*)

  def deleteDirectory (path: Path) {
    Files.walkFileTree (path, new FileVisitor [Path] () {

      def postVisitDirectory (p: Path, e: IOException) = {
        Files.delete (p)
        FileVisitResult.CONTINUE
      }

      def preVisitDirectory (p: Path, as: BasicFileAttributes) = FileVisitResult.CONTINUE

      def visitFile (p: Path, as: BasicFileAttributes) = {
        Files.delete (p)
        FileVisitResult.CONTINUE
      }

      def visitFileFailed (p: Path, e: IOException) = FileVisitResult.TERMINATE
    })
  }}
