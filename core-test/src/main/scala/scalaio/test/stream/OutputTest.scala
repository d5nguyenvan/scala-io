/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2009-2010, Jesse Eichar             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scalaio.test.stream

import scalaio.test._

import java.io.{
ByteArrayInputStream, ByteArrayOutputStream
}
import org.junit.Test
import org.junit.Assert._
import scalax.io._

class OutputTest extends AbstractOutputTests {
  def open() = {
    val cache = new Array[Byte](1000)
    val out = new ByteArrayOutputStream()
    def in = new ByteArrayInputStream(out.toByteArray)

    val inResource = Resource.fromInputStream(in)
    val outResource = Resource.fromOutputStream(out)

    (inResource, outResource)
  }

}
