/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2009-2010, Jesse Eichar             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scalaio.test

import scalax.io._
import org.junit.Assert._
import org.junit.{
Test, Ignore
}

import Constants.TEXT_VALUE

abstract class AbstractWriteCharsTests extends scalax.test.sugar.AssertionSugar {
  private final val DEFAULT_DATA = "default data"

  def open(): (ReadChars, WriteChars)

  @Test(timeout = 3000)
  def write_string(): Unit = {
    val (input, output) = open()
    output write DEFAULT_DATA

    assertEquals(DEFAULT_DATA, input.slurpString)
  }


  @Test(timeout = 3000)
  def write_many_strings(): Unit = {
    val (input, output) = open()

    output writeStrings (DEFAULT_DATA :: DEFAULT_DATA :: DEFAULT_DATA :: Nil)
    assertEquals(DEFAULT_DATA + DEFAULT_DATA + DEFAULT_DATA, input.slurpString)

    val (input2, output2) = open()

    output2 writeStrings (DEFAULT_DATA :: DEFAULT_DATA :: DEFAULT_DATA :: Nil, "-")
    assertEquals(DEFAULT_DATA + "-" + DEFAULT_DATA + "-" + DEFAULT_DATA, input2.slurpString)
  }


  @Test //@Ignore
  def openOutput: Unit = {
    val (in,out0) = open()
    out0 match {
      case out0:WriteCharsResource[_] =>
        var closes = 0;
        val out = out0.appendCloseAction(_ => closes += 1)

        assertEquals(0,closes)
        out.write("whoop!")
        assertEquals(1,closes)

        out.open(opened => {
          opened.write("hello")
          opened.write(" ")
          opened.write("world")
        })
        assertEquals(2,closes)
        assertEquals("whoop!hello world",in.slurpString)
      case _ => ()
    }
  }

}
