/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2009-2011, Jesse Eichar          **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */
package scalax.io

/**
 * Not public API.  I don't think I like the idea of having constants
 * defined like this.  At the very least there needs to be a way to
 * override the default values.
 */
object Constants {
  final val BufferSize = 1024 * 1024
}
