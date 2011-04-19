/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2009-2010, Jesse Eichar             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scalax.io

import scala.collection._
import scala.collection.generic._
import TraversableView.NoBuilder
import java.util.NoSuchElementException

/**
 * The view object associated with LongTraversable.  If you are nor familiar with the pattern essentially a view allows
 * the data to be loaded lazily and only as necessary.  So you can perform slices and maps without loading any data.
 * Only when the data is pulled will it actually be loaded.
 *
 * The behaviour is the essentially the same as for
 * [[http://www.scala-lang.org/api/current/scala/collection/TraversableView.html]] except adding the LongTraversable
 * methods
 */
trait LongTraversableView[+A, +Coll] extends LongTraversableViewLike[A, Coll, LongTraversableView[A, Coll]] {
  override def toString() = "LongTraversableView(...)"
}

/**
 * Defines the required canBuildFrom and Type definitions.  These are required by the collections framework.
 *
 * Probably not interesting API
 */
object LongTraversableView {
  type Coll = TraversableView[_, C] forSome {type C <: Traversable[_]}
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, LongTraversableView[A, LongTraversable[_]]] =
    new CanBuildFrom[Coll, A, LongTraversableView[A, LongTraversable[_]]] {
      def apply(from: Coll) = new NoBuilder
      def apply() = new NoBuilder
    }
}

/**
 * The actual [[scalax.io.LongTraversableView]] implementations.  Not interesting API beyond what is exposed in
 * [[scalax.io.LongTraversableView]] and [[scalax.io.LongTraversable]]
 */
trait LongTraversableViewLike[+A, +Coll, +This <: LongTraversableView[A,Coll] with LongTraversableViewLike[A,Coll, This]]
      extends LongTraversable[A] with LongTraversableLike[A, This] with TraversableView[A,Coll] with TraversableViewLike[A,Coll,This]{
  self =>

  trait Transformed[+B] extends LongTraversableView[B, Coll] with super.Transformed[B] {
    protected[io] def iterator:CloseableIterator[B]
    override def foreach[U](f: (B) => U) = {
      val iter = iterator
      try {
        iter.foreach(f)
      } finally {
        iter.close()
      }
    }
  }


  trait Forced[B] extends super.Forced[B] with Transformed[B] {
    def iterator = CloseableIterator(forced.iterator)
  }

  trait LSliced extends Transformed[A] {
    protected[this] def from: Long
    protected[this] def until: Long

    protected[io] def iterator = self.iterator.lslice(from,until)
  }

  trait Mapped[B] extends super.Mapped[B] with Transformed[B] {
    protected[io] def iterator = self.iterator.map(mapping)
  }
  trait FlatMapped[B] extends super.FlatMapped[B] with Transformed[B] {
    protected[io] def iterator = self.iterator.flatMap(mapping.andThen(_.toIterator))
  }
  trait Appended[B >: A] extends super.Appended[B] with Transformed[B] {
    protected[io] def iterator = self.iterator ++ rest.toIterator
  }
  trait Filtered extends super.Filtered with Transformed[A] {
    protected[io] def iterator = self.iterator filter pred
  }
  trait TakenWhile extends super.TakenWhile with Transformed[A] {
    protected[io] def iterator: CloseableIterator[A] = self.iterator takeWhile pred
  }
  trait DroppedWhile extends super.DroppedWhile with Transformed[A] {
    protected[io] def iterator: CloseableIterator[A] = self.iterator dropWhile pred
  }

  trait Zipped[B] extends Transformed[(A, B)] {
    protected[this] val other: Iterable[B]
    def iterator: CloseableIterator[(A, B)] = self.iterator zip other.iterator
  }

  trait ZippedAll[A1 >: A, B] extends Transformed[(A1, B)] {
    protected[this] val other: Iterable[B]
    protected[this] val thisElem: A1
    protected[this] val thatElem: B
    def iterator: CloseableIterator[(A1, B)] =
      self.iterator.zipAll(other.iterator, thisElem, thatElem)
  }

  override def zip[A1 >: A, B, That](that: Iterable[B])(implicit bf: CanBuildFrom[This, (A1, B), That]): That = {
    newZipped(that).asInstanceOf[That]
// was:    val b = bf(repr)
//    if (b.isInstanceOf[NoBuilder[_]]) newZipped(that).asInstanceOf[That]
//    else super.zip[A1, B, That](that)(bf)
  }

  override def zipWithIndex[A1 >: A, That](implicit bf: CanBuildFrom[This, (A1, Int), That]): That =
    zip[A1, Int, That](Stream from 0)(bf)

  override def zipAll[B, A1 >: A, That](that: Iterable[B], thisElem: A1, thatElem: B)(implicit bf: CanBuildFrom[This, (A1, B), That]): That =
    newZippedAll(that, thisElem, thatElem).asInstanceOf[That]

  /** Boilerplate method, to override in each subclass
   *  This method could be eliminated if Scala had virtual classes
   */
  protected def newZipped[B](that: Iterable[B]): Transformed[(A, B)] = new { val other = that } with Zipped[B]
  protected def newZippedAll[A1 >: A, B](that: Iterable[B], _thisElem: A1, _thatElem: B): Transformed[(A1, B)] = new {
    val other: Iterable[B] = that
    val thisElem = _thisElem
    val thatElem = _thatElem
  } with ZippedAll[A1, B]

  /** Boilerplate method, to override in each subclass
   *  This method could be eliminated if Scala had virtual classes
   */
  protected override def newForced[B](xs: => Seq[B]): Transformed[B] = new Forced[B] { val forced = xs }
  protected override def newAppended[B >: A](that: Traversable[B]): Transformed[B] = new Appended[B] { val rest = that }
  protected override def newMapped[B](f: A => B): Transformed[B] = new Mapped[B] { val mapping = f }
  protected override def newFlatMapped[B](f : A => Traversable[B]): Transformed[B] = new FlatMapped[B] { val mapping = f }
  protected override def newFiltered(p: A => Boolean): Transformed[A] = new Filtered { val pred = p }
  protected override def newSliced(_from: Int, _until: Int): Transformed[A] = newLSliced(_from.toLong, _until.toLong)
  protected def newLSliced(_from: Long, _until: Long): Transformed[A] = new LSliced { val from = _from; val until = _until }
  protected override def newDroppedWhile(p: A => Boolean): Transformed[A] = new DroppedWhile { val pred = p }
  protected override def newTakenWhile(p: A => Boolean): Transformed[A] = new TakenWhile { val pred = p }

  override def drop(n: Int): This = newLSliced(n max 0, Long.MaxValue).asInstanceOf[This]
  override def ldrop(n: Long): This = newLSliced(n max 0, Long.MaxValue).asInstanceOf[This]

  override def take(n: Int): This = newLSliced(0, n).asInstanceOf[This]
  override def ltake(n: Long): This = newLSliced(0, n).asInstanceOf[This]

  override def stringPrefix = "LongTraversableView"
}
