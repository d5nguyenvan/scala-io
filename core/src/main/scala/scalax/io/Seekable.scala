/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2009-2010, Jesse Eichar          **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scalax.io

import java.nio.{ByteBuffer, CharBuffer}
import nio.SeekableFileChannel
import scalax.io._
import Constants.BufferSize
import scala.collection.Traversable
import scala.annotation._
import collection.mutable.{ArrayOps, WrappedArray}
import StandardOpenOption._
import java.nio.channels.{FileChannel, ByteChannel, WritableByteChannel}
import java.io.{OutputStream, RandomAccessFile, File}
import scalax.io.ResourceAdapting.ChannelOutputStreamAdapter

/**
 * A strategy trait used with the Seekable.patch to control how data is
 * written to the object.
 *
 * Note: Methods are not part of API.
 */
sealed trait Overwrite {
  private[io] def getOrElse(opt: => Long):Long
  private[io] def map(f:Long => Long):Overwrite
  private[io] def foreach[U](f:Long => U):Unit
  private[io] def exists(p:Long => Boolean):Boolean
}

/**
 * Strategy to overwrite as much data as possible.
 *
 * IE if the data to write is 4 bytes try to overwrite 4 bytes.
 */
case object OverwriteAll extends Overwrite {
  private[io] def getOrElse(opt: => Long) = opt
  private[io] def map(f:Long => Long) = this
  private[io] def foreach[U](f:Long => U) = ()
  private[io] def exists(p:Long => Boolean) = false
}

/**
 * Strategy for (potentially) only overwriting a subset of the data.
 *
 * @param replacementLength maximum number of ''units'' to overwrite.  If ''Longs'' are written
 * the the ''unit'' are ''Longs''.
 */
case class OverwriteSome(replacementLength:Long) extends Overwrite {
  private[io] def getOrElse(opt: => Long) = replacementLength
  private[io] def map(f:Long => Long) = new OverwriteSome(f(replacementLength))
  private[io] def foreach[U](f:Long => U) : Unit = f(replacementLength)
  private[io] def exists(p:Long => Boolean) = p(replacementLength)
}

/**
 * An object for reading and writing to Random Access IO objects such as Files.
 *
 * In addition to the methods contributed by [[scalax.io.Input]] and [[scalax.io.Output]] patch, insert
 * and append are provided as more random access style operations.
 *
 * Note: The methods in [[scalax.io.Output]] are always fully destructive.  IE write
 * will replace all data in the file, insert, patch or append are your friends if that is
 * not the behaviour you want.
 *
 *
 * @author Jesse Eichar
 * @since 1.0
 *
 * @define overwriteParam @param overwrite The strategy that dictates how many characters/bytes/units are overwritten
 * @define outputConverter [[scalax.io.OutputConverter]]
 * @define dataParam  @param data
 *          The data to write.  This can be any type that has a $outputConverter associated
 *          with it.  There are predefined $outputConverters for several types.  See the
 *          $outputConverter object for the predefined types and for objects to simplify implementing
 *          custom $outputConverter
 * @define intAsByteExplanation Since the [[scalax.io.OutputConverter]] object defined for writing Ints encodes Ints using 4 bytes this method
 *  is provided to simply write an array of Ints as if they are Bytes.  In other words just taking the first
 *  byte.  This is pretty common in Java.io style IO.  IE
 *  {{{ outputStream.write(1) }}}
 *  1 is written as a single byte.
 * @define arrayRecommendation '''Important:''' The use of an Array is highly recommended
 *  because normally arrays can be more efficiently written using
 *  the underlying APIs
 * @define patchDesc Update a portion of the file content at
 *  the declared location. This is the most flexible of the
 *  random access methods but is also (probably) the trickiest
 *  to fully understand.  That said it behaves (almost) identical
 *  to a scala.collection.Seq.patch method, so if you understand that
 *  you should not have difficulty understanding this method.
 * @define converterParamconverterParam @param converter The strategy for writing the data/converting the data to bytes
 */
trait Seekable extends Input with Output {
  self =>

  private final val MaxPermittedInMemory = 50 * BufferSize

  private final val OVERWRITE_CODE = Long.MinValue


  protected def underlyingChannel(append:Boolean):SeekableByteChannel
  // for Java 7 change this to a Seekable Channel
  protected def readWriteChannel[U](f:SeekableByteChannel => U) : U =
    Resource.fromSeekableByteChannel(underlyingChannel(false)).acquireAndGet(f)

  protected def appendChannel[U](f:SeekableByteChannel => U) : U = {
    Resource.fromSeekableByteChannel(underlyingChannel(true)).acquireAndGet(f)
  }

  /**
   * Execute the function 'f' passing an Output instance that performs all operations
   * on a single opened connection to the underlying resource. Typically each call to
   * one of the Output's methods results in a new connection.  For example if the underlying
   * OutputStream truncates the file each time the connection is made then calling write
   * two times will result in the contents of the second write overwriting the second write.
   *
   * Even if the underlying resource is an appending, using open will be more efficient since
   * the connection only needs to be made a single time.
   *
   * @param f the function to execute on the new Output instance (which uses a single connection)
   * @return the result of the function
   */
  def open[U](f: Seekable => U):U = {
    readWriteChannel{ out =>
      val nonSeekable:Seekable = new SeekableByteChannelResource[SeekableByteChannel](null,CloseAction.Noop,() => Some(out.size),KnownName("Seekable opened resource"),None) {
        override def open():OpenedResource[SeekableByteChannel] = new OpenedResource[SeekableByteChannel]{
          def close(): List[Throwable] = Nil
          def get = out
        }

        override def toString: String = "Opened "+self.toString
      }
      f(nonSeekable)
    }
  }

  /**
   * $patchDesc
   *
   * If the position is beyond the end of the file a BufferUnderflow
   * Exception will be thrown
   *
   * If the position is within the file but the
   * `position + string.getBytes(codec).length`
   * is beyond the end of the file the file will be enlarged so
   * that the entire string can fit in the file
   *
   * The write begins at the position indicated.  So if position = 0
   * then the write will begin at the first byte of the file.
   *
   * @param position
   *          The start position of the update starting at 0.
   *          The position is the position'th character in the file using
   *          the codec for decoding the file
   *          The position must be within the file.
   * @param string
   *          The string to write to the file starting at
   *          position.
   * $overwriteParam
   * @param codec
   *          The codec to use for decoding the underlying data into characters
   */
  def patch(position: Long,
            string: String,
            overwrite : Overwrite)(implicit codec: Codec = Codec.default): Unit = {
    require(position >= 0, "The patch starting position must be greater than or equal 0")

    if (size.forall{position >= _} || codec.hasConstantSize) {
      patch(position, string:Traversable[Char], overwrite)(OutputConverter.charsToOutputFunction(codec))
    } else {
      // when a charset is not constant (like UTF-8 or UTF-16) the only
      // way is to find position is to iterate to the position counting characters
      // Same with figuring out what replaced is in bytes


      val replaced = overwrite match {
        case OverwriteAll => OVERWRITE_CODE
        case OverwriteSome(r) => r
      }

      val bytes = string.getBytes(codec.name)

      // this is very inefficient.  The file is opened 3 times.
      val posInBytes = charCountToByteCount(0, position)
      if(overwrite.exists{_ < 0}) {
        insert(posInBytes, bytes)
      } else {
        val replacedInBytes = charCountToByteCount(position, position+(if(overwrite == OverwriteAll) string.size else replaced))

        patch(posInBytes, bytes, OverwriteSome(replaced max replacedInBytes))
      }
    }
  }

  /**
   * $patchDesc
   *
   * $arrayRecommendation
   *
   * To append data the `position must >= size`
   *
   * If the position is within the file but the
   * `position + bytes.length`
   * is beyond the end of the file the file will be enlarged so
   * that the entire string can fit in the file
   *
   * The write begins at the position indicated.  So if position = 0
   * then the write will begin at the first byte of the file.
   *
   * @param position
   *          The start position of the update starting at 0.
   *          The position must be within the file or == size (for appending)
   * $dataParam
   * $overwriteParam
   * $converterParam
   */
  def patch[T](position: Long,
            data: T,
            overwrite : Overwrite)(implicit converter:OutputConverter[T]): Unit = {
    val bytes = converter.toBytes(data)
    val actualPosition = converter.sizeInBytes * position
    require(actualPosition >= 0, "The patch starting position must be greater than or equal 0")

    // replaced is the old param.  I am migrating to the Overwrite options
    val replaced = overwrite.getOrElse(OVERWRITE_CODE)

    val appendData = size.forall{actualPosition >= _}
    val insertData = replaced <= 0 && replaced != OVERWRITE_CODE

    if(appendData) {
      append(bytes)(OutputConverter.TraversableByteConverter)
    } else if(insertData) {
      insert(actualPosition, bytes)(OutputConverter.TraversableByteConverter)
    } else {
      // overwrite data
      overwriteFileData(actualPosition, bytes, replaced)
    }
  }

 /**
   * $intAsByteExplanation
   */
  def patchIntsAsBytes(position:Long,
                       overwrite : Overwrite,
                       data:Int*) = patch(position,data,overwrite)(OutputConverter.TraversableIntAsByteConverter)

  /**
   * Inserts a string at a position in the Seekable. This is a potentially inefficient because of the need to
   * count characters.  If the codec is not a fixed sized codec (for example UTF8) each character must be
   * converted in the file up to the point of insertion.
   *
   * @param position The position in the file to perform the insert.  A position of 2 will insert the character after
   *              the second character (not byte).
   * @param string The string that will be inserted into the Seekable
   * @param codec The codec to use for determining the location for inserting the string and for encoding the
   *              string as bytes
   */
  def insert(position : Long, string: String)(implicit codec: Codec = Codec.default): Unit = {
    insert(position, codec encode string)
  }

   /**
    * Inserts data at a position in the Seekable.  The actual position in the Seekable where the data is inserted depends on
    * the type of data being written.  For example if Longs are being written then position calculated as position * 8
    *
    * $arrayRecommendation
    *
    * @param position  The position where the data is inserted into the Seekable.  The actual position in the Seekable
    * where the data is inserted depends on the type of data being written.  For example if
    * Longs are being written then position calculated as position * 8
    *
    * $dataParam
    *
    * $converterParam
   */
  def insert[T](position : Long, data : T)(implicit converter:OutputConverter[T]) = {
    val bytes = converter.toBytes(data)
    if(size.forall(_ <= position)) {
      append(bytes)
    } else if (bytes.hasDefiniteSize && bytes.size <= MaxPermittedInMemory) {
        insertDataInMemory(position max 0, bytes)
    } else {
        insertDataTmpFile(position max 0, bytes)
    }
  }
 /**
   * $intAsByteExplanation
   */
  def insertIntsAsBytes(position : Long, data : Int*) = insert(position, data)(OutputConverter.TraversableIntAsByteConverter)

  private def insertDataInMemory(position : Long, bytes : TraversableOnce[Byte]) = {
    readWriteChannel { channel =>
      channel position position
      var buffers = (ByteBuffer allocateDirect MaxPermittedInMemory, ByteBuffer allocateDirect MaxPermittedInMemory)

      channel read buffers._1
      buffers._1.flip
      buffers = buffers.swap

      channel position position
      writeTo(channel, bytes, bytes.size)

      while(channel.position < channel.size - buffers._2.remaining) {
          channel read buffers._1
          buffers._1.flip

          channel.write(buffers._2, channel.position - bytes.size)

          buffers = buffers.swap
      }

      channel.write(buffers._2)
      }
  }

  /**
   * Create a temporary file to use for performing certain operations.  It should
   * be as efficient as possible to copy from the temporary file to this Seekable and
   * vice-versa.  Can be overridden for performance
   */
  protected def tempFile() : Input with Output = {
    val file = File.createTempFile("SeekableCache","tmp")
    Resource.fromFile(file)
  }

  private def insertDataTmpFile(position : Long, bytes : TraversableOnce[Byte]) = {
      val tmp = tempFile()

      tmp write (this.bytes ldrop position)

      readWriteChannel {channel =>
           channel position  position
           writeTo(channel, bytes, -1)
           writeTo(channel, tmp.bytes, tmp.size.get)
      }
  }

  private def overwriteFileData(position : Long, bytes : TraversableOnce[Byte], replaced : Long) = {
      readWriteChannel {channel =>
        channel.position(position)
//            println("byte size,replaced",bytes.size,replaced)
          val (wrote, earlyTermination) = writeTo(channel,bytes, replaced)

//          println("wrote,earlyTermination",wrote,earlyTermination)

          if(replaced > channel.size // need this in the case where replaced == Long.MaxValue
             || position + replaced > channel.size) {
              channel.truncate(channel.position)
          } else if (replaced > wrote) {
              val length = channel.size - position - replaced
              val srcIndex = position + replaced
              val destIndex = position + wrote
              copySlice(channel, srcIndex, destIndex, length.toInt)
              channel.truncate(destIndex+length)
          } else if (earlyTermination) {
            val adjustedPosition = position +  replaced
            bytes match {
              case b : LongTraversable[_] => insert(adjustedPosition,b.asInstanceOf[LongTraversable[Byte]] ldrop wrote)
              case b : Traversable[_] => insert(adjustedPosition,b.asInstanceOf[Traversable[Byte]] drop wrote.toInt)
              case i:Iterator[_] => insert(adjustedPosition, bytes)
              case _ => insert(adjustedPosition,TraversableOnceOps.drop(bytes, wrote.toInt))
            }
          }
      }
  }

  private def copySlice(channel : SeekableByteChannel, srcIndex : Long, destIndex : Long, length : Int) : Unit = {

//      println("copySlice(srcIndex, destIndex, length)", srcIndex, destIndex, length)

      val buf = ByteBuffer.allocate(BufferSize.min(length))
      def write(done : Int) = {
//        println("copySlice:write(done)", done)
        if(length < done + BufferSize) buf.limit((length - done).toInt)

        buf.clear()
        val read = channel.read(buf, srcIndex + done)
        buf.flip()
        val written = channel.write(buf, destIndex + done)
      }

      (0 to length by BufferSize) foreach write
  }


  /**
   * Append bytes to the end of a file
   *
   * $arrayRecommendation
   *
   * $dataParam
   *
   * $converterParam
   */
  def append[T](data: T)(implicit converter:OutputConverter[T]): Unit = {
    val bytes = converter.toBytes(data)
    appendChannel{channel =>
      writeTo(channel, bytes, -1)
    }
  }

  /**
   * $intAsByteExplanation
   */
  def appendIntsAsBytes(data:Int*) = {
    append(data)(OutputConverter.TraversableIntAsByteConverter)
  }

  // returns (wrote,earlyTermination)
  private def writeTo(c : WritableByteChannel, bytes : TraversableOnce[Byte], length : Long) : (Long,Boolean) = {
      def writeArray(array:Array[Byte]) = {
        // for performance try to write Arrays directly
        val count = if(length < 0) bytes.size else length.min(bytes.size)
        val wrote = c.write(ByteBuffer.wrap(array, 0, count.toInt))

        val isWriteAll = length > 0
        (wrote.toLong, isWriteAll && length < bytes.size)
      }

      bytes match {
        case wrappedArray : WrappedArray[Byte] =>
          writeArray(wrappedArray.array)
        case ops:ArrayOps[Byte] =>
          writeArray(ops.toArray)
        case _ =>
          // TODO user hasDefinitateSize to improve performance
          // if the size is small enough we can probably open a memory mapped buffer
          // or at least copy to a buffer in one go.
            val buf = ByteBuffer.allocateDirect(if(length > 0) length min BufferSize toInt else BufferSize)
            var earlyTermination = false

            @tailrec
            def write(written : Long, data:TraversableOnce[Byte], acc:Long) : Long = {
                val numBytes = length match {
                    case -1 | OVERWRITE_CODE => BufferSize
                    case _ => (length - written).min(BufferSize).toInt
                }

                val (toWrite, remaining) = TraversableOnceOps.splitAt(data, numBytes)

              if(!(numBytes > 0 || remaining.nonEmpty)) {
                assert(numBytes > 0 || remaining.nonEmpty)
            }

                toWrite foreach buf.put
                buf.flip
                val currentWrite : Long = c write buf
                earlyTermination = length <= written + numBytes && remaining.nonEmpty

                if (earlyTermination || remaining.isEmpty) currentWrite + acc
                else write (written + numBytes, remaining, currentWrite + acc )
            }

           (write(0, bytes, 0), earlyTermination)
      }
  }

  /**
  * Append a string to the end of the Seekable object.
  *
  * @param string
  *          the data to write
  * @param codec
  *          the codec of the string to be written. The string will
  *          be converted to the encoding of {@link codec}
  */
  def append(string: String)(implicit codec: Codec = Codec.default): Unit = {
      append(codec encode string)
  }

  /**
  * Append several strings to the end of the Seekable object.
  *
  * @param strings
  *          The strings to write
  * @param separator
  *          A string to add between each string.
  *          It is not added to the before the first string
  *          or after the last.
  * @param codec
  *          The codec of the strings to be written. The strings will
  *          be converted to the encoding of {@link codec}
  */
  def appendStrings(strings: Traversable[String], separator:String = "")(implicit codec: Codec = Codec.default): Unit = {
    val sepBytes = codec encode separator
    appendChannel { c =>
      (strings foldLeft false){
        (addSep, string) =>
          if(addSep) writeTo(c, sepBytes, Long.MaxValue)
          writeTo(c, codec encode string, Long.MaxValue)

          true
      }
    }
  }

  /**
   * Truncate/Chop the Seekable to the number of bytes declared by the position param
   */
  def truncate(position : Long) : Unit = {
       appendChannel{_.truncate(position)}
  }
   /**
    * Truncate/Chop the Seekable to the number of bytes declared by the position param.  In this
    * method each position is one character instead of bytes.
    */
  def truncateString(position : Long)(implicit codec:Codec = Codec.default) : Unit = {
    val posInBytes = charCountToByteCount(0,position)
    appendChannel{_.truncate(posInBytes)}
  }


  protected def underlyingOutput: OutputResource[OutputStream] =
    Resource.fromOutputStream(new ChannelOutputStreamAdapter(underlyingChannel(false)))

  def chars(implicit codec: Codec) = Resource.fromByteChannel(underlyingChannel(false)).chars(codec)

  def bytesAsInts = Resource.fromByteChannel(underlyingChannel(false)).bytesAsInts

  private def charCountToByteCount(start:Long, end:Long)(implicit codec:Codec) = {
    val encoder = codec.encoder
    val byteBuffer = ByteBuffer.allocateDirect(encoder.maxBytesPerChar.toInt)
    val charBuffer = CharBuffer.allocate(1)

    def sizeInBytes(c : Char) = {
      c.toString.getBytes(codec.name).size // this is very inefficient

      /* TODO There is a bug in this implementation when encoding certain characters like \n

      encoder.reset
      byteBuffer.clear()
      charBuffer.put(0,c)
      charBuffer.position(0)
      val result = encoder.encode(charBuffer, byteBuffer, true)

      assert(!result.isUnderflow, "Attempted to encode "+c+" in charset "+codec.name+" but got an underflow error")
      assert(!result.isOverflow, "Attempted to encode "+c+" in charset "+codec.name+" but got an overflow error")
      assert(!result.isError, "Attempted to encode "+c+" in charset "+codec.name+" but got an error")

      println("sizeInBytes of '"+c+"' is "+byteBuffer.position)

      byteBuffer.position
      */
    }

    val segment = Resource.fromByteChannel(underlyingChannel(false)).chars().lslice(start, end)

    (0L /: segment ) { (replacedInBytes, nextChar) =>
          replacedInBytes + sizeInBytes(nextChar)
    }
  }
}

object Seekable {
  class AsSeekable(op: => Seekable) {
    /** An object to an Seekable object */
    def asSeekable: Seekable = op
  }

  /**
   * Wrap an arbitraty object as and AsSeekable object allowing the object to be converted to an Seekable object.
   *
   * The possible types of src are the subclasses of [[scalax.io.AsSeekableConverter]]
   */
  implicit def asSeekableConverter[B](src:B)(implicit converter:AsSeekableConverter[B]) =
    new AsSeekable(converter.toSeekable(src))

    
  /**
   * Used by the [[scalax.io.Seekable]] object for converting an arbitrary object to an Seekable Object
   *
   * Note: this is a classic use of the type class pattern
   */
  trait AsSeekableConverter[-A] {
    def toSeekable(t:A) : Seekable
  }
  
  /**
   * contains several implementations of [[scalax.io.AsSeekableConverter]].  They will be implicitely resolved allowing
   * a user of the library to simple call A.asSeekable and the converter will be found without the user needing to look up these classes
   */
  object AsSeekableConverter {
  
    /**
     * Converts a File to an Seekable object
     */
    implicit object FileConverter extends AsSeekableConverter[File]{
      def toSeekable(file: File) = Resource.fromFile(file)
    }
    /**
     * Converts a RandomAccessFile to an Seekable object
     */
    implicit object RandomAccessFileConverter extends AsSeekableConverter[RandomAccessFile]{
      def toSeekable(raf: RandomAccessFile) = Resource.fromRandomAccessFile(raf)
    }
    /**
     * Converts a FileChannel to an Seekable object
     */
    implicit object FileChannelConverter extends AsSeekableConverter[FileChannel]{
      def toSeekable(channel: FileChannel) = Resource.fromSeekableByteChannel(new SeekableFileChannel(channel))
    }
    /**
     * Converts a SeekableByteChannel to an Seekable object
     */
    implicit object SeekableByteChannelConverter extends AsSeekableConverter[SeekableByteChannel]{
      def toSeekable(channel: SeekableByteChannel) = Resource.fromSeekableByteChannel(channel)
    }
  }
}
