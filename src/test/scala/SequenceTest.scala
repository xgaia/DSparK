import org.scalatest._
import scala.collection.immutable.BitSet
import sequence.Sequence


class SequenceTest extends FlatSpec {

  val ksize = 31
  val sequence = new Sequence(ksize)

  val asciiString = "ATGTAAACAACATTACACAAATAAAAAAAGT"
  val bitset = BitSet(2,4,5,6,15,21, 24, 31, 35, 42, 26, 31, 58, 59, 60)
  val long = 2017617067701731444L

  "Ascii string" must "be converted to BitSet" in {
    assert(sequence.asciiStringToBitset(asciiString, BitSet()) == bitset)
  }

  "Bitset" must "be converted to long" in {
    assert(sequence.bitsetToLong(bitset) == long)
  }

  "Long" must "be converted to BitSet" in {
    assert(sequence.longToBitset(long) == bitset)
  }

  "BitSet" must "be converted to ASCII string" in {
    assert(sequence.bitsetToAsciistring(bitset, "") == asciiString)
  }

  "Long" must "be converted to ASCII string" in {
    assert(sequence.longToAsciiString(long) == asciiString)
  }



}
