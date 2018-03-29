package sequence

import scala.collection.immutable.BitSet


class Sequence(kmerSize: Int) extends java.io.Serializable {

  val nuclMapForward = Map('A' -> BitSet(), 'T' -> BitSet(0), 'G' -> BitSet(0, 1), 'C' -> BitSet(1))
  val nuclMapReverse = Map('T' -> BitSet(), 'A' -> BitSet(0), 'C' -> BitSet(0, 1), 'G' -> BitSet(1))
  val boolTupleMap = Map((false, false) -> 'A', (true, false) -> 'T', (true, true) -> 'G', (false, true) -> 'C')

  def readToBinaryKmersIterator(iterReads: Iterator[String]): Iterator[(BitSet, Int)] = {
    iterReads.flatMap(sequenceToBinaryCanonicalKmers)
  }

  def sequenceToBinaryCanonicalKmers(sequence: String): Array[(BitSet, Int)] = {
    // Build the first kmer
    val firstStringKmer = sequence.take(kmerSize)
    val firstBinaryKmerTuple = kmerToBitsetTuple(firstStringKmer, firstStringKmer.reverse, (BitSet(), BitSet()))
    // Send the rest of the sequence to the extends fonction
    val restOfSequence = sequence.takeRight(sequence.length - kmerSize)
    val arrayOfKmersBinaryTuple = extendsArrayOfKmersTuple(restOfSequence, Array(firstBinaryKmerTuple))
    // return the canonical kmers
    arrayOfKmersBinaryTuple.map(getCanonicalWithOne)
  }

  def kmerToBitsetTuple(kmer: String, revKmer: String, bitsetTuple: (BitSet, BitSet)): (BitSet, BitSet) = {
    val kmerLen = kmer.length
    if (kmerLen == 0) {
      bitsetTuple
    }else{
      val bitPosition = (kmerSize - kmerLen) * 2
      val forwardKmerBitset = bitsetTuple._1 ++ nuclMapForward(kmer.head).map(_ + bitPosition)
      val reverseKmerBitset = bitsetTuple._2 ++ nuclMapReverse(revKmer.head).map(_ + bitPosition)
      kmerToBitsetTuple(kmer.tail, revKmer.tail, (forwardKmerBitset, reverseKmerBitset))
    }
  }

  def extendsArrayOfKmersTuple(restOfStringSequence:String, arrayOfBinaryKmersTuple: Array[(BitSet, BitSet)]): Array[(BitSet, BitSet)] = {
    val lenOfRest = restOfStringSequence.length
    val bitPosition = (kmerSize - 1) * 2
    if (lenOfRest == 0) {
      arrayOfBinaryKmersTuple
    }else {
      val head = restOfStringSequence.head
      val tail = restOfStringSequence.tail
      // shift the forward kmer (remove 0 and 1, and subtract 2 of all value)
      val shiftedForward = arrayOfBinaryKmersTuple.last._1.-(0).-(1).map(_ - 2)
      // shift the reverse kmer (remove the last bit)
      val shiftedReverse = arrayOfBinaryKmersTuple.last._2.-(bitPosition).-(bitPosition + 1).map(_ + 2)
      // insert the new nucl at the end of the forward strand
      val newForward = shiftedForward ++ nuclMapForward(head).map(_ + bitPosition)
      // insert the reverse nucl at the beginning of the reverse strand
      val newReverse = shiftedReverse ++ nuclMapReverse(head)
      extendsArrayOfKmersTuple(tail, arrayOfBinaryKmersTuple :+ (newForward, newReverse))
    }
  }

  //FIXME: Improve this method
  def getCanonicalWithOne(bitsetTuple: (BitSet, BitSet)): (BitSet, Int) = {
    (bitsetTuple._1.isEmpty, bitsetTuple._2.isEmpty) match {
      case (false, false) => {
        if ((bitsetTuple._1 &~ bitsetTuple._2).min > (bitsetTuple._2 &~ bitsetTuple._1).min) {
          (bitsetTuple._1, 1)
        }else{
          (bitsetTuple._2, 1)
        }
      }
      case (true, false) => (bitsetTuple._1, 1)
      case _ => (bitsetTuple._2, 1)
    }
  }

  // Bitset Conversion
  def asciiStringToBitset(asciiString: String, bitset: BitSet): BitSet = {
  val kmerLen = asciiString.length
    if (kmerLen == 0){
      bitset
    }else{
      val bitPosition = (kmerSize - kmerLen) * 2
      val newBitset = bitset ++ nuclMapForward(asciiString.head).map(_ + bitPosition)
      asciiStringToBitset(asciiString.tail, newBitset)
    }
  }

  def bitsetToLong(bitset: BitSet): Long = {
    bitset.toBitMask(0)
  }

  def longToBitset(long: Long): BitSet = {
    BitSet.fromBitMask(Array(long))
  }

  def bitsetToAsciistring(bitset: BitSet, asciiString: String): String = {
    val len = asciiString.length
    if (len == kmerSize) {
      asciiString
    }else{
      val bitPosition = len * 2
      val bitPositionPlusOne = bitPosition + 1
      val newAsciiString = asciiString + boolTupleMap((bitset.contains(bitPosition), bitset.contains(bitPositionPlusOne)))
      val newBitset = bitset - bitPosition - bitPositionPlusOne
      bitsetToAsciistring(newBitset, newAsciiString)
    }
  }

  def longToAsciiString(long: Long): String = {
    bitsetToAsciistring(longToBitset(long), "")
  }
}
