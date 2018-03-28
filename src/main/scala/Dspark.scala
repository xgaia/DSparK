import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.rdd.RDDFunctions._
//import scala.util.{Failure, Success, Try}
import fr.inra.lipm.general.paramparser.ArgParser
import scala.collection.immutable.BitSet


object Dspark {
  def main(args: Array[String]) = {

    // Spark configuration
    val conf = new SparkConf().setAppName("DSparK")
    val sc = new SparkContext(conf)

    // Param parser
    val parser = new ArgParser()
    parser.addParamString("input", 'i', "Input files or directory")
    parser.addParamString("input-type", 't', "Input type")
    parser.addParamString("output", 'o', "Output directory")
    parser.addParamLong("kmer-size", 'k', "Kmer Size", Some(31))
    parser.addParamLong("abundance-max", 'x', "Maximum abundance", Some(2147483647))
    parser.addParamLong("abundance-min", 'n', "Minimum abundance", Some(2))
    parser.addParamCounter("sorted", 's')
    parser.addParamString("format", 'f', "Output format", Some("binary"))

    parser.parse(args)

    parser.assertAllowedValue("input-type", Array("fasta", "fastq"))
    parser.assertAllowedValue("format", Array("binary", "text"))
    // TODO: assertion (don't work well in Spark, filesystem are different)
    //parser.assertPathIsFile("input")

    val input = parser.getString("input")
    val inputType = parser.getString("input-type")
    val output = parser.getString("output")
    val kmerSize = parser.getLong("kmer-size").toInt
    val abundanceMax = parser.getLong("abundance-max")
    val abundanceMin = parser.getLong("abundance-min")
    val sorted = parser.getCounter("sorted")
    val format = parser.getString("format")

    val nuclMapForward = Map('A' -> BitSet(), 'T' -> BitSet(0), 'G' -> BitSet(0, 1), 'C' -> BitSet(1))
    val nuclMapReverse = Map('T' -> BitSet(), 'A' -> BitSet(0), 'C' -> BitSet(0, 1), 'G' -> BitSet(1))
    val boolTupleMap = Map((false, false) -> 'A', (true, false) -> 'T', (true, true) -> 'G', (false, true) -> 'C')

    //Broadcast variables on all nodes
    val broadcastedKmerSize = sc.broadcast(kmerSize)
    val broadcastedHalphKmerSize = sc.broadcast((kmerSize / 2 ) + 1)
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)
    val broadcastedNuclMapForward = sc.broadcast(nuclMapForward)
    val broadcastedNuclMapReverse = sc.broadcast(nuclMapReverse)
    val broadcastedBoolTupleMap = sc.broadcast(boolTupleMap)

    // main --------------------------
    val reads = inputType match {
      case "fasta" => sc.textFile(input).sliding(2, 2).map{case Array(id, seq) => seq}
      case "fastq" => sc.textFile(input).sliding(4, 4).map{case Array(id, seq, _, qual) => seq}
    }

    def readToBinaryKmersIterator(iterReads: Iterator[String]): Iterator[(BitSet, Int)] = {

      def sequenceToBinaryCanonicalKmers(sequence: String): Array[(BitSet, Int)] = {
        // Build the first kmer
        val firstStringKmer = sequence.take(broadcastedKmerSize.value)
        val firstBinaryKmerTuple = kmerToBitsetTuple(firstStringKmer, firstStringKmer.reverse, (BitSet(), BitSet()))
        // Send the rest of the sequence to the extends fonction
        val restOfSequence = sequence.takeRight(sequence.length - broadcastedKmerSize.value)
        val arrayOfKmersBinaryTuple = extendsArrayOfKmersTuple(restOfSequence, Array(firstBinaryKmerTuple))
        // return the canonical kmers
        arrayOfKmersBinaryTuple.map(getCanonicalWithOne)
      }

      def kmerToBitsetTuple(kmer: String, revKmer: String, bitsetTuple: (BitSet, BitSet)): (BitSet, BitSet) = {
        val kmerLen = kmer.length
        if (kmerLen == 0) {
          bitsetTuple
        }else{
          val bitPosition = (broadcastedKmerSize.value - kmerLen) * 2
          val forwardKmerBitset = bitsetTuple._1 ++ broadcastedNuclMapForward.value(kmer.head).map(_ + bitPosition)
          val reverseKmerBitset = bitsetTuple._2 ++ broadcastedNuclMapReverse.value(revKmer.head).map(_ + bitPosition)
          kmerToBitsetTuple(kmer.tail, revKmer.tail, (forwardKmerBitset, reverseKmerBitset))
        }
      }

      def extendsArrayOfKmersTuple(restOfStringSequence:String, arrayOfBinaryKmersTuple: Array[(BitSet, BitSet)]): Array[(BitSet, BitSet)] = {
        val lenOfRest = restOfStringSequence.length
        val bitPosition = (broadcastedKmerSize.value - 1) * 2
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
          val newForward = shiftedForward ++ broadcastedNuclMapForward.value(head).map(_ + bitPosition)
          // insert the reverse nucl at the beginning of the reverse strand
          val newReverse = shiftedReverse ++ broadcastedNuclMapReverse.value(head)
          extendsArrayOfKmersTuple(tail, arrayOfBinaryKmersTuple :+ (newForward, newReverse))
        }
      }

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

      iterReads.flatMap(sequenceToBinaryCanonicalKmers)
    }

    // Bitset Conversion   ----------------------------------------
    def bitsetToBinaryString(bitset: BitSet): String = {
      bitset.toBitMask.map(_.toChar).mkString
    }

    def binaryStringToBitset(binaryString: String): BitSet = {
      BitSet.fromBitMask(binaryString.toCharArray.map(_.toLong))
    }

    def bitsetToAsciistring(bitset: BitSet, asciiString: String): String = {
      val len = asciiString.length
      if (len == broadcastedKmerSize.value) {
        asciiString
      }else{
        val bitPosition = len * 2
        val bitPositionPlusOne = bitPosition + 1
        val newAsciiString = asciiString + broadcastedBoolTupleMap.value((bitset.contains(bitPosition), bitset.contains(bitPositionPlusOne)))
        val newBitset = bitset - bitPosition - bitPositionPlusOne
        bitsetToAsciistring(newBitset, newAsciiString)
      }
    }

    def binaryStringToAsciiString(binaryString: String): String = {
      bitsetToAsciistring(binaryStringToBitset(binaryString), "")
    }
    // -------------------------------------------------------------------------

    // Convert reads to binary canonical kmers tuple => (BinaryCanonicalKmer, 1)
    val binaryKmers = reads.mapPartitions(readToBinaryKmersIterator)

    // Count kmer
    val countedBinaryKmers = binaryKmers.reduceByKey(_ + _)

    // filter on abundance
    val filteredBinaryKmers = countedBinaryKmers.filter(kmer_tpl => kmer_tpl._2 >= broadcastedAbundanceMin.value && kmer_tpl._2 <= broadcastedAbundanceMax.value)

    // ascii or binary output
    val formatedOutput = format match {
      case "binary" => filteredBinaryKmers.map(tpl => (bitsetToBinaryString(tpl._1), tpl._2))
      case "text" => {
        // sort ?
        if (sorted != 0){
          filteredBinaryKmers.map(tpl => (bitsetToAsciistring(tpl._1, ""), tpl._2)).sortByKey()
        }else {
          filteredBinaryKmers.map(tpl => (bitsetToAsciistring(tpl._1, ""), tpl._2))
        }
      }
    }

    // Save results
    formatedOutput.saveAsTextFile(output)
  }
}
