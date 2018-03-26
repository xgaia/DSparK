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

    val sortOrder = Map('A' -> 0, 'C' -> 1, 'T' -> 2, 'G' -> 3)
    val baseComplement = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')

    //Broadcast variables on all nodes
    val broadcastedKmerSize = sc.broadcast(kmerSize)
    val broadcastedHalphKmerSize = sc.broadcast((kmerSize / 2 ) + 1)
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)
    val broadcastedSortOrder = sc.broadcast(sortOrder)
    val broadcastedBaseComplement = sc.broadcast(baseComplement)

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
          val index = broadcastedKmerSize.value - kmerLen
          val forwardKmerBitset = kmer.head match {
            case 'C' => bitsetTuple._1 + (index * 2 + 1)
            case 'T' => bitsetTuple._1 + (index * 2)
            case 'G' => bitsetTuple._1 + (index * 2) + (index * 2 + 1)
            case _ => bitsetTuple._1
          }
          val reverseKmerBitset = revKmer.head match {
            case 'G' => bitsetTuple._2 + (index * 2 + 1)
            case 'A' => bitsetTuple._2 + (index * 2)
            case 'C' => bitsetTuple._2 + (index * 2) + (index * 2 + 1)
            case _ => bitsetTuple._2
          }
          kmerToBitsetTuple(kmer.tail, revKmer.tail, (forwardKmerBitset, reverseKmerBitset))
        }
      }

      def extendsArrayOfKmersTuple(restOfStringSequence:String, arrayOfBinaryKmersTuple: Array[(BitSet, BitSet)]): Array[(BitSet, BitSet)] = {
        val lenOfRest = restOfStringSequence.length
        val ksizeMinusOne = broadcastedKmerSize.value - 1
        if (lenOfRest == 0) {
          arrayOfBinaryKmersTuple
        }else {
          val head = restOfStringSequence.head
          val tail = restOfStringSequence.tail
          // shift the forward kmer (remove 0 and 1, and subtract 2 of all value)
          val shiftedForward = arrayOfBinaryKmersTuple.last._1.-(0).-(1).map(_ - 2)
          // shift the reverse kmer (remove the last bit)
          val shiftedReverse = arrayOfBinaryKmersTuple.last._2.-(ksizeMinusOne * 2).-(ksizeMinusOne * 2 + 1).map(_ + 2)
          // insert the new nucl at the end of the forward strand
          val newForward = head match {
            case 'C' => shiftedForward + (ksizeMinusOne * 2 + 1)
            case 'T' => shiftedForward + (ksizeMinusOne * 2)
            case 'G' => shiftedForward + (ksizeMinusOne * 2) + (ksizeMinusOne * 2 + 1)
            case _ => shiftedForward // A
          }
          // insert the compl nucl at the begining of the reverse strand
          val newReverse = head match {
            case 'G' => shiftedReverse + 1     // C
            case 'A' => shiftedReverse + 0     // T
            case 'C' => shiftedReverse + 0 + 1 // G
            case _ => shiftedReverse           // A
          }
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
    def bitsetToKmer(binaryKmer: BitSet, kmer: String): String = {
      val kmerLen = kmer.length
      if (kmerLen == broadcastedKmerSize.value) {
        kmer
      }else{
        val index = kmerLen
        val bitPosOne = binaryKmer.contains(index * 2)
        val bitPosTwo = binaryKmer.contains(index * 2 + 1)

        val bitTuple = (bitPosOne, bitPosTwo)

        val newNucl = bitTuple match{
          case (true, true) => 'G'
          case (true, false) => 'T'
          case (false, true) => 'C'
          case _ => 'A'
        }

        val newBitset = binaryKmer - (index * 2) - (index * 2 + 1)
        val extendedKmer = kmer + newNucl

        bitsetToKmer(newBitset, extendedKmer)
      }
    }

    def bitsetToBinaryString(bitset: BitSet): String = {
      bitset.toBitMask.map(_.toChar).mkString
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
          filteredBinaryKmers.map(tpl => (bitsetToKmer(tpl._1, ""), tpl._2)).sortByKey()
        }else {
          filteredBinaryKmers.map(tpl => (bitsetToKmer(tpl._1, ""), tpl._2))
        }
      }
    }

    // Save results
    formatedOutput.saveAsTextFile(output)
  }
}
