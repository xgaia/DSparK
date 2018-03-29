import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.rdd.RDDFunctions._
//import scala.util.{Failure, Success, Try}
import fr.inra.lipm.general.paramparser.ArgParser

import sequence.Sequence



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

    // def a sequence object (contain all sequence method)
    val sequence = new Sequence(kmerSize)
    // broadcast this object to all nodes
    val broadcastedSequence = sc.broadcast(sequence)

    //Broadcast variables on all nodes
    //FIXME: Test if broadcast abundance is efficient
    val broadcastedAbundanceMax = sc.broadcast(abundanceMax)
    val broadcastedAbundanceMin = sc.broadcast(abundanceMin)


    // main --------------------------

    // get reads from fasta/q files
    val reads = inputType match {
      case "fasta" => sc.textFile(input).sliding(2, 2).map{case Array(id, seq) => seq}
      case "fastq" => sc.textFile(input).sliding(4, 4).map{case Array(id, seq, _, qual) => seq}
    }

    // Convert reads to binary canonical kmers tuple => (BinaryCanonicalKmer, 1)
    val binaryKmers = reads.mapPartitions(broadcastedSequence.value.readToBinaryKmersIterator)

    // Count kmer
    val countedBinaryKmers = binaryKmers.reduceByKey(_ + _)

    // filter on abundance
    val filteredBinaryKmers = countedBinaryKmers.filter(kmer_tpl => kmer_tpl._2 >= broadcastedAbundanceMin.value && kmer_tpl._2 <= broadcastedAbundanceMax.value)

    // ascii or binary output
    val formatedOutput = format match {
      case "binary" => filteredBinaryKmers.map(tpl => (broadcastedSequence.value.bitsetToLong(tpl._1), tpl._2))
      case "text" => {
        // sort ?
        if (sorted != 0){
          filteredBinaryKmers.map(tpl => (broadcastedSequence.value.bitsetToAsciistring(tpl._1, ""), tpl._2)).sortByKey()
        }else {
          filteredBinaryKmers.map(tpl => (broadcastedSequence.value.bitsetToAsciistring(tpl._1, ""), tpl._2))
        }
      }
    }

    // Save results
    formatedOutput.saveAsTextFile(output)
  }
}
