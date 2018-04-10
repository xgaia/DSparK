import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.rdd.RDDFunctions._
import fr.inra.lipm.general.paramparser.ArgParser

import sequence.Sequence

object Dspark {
  def main(args: Array[String]) = {

    // Spark configuration
    val conf = new SparkConf().setAppName("DSparK")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("DSparK")
      .getOrCreate()

    import spark.implicits._

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
    parser.assertMax("kmer-size", 31)
    parser.assertCondition("kmer-size",  _ % 2 != 0)
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

    // get a dataFrame of kmer
    val binaryKmersDataFrame = reads.mapPartitions(broadcastedSequence.value.sequenceToLongCanonicalKmersIterator).toDF("kmer")

    // count kmers
    val result = binaryKmersDataFrame.groupBy("kmer").count.filter($"count" >= abundanceMin).filter($"count" <= abundanceMax)

    // Format and sort if needed
    def longToString:(Long => String) = {long => sequence.longToString(long)}
    val longToStringUDF = udf(longToString)

    val formatedResults = format match {
      case "binary" => result
      case "text" => {
        if (sorted != 0) {
          result.withColumn("string kmer", longToStringUDF(result("kmer"))).select("string kmer", "count").sort("string kmer")
        }else{
          result.withColumn("string kmer", longToStringUDF(result("kmer"))).select("string kmer", "count")
        }
      }
    }

    // output as CSV
    formatedResults.write.format("csv").save(output)
  }
}
