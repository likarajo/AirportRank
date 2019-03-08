import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.spark.sql.SparkSession

object AirportRank {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: AirportRank <data> <iterations> <outputDirectory>\n")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("AirportRank")
      .getOrCreate()

    // Accept datafile as argument from user and create RDD from it
    val lines = spark.read.textFile(args(0)).rdd

    // Form airport pairs for linked airports
    val airport_pairs = lines.map{
      s => val parts = s.split(",")
        (parts(1), parts(4))
    }

    // Find connection from each airport to others: (airport, [list of linked airports] )
    val connections = airport_pairs.distinct().groupByKey().cache()

    // Initialize the rank values for each airport to 10.0
    var ranks = connections.mapValues(v => 10.0)

    // Accept iterations value as argument from user
    val iterations = args(1).toInt

    // Perform iterations to calculate contribution based on the number of links for each airport
    for (i <- 1 to iterations) {

      val contribution = connections.join(ranks) // apply initial rank: (airport, ([list of linked airports], 10.0) )
        .values // extract the value part: ([list of linked airports], 10.0)
        .flatMap{ case (airports, rank) =>
        val size = airports.size // count the number of airports
        airports.map(airport => (airport, rank / size)) // each airport rank is distributed evenly amongst the airports it has routes to
      }
      ranks = contribution.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) //Recalculate ranks based on parameter values
      //(airport, rank)
    }

    // Sort the output based on decreasing value of rank
    val output = ranks.sortBy(-_._2).collect()

    // Print the output
    // output.foreach(tuple => println("Airport " + tuple._1 + " has Rank: " + tuple._2))

    // Save output to file
    val outfile = args(2)+"/airport_rank.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile)))
    output.foreach(tuple => writer.write("Airport " + tuple._1 + " has Rank: " + tuple._2 + "\n"))
    writer.close()

  }

}
