import org.apache.spark.{SparkConf, SparkContext}

object AirportRank extends Serializable {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: AirportRank <data> <iterations> <outputDirectory>\n")
      System.exit(1)
    }

    val inputFile = args(0)
    val iterations = args(1).toInt
    val outputDir = args(2)

    val conf = new SparkConf().setAppName("MoviePlot")
    val sc = new SparkContext(conf)

    // Accept datafile as argument from user and create RDD from it
    val lines = sc.textFile(inputFile)

    // form airport pairs for linked airports
    val airport_pairs = lines.map {
      s =>
        val parts = s.split(",")
        (parts(1), parts(4))
    }

    // find connection from each airport to others: (airport, [list of linked airports] )
    val connections = airport_pairs.distinct().groupByKey()

    // Initialize the rank values for each airport to 10.0
    var ranks = connections.mapValues(v => 10.0)

    // Perform multiple iterations based on user input
    // to calculate contribution based on the number of links for each airport

    for (i <- 1 to iterations) {
      val contribution = connections.join(ranks) // apply initial rank: (airport, ([list of linked airports], 10.0) )
        .values // extract the value part: ([list of linked airports], 10.0)
        .flatMap { case (airports, rank) =>
        val size = airports.size // count the number of airports
        airports.map(airport => (airport, rank / size)) // each airport rank is distributed evenly amongst the airports it has routes to
      }
      ranks = contribution.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) //Recalculate ranks based on parameter values
    }

    // Sort the output based on decreasing value of rank
    val output = ranks.sortBy(-_._2)

    // Save output to file
    output.saveAsTextFile(outputDir)

  }

}
