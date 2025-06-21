import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Food {
  def center(sc: SparkContext, matrix: RDD[(Int, Int, Double)]): RDD[(Int, Int, Double)] = {
    // Calculate the sum and count in a single pass using aggregateByKey
    val sumAndCount = matrix
      .map { case (userID, itemID, rating) => (itemID, (rating, 1)) }
      .aggregateByKey((0.0, 0))(
        (acc, value) => (acc._1 + value._1, acc._2 + value._2), // SeqOp: sum and count within partition
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // CombOp: sum and count across partitions
      )

    // Calculate the mean rating for each item
    val meanRatings = sumAndCount.mapValues { case (sum, count) => sum / count }

    // Subtract the mean rating from each original rating
    val centeredRatings = matrix
      .map { case (userID, itemID, rating) => (itemID, (userID, rating)) }
      .join(meanRatings)
      .map { case (itemID, ((userID, rating), meanRating)) => (userID, itemID, rating - meanRating) }

    centeredRatings
  }

  def cosineSimilarityPerRow(sc: SparkContext, matrix: RDD[(Int, Int, Double)]): RDD[((Int, Int), Double)] = {
    // Create (userId, (itemId, rating)) pairs
    val itemUserRating: RDD[(Int, (Int, Double))] = matrix.map {
      case (userId, itemId, rating) => (userId, (itemId, rating))
    }

    // Join on userID to get all item pairs that were rated by the same user
    val joinedRatings: RDD[(Int, ((Int, Double), (Int, Double)))] = itemUserRating.join(itemUserRating)
      .filter {
        case (_, ((itemId1, _), (itemId2, _))) => itemId1 < itemId2 // Avoid duplicate and self pairs
      }

    // Length (norm squared) of each item
    val length: RDD[(Int, Double)] = matrix
      .map { case (_, itemId, rating) => (itemId, rating * rating) }
      .reduceByKey(_ + _)

    // Map to ((itemId1, itemId2), dotProduct)
    val userPairRatings: RDD[((Int, Int), Double)] = joinedRatings.map {
      case (_, ((itemId1, rating1), (itemId2, rating2))) =>
        ((itemId1, itemId2), rating1 * rating2)
    }.reduceByKey(_ + _) // Sum up the dot products for each item pair

    // Compute cosine similarity
    val itemCosineSimilarity: RDD[((Int, Int), Double)] = userPairRatings.mapPartitions { iter =>
      val itemNorms = length.collectAsMap()
      iter.map { case ((itemId1, itemId2), dotProduct) =>
        val normA = math.sqrt(itemNorms.getOrElse(itemId1, 0.0))
        val normB = math.sqrt(itemNorms.getOrElse(itemId2, 0.0))
        val denominator = normA * normB
        val cosineSimilarity = if (denominator == 0.0) 0.0 else dotProduct / denominator
        ((itemId1, itemId2), cosineSimilarity)
      }
    }

    itemCosineSimilarity
  }

  def predictTopK(threshold: Int, UserId: Int, sc: SparkContext, userRecipies: RDD[Int], recipiesToPredict: RDD[Int],
             ratingFromUsers: RDD[(Int, Double)], itemCosineSimilarity: RDD[((Int, Int), Double)]): RDD[(Int, Int, Double)] = {
    val userRecipies2 = userRecipies.map(ur => (ur, true)) // only needed for joining later on
    val recipiesToPredict2 = recipiesToPredict.map(ur => (ur, true)) // only needed for joining later on

    val duplicatesSimilarities = itemCosineSimilarity.flatMap { case ((k1, k2), v) => Seq((k1, (k2, v)), (k2, (k1, v)))}

    // Top k nearest recipies to predict a value that are rated by the user
    // Duplicatesimilaritiesratedbyuser -> (id of item, [(id of top k items similar to id of item, similarity between the item in the list and the key])])
    val duplicateSimilaritiesRatedByUser = duplicatesSimilarities
    .join(userRecipies2).map { case (itemId, (value, _)) => (itemId, value) } // Get recipies that a user rated
    .map{ case (itemId1, (itemId2, sim)) => (itemId2, (itemId1, sim)) } // Reformat
    .groupByKey() // Group them to get a list of rated recipies for each recipie
    .mapValues(iter => iter.toList.sortBy(-_._2).take(threshold)) // For each recipie list get the top 2 rated recipies
    .join(recipiesToPredict2).map { case (itemId, (value, _)) => (itemId, value) } // Kick out all recipies with rating
    .flatMapValues(v => v)
    .map{case (itemId1, (itemId2, sim)) => (itemId2, (itemId1, sim))}
    .join(ratingFromUsers)
    .map{case(itemid2 , ((itemid1, sim), rating)) => (itemid1, (itemid2, sim, rating))}

    //Predict
    val reduced = duplicateSimilaritiesRatedByUser.reduceByKey { (acc, value) =>
      val (accItemid2, accSim, accWeightedRating) = acc
      val (itemid2, sim, rating) = value
      // Combine similarities and weighted ratings for the same itemid1
      (itemid2, accSim + sim, rating * sim + accWeightedRating * accSim)
    }
    val prediction = reduced.mapValues { case (_, totalSim, weightedSum) =>
      weightedSum / totalSim
    }.map{case (itemid1, pred) => (UserId, itemid1, pred)}
    .sortBy(_._3)
    prediction
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 10) {
      println(">>> [ERROR] Expected arguments: Input, UserId, Output for prediction and Output for RMSE")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("Food").setMaster("local[*]").set("spark.sql.shuffle.partitions", "8")
    val sc = new SparkContext(conf)

    // Load and process interactions
        val interactions: RDD[(Int, Int, Double)] = sc.textFile(args(0))
            .filter(line => !line.startsWith("user_id")) // Skip header
            .map(line => line.split(","))
            .filter(parts => parts.length > 3 && parts(0).forall(_.isDigit) && parts(1).forall(_.isDigit) && parts(3).matches("""\d+(\.\d+)?"""))
            //.map(parts => (parts(0).toInt, parts(1).toInt, parts(3).toDouble)) // (userId, itemId, rating) mini sample
            .map(parts => (parts(0).toInt, parts(5).toInt, parts(3).toDouble - 1)) // (userId, itemId, rating) real data

    interactions.saveAsTextFile(args(1))

    sc.stop()
  }
}
