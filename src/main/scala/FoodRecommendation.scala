import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FoodRecommendation {
  def center(sc: SparkContext, matrix: RDD[(Int, Int, Double)]): RDD[(Int, Int, Double)] = {
    // Group by itemID
    val grouped = matrix.groupBy(_._2)

    // Count the number of ratings per item
    val n = grouped.mapValues(_.size)
      .map { case (itemID, count) => (itemID, count) }

    // Sum the ratings per item
    val summ = grouped.mapValues(_.map(_._3).sum)
      .map { case (itemID, sum) => (itemID, sum) }

    // Calculate the mean rating per user
    val mittelwert = summ.join(n)
      .map { case (itemID, (sum, count)) => (itemID, sum / count) }

    // Join the original data with the mean rating to get the final result
    val endergebnis = matrix.map(ur => (ur._2, ur))
      .join(mittelwert)
      .map { case (itemID, (ur, meanRating)) => (ur._1, ur._2, ur._3 - meanRating) }
    endergebnis
  }

  def cosineSimilarityPerRow(sc: SparkContext, matrix: RDD[(Int, Int, Double)]): RDD[((Int, Int), Double)] = {
      // Create (userId, (itemId, rating)) pairs
      val itemUserRating: RDD[(Int, (Int, Double))] = matrix.map {
        case (userId, itemId, rating) => (userId, (itemId, rating))
      }

      // Join on userID to get all item pairs who was rated the same 
      val joinedRatings: RDD[(Int, ((Int, Double), (Int, Double)))] = itemUserRating.join(itemUserRating)
        .filter {
          case (_, ((itemId1, _), (itemId2, _))) => itemId1 < itemId2 // Avoid duplicate and self pairs
        }

      // Length of each item
      val length = matrix.map {
        case (userId, itemId, rating) => 
        (itemId, rating*rating)
      }.reduceByKey(_+_)

      // Map to ((itemId1, itemId2), (dotProduct, normA, normB))
      val userPairRatings: RDD[((Int, Int), (Double, Double, Double))] = joinedRatings.map {
        case (_, ((itemId1, rating1), (itemId2, rating2))) =>
          ((itemId1, itemId2), (rating1 * rating2, rating1, rating2))
    }

    // Group by item pairs
    val groupedUserPairRatings: RDD[((Int, Int), Iterable[(Double, Double, Double)])] = userPairRatings.groupByKey()
    // val itemNormsMap = length.collectAsMap()
    // val broadcastItemNorms = sc.broadcast(itemNormsMap)

    // // Compute cosine similarity
    // val itemCosineSimilarity: RDD[((Int, Int), Double)] = groupedUserPairRatings.map {
    //   case (key, values) =>
    //     val sumDotProduct = values.map { case (first, _, _) => first }.sum

    //     // Access the norms using the broadcasted variable
    //     val sumNormA = broadcastItemNorms.value.getOrElse(key._1, 0.0)
    //     val sumNormB = broadcastItemNorms.value.getOrElse(key._2, 0.0)

    //     val denominator = math.sqrt(sumNormA) * math.sqrt(sumNormB)
    //     if (denominator == 0.0) (key, 0.0) else (key, sumDotProduct / denominator)
    // }

    val itemCosineSimilaritynew = groupedUserPairRatings
      .map { case (key, values) => (key._1, (key._2, values.map { case (first, _, _) => first }.sum)) }
      .join(length)
      .map { case (itemid1, ((itemid2, dot), len1)) => (itemid2, (itemid1, dot, len1))}
      .join(length)
      .map { case (itemid2, ((itemid1, dot, len1), len2)) => ((itemid1, itemid2), dot / (0.000001 + math.sqrt(len1) * math.sqrt(len2)))}

    itemCosineSimilaritynew
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
    .mapValues(iter => iter.filter{case(id, v) => v > 0})//.toList.sortBy(-_._2).take(threshold)) // For each recipie list get the top 2 rated recipies
    .join(recipiesToPredict2).map { case (itemId, (value, _)) => (itemId, value) } // Kick out all recipies with rating
    .flatMapValues(v => v)
    .map{case (itemId1, (itemId2, sim)) => (itemId2, (itemId1, sim))}
    .join(ratingFromUsers)
    .map{case(itemid2 , ((itemid1, sim), rating)) => (itemid1, (itemid2, sim, sim * rating))}

    //Predict
        val reduced = duplicateSimilaritiesRatedByUser.reduceByKey { (lhs, rhs) =>
          val (id1, sim1, weightedrating1) = lhs
          val (id2, sim2, weightedrating2) = rhs
          // Combine similarities and weighted ratings for the same itemid1
          (id2, sim1 + sim2, weightedrating1 + weightedrating2)
        }
        val prediction = reduced.mapValues { case (_, totalSim, weightedSum) =>
          (weightedSum / totalSim)
        }.map{case (itemid1, pred) => (UserId, itemid1, pred)}
        .sortBy(_._3)
        prediction
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 10) {
      println(">>> [ERROR] Expected arguments: Input, UserId, Output for prediction and Output for RMSE")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("FoodRecommendation").setMaster("local[*]").set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    // Load and process interactions
    val interactions: RDD[(Int, Int, Double)] = sc.textFile(args(0))
      .filter(line => !line.startsWith("user_id")) // Skip header
      .map(line => line.split(","))
      .filter(parts => parts.length > 3 && parts(0).forall(_.isDigit) && parts(1).forall(_.isDigit) && parts(3).matches("""\d+(\.\d+)?"""))
      .map(parts => (parts(0).toInt, parts(1).toInt, parts(3).toDouble)) // (userId, itemId, rating)

    // Centralize the data
    val centeralized = center(sc, interactions)

    // Get cosine similarity
    val itemCosineSimilarity = cosineSimilarityPerRow(sc, centeralized)
//    itemCosineSimilarity.saveAsTextFile(args(4))
    val newSim = itemCosineSimilarity.map{case ((itemid1, itemid2), sim) => ((itemid1, itemid2), (sim + 1) / 2 )}

    // Now we have distances and need to predict the missing values for a user
    val threshold = 2
    val UserId = args(1).toInt
    val topK = 5
      
    val allRecipies = centeralized.map(ur => ur._2).distinct()
    val userRecipies = centeralized.filter{ case (userId, itemId, v) => userId == UserId }.map(ur => ur._2).distinct()
    val recipiesToPredict = allRecipies.subtract(userRecipies)
    val ratingFromUsers = interactions.filter{ case (user, item, rating) => user == UserId }
                                        .map{ case (user, item, rating) => (item, rating)}

    // Prediction for empty cells of user
    val prediction = predictTopK(threshold, UserId, sc, userRecipies, recipiesToPredict, ratingFromUsers, itemCosineSimilarity).take(topK)

    sc.parallelize(prediction).saveAsTextFile(args(2))

    // RMSE
    // val squaredError = predictTopK(threshold, UserId, sc, userRecipies, userRecipies, ratingFromUsers, newSim)
    //  .map{ case (uid, iid, pred) => ((iid), (pred))}
    //  .join(ratingFromUsers)
    //  .map{case (iid, (pred, truth)) => (iid, pred, truth, (pred - truth) * (pred - truth)) }
    // val rmse = math.sqrt(squaredError.map{case (ite, pre, tru, err) => (err)}.reduce(_ + _)) / squaredError.count()
    // println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    // println(rmse)
    // println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    // squaredError.saveAsTextFile(args(3))

    sc.stop()
  }
}
