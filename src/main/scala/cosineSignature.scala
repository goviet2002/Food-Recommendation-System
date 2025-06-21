import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.math.signum
import scala.util.hashing.MurmurHash3
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.kg
import FoodRecommendation.predictTopK

object CosineSignature{
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


    def CompleteCosineSimilarityPerRow(sc: SparkContext, matrix: RDD[(Int, Int, Double)]): RDD[((Int, Int), Double)] = {
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
      val itemNormsMap = length.collectAsMap()
      val broadcastItemNorms = sc.broadcast(itemNormsMap)

      // Compute cosine similarity
      val itemCosineSimilarity: RDD[((Int, Int), Double)] = groupedUserPairRatings.map {
        case (key, values) =>
          val sumDotProduct = values.map { case (first, _, _) => first }.sum

          // Access the norms using the broadcasted variable
          val sumNormA = broadcastItemNorms.value.getOrElse(key._1, 0.0)
          val sumNormB = broadcastItemNorms.value.getOrElse(key._2, 0.0)

          val denominator = math.sqrt(sumNormA) * math.sqrt(sumNormB)
          if (denominator == 0.0) (key, 0.0) else (key, sumDotProduct / denominator)
      }
      itemCosineSimilarity
  }

    def candidateCosineSimilarityPerRow(sc: SparkContext, matrix: RDD[(Int, Int, Double)], candidates: RDD[(Int, Int)]): RDD[((Int, Int), Double)] = {
      // Create (userId, (itemId, rating)) pairs
      val itemUserRating: RDD[(Int, (Int, Double))] = matrix.map {
        case (userId, itemId, rating) => (userId, (itemId, rating))
      }

      // Join on userID to get all item pairs who was rated the same 
      val candidatePairsSet = candidates.collect().toSet
      val candidatePairsBroadcast = sc.broadcast(candidatePairsSet)
      val joinedRatings: RDD[(Int, ((Int, Double), (Int, Double)))] = itemUserRating.join(itemUserRating)
        .filter {
          case (_, ((itemId1, _), (itemId2, _))) => itemId1 < itemId2 && candidatePairsBroadcast.value.contains((itemId1, itemId2)) // Avoid duplicate and self pairs
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
      val itemNormsMap = length.collectAsMap()
      val broadcastItemNorms = sc.broadcast(itemNormsMap)

      // Compute cosine similarity
      val itemCosineSimilarity: RDD[((Int, Int), Double)] = groupedUserPairRatings.map {
        case (key, values) =>
          val sumDotProduct = values.map { case (first, _, _) => first }.sum

          // Access the norms using the broadcasted variable
          val sumNormA = broadcastItemNorms.value.getOrElse(key._1, 0.0)
          val sumNormB = broadcastItemNorms.value.getOrElse(key._2, 0.0)

          val denominator = math.sqrt(sumNormA) * math.sqrt(sumNormB)
          if (denominator == 0.0) (key, 0.0) else (key, sumDotProduct / denominator)
      }
      itemCosineSimilarity
  }

    def cosineSignatureMatrix(sc: SparkContext, num_planes: Int, matrix: RDD[(Int, Int, Double)]): RDD[(Int, Int, Double)] = {
        // Get the random hyperplanes (plain id, recipe id, value)
        val num_cols = matrix.map{case (uid, iid, _) => (uid)}.distinct().count().toInt
        val hyperplanes: RDD[(Int, Int, Double)] = sc.parallelize(for {
          i <- 0 until num_planes
          j <- 0 until num_cols
        //} yield (i, j, 2 * (Random.nextDouble() - 0.5))) // Random in 0,1, map to -1,1
        } yield (i, j, if (Random.nextBoolean()) 1 else -1)) // Random -1 or 1

        // Multiply matrices to get the signature (item id, plane id, value)
        val signature: RDD[(Int, Int, Double)] = hyperplanes
          .map { case (plain, recipe, rand) => (recipe, (plain, rand)) } // Reformat A for join
          .join(matrix.map { case (uid, iid, rating) => (uid, (iid, rating)) }) // Join on k
          .map { case (uid, ((plane, rand), (iid, rating))) => ((iid, plane), rand * rating) } // Multiply the values
          .reduceByKey(_+_) // Sum up products with the same (i, j)
          .map{case ((iid, plane), dot) => (iid, plane, signum(dot))}
        signature
    }

    def hashFun(values: Seq[Long]): Int = {
        MurmurHash3.seqHash(values)
    }

    def lSHashing(spark: SparkContext, input: RDD[(Int, Int, Double)], b: Int, k: Int): RDD[(Int, Int)] = {
        // Calculate 'r' as the number of rows per band
        val r = (k / b).toInt

        // Generate band signature matrix with (sigma, id, signature, b)
        val bandSigMatRDD: RDD[(Int, Int, Double, Int)] = input.map {
            case (itemid, sigma, signature) =>
            (itemid, sigma, signature, (sigma / r).toInt)
        }
        
        // Create candidate pairs by hashing and grouping
        val candidatePairsinter = bandSigMatRDD
        .groupBy { case (itemid, _, _, b) => (b, itemid) }
        .mapValues(itr => itr.toSeq.sortBy(_._2))  // Sort individual sequences
        .map { case ((b, id), sigs) => ((b, hashFun(sigs.map{case(itemid, sigma, signature, b) => signature.toLong})), id)}  // Flatten and hash
//        .groupBy { case (b, _, hash) => (b, hash) }
//        .flatMap { case (_, grouped) =>
//            val ids = grouped.map(_._2).toList.sorted
//            ids.combinations(2).collect {
//              case List(a, b) => (a, b)
//            }
//        }
//        .distinct()

        val candidatePairs = candidatePairsinter
          .join(candidatePairsinter)
          .filter{case ((b, h), (id1, id2)) => id1 < id2}
          .map{case ((b, h), (id1, id2)) => (id1, id2)}
          //.distinct()
        candidatePairs
    }

    def main(args: Array[String]): Unit ={
            if (args.length > 10) {
              println(">>> [ERROR] Expected arguments: Input, UserId, Output for prediction and Output for RMSE")
              sys.exit(1)
            }

        val num_planes = args(1).toInt

        val conf = new SparkConf().setAppName("CosineSignature").setMaster("local[*]").set("spark.sql.shuffle.partitions", "1").set("spark.driver.maxResultSize", "4g")
        val sc = new SparkContext(conf)
        
        /////////////////// item profiles by rating /////////////////////////////
        // Get the interactions (userid, recipe id, value)
        val interactions: RDD[(Int, Int, Double)] = sc.textFile(args(0))
            .filter(line => !line.startsWith("user_id")) // Skip header
            .map(line => line.split(","))
            .filter(parts => parts.length > 3 && parts(0).forall(_.isDigit) && parts(1).forall(_.isDigit) && parts(3).matches("""\d+(\.\d+)?"""))
            //.map(parts => (parts(0).toInt, parts(1).toInt, parts(3).toDouble)) // (userId, itemId, rating) mini sample
            .map(parts => (parts(0).toInt, parts(5).toInt, parts(3).toDouble)) // (userId, itemId, rating) real data

        // val centeralized = center(sc, interactions)
        val centeralized = interactions

        // Get the number of recipes to know how many random numbers to generate
        val signature = cosineSignatureMatrix(sc, num_planes, centeralized)

        // LSHashing
        val band = args(2).toInt
        val candidatePairs = lSHashing(sc, signature, band, num_planes)
        val sz = candidatePairs.count()
        println("All candidates", sz)

        // get cosine similarity for candidate pairs
        val similarities = candidateCosineSimilarityPerRow(sc, centeralized, candidatePairs)
        //val similarities = CompleteCosineSimilarityPerRow(sc, centeralized)

        // predict with cosine similarity
        val thresh = 2
        val uid = args(5).toInt // 1535, 4740, 2695
        val allRecipies = centeralized.map(ur => ur._2).distinct()
        val userRecipies = centeralized.filter{ case (userId, itemId, v) => userId == uid }.map(ur => ur._2).distinct()
        val recipiesToPredict = allRecipies.subtract(userRecipies)
        val ratingFromUsers = interactions.filter{ case (user, item, rating) => user == uid }.map{ case (user, item, rating) => (item, rating)}

        val szR = userRecipies.count()
        println("Recipies rated by user", szR)        
        val szP = recipiesToPredict.count()
        println("Recipies to predict", szP)
        
        // val predictions = predictTopK(thresh, uid, sc, userRecipies, recipiesToPredict, ratingFromUsers, similarities)
        // predictions.saveAsTextFile(args(3))
        val errorpreds = predictTopK(thresh, uid, sc, userRecipies, userRecipies, ratingFromUsers, similarities)

        // get the rmse for the prediction
         val error = errorpreds
          .map{ case (uid, iid, pred) => ((iid), (pred))}
          .join(ratingFromUsers)
          .map{case (iid, (pred, truth)) => (iid, pred, truth, math.abs(pred - truth)) }
        error.cache()
         val rmse = math.sqrt(error.map{case (ite, pre, tru, err) => (err * err)}.reduce(_ + _)) / math.sqrt(error.count())
         val avgerr = error.map{case (ite, pre, tru, err) => (err)}.reduce(_ + _) / error.count()
         println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
         println("Recipies rated by user", szR) 
         println("All candidates", sz)
         println("RMSE", rmse)
         println("AVGERR", avgerr)
         println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")


        // Check if the cosine approximation works (minhash vs real cos dist)
        // val comparison = CompleteCosineSimilarityPerRow(sc, interactions.map{case (r, c, v) => (r, c, v)})
        //     .join(CompleteCosineSimilarityPerRow(sc, signature.map{case (r, c, v) => (c, r, v)}))
        //     .map{case ((i1, i2), (real, pred)) => ((i1, i2), (real, pred, math.abs(real - pred)))}

        // comparison.cache() // just that spark does not compute it twice, for the map and the count
        // val rmsee = math.sqrt(comparison.map{case (k, (r, p, e)) => (e * e)}.sum() / comparison.count())
        // val avgerrr = comparison.map{case (k, (r, p, e)) => e}.sum() / comparison.count()
        // println("\n\n\n\n\n\n\n\n\n\n\n\n")
        // println("RMSE", rmsee)
        // println("AVG ERR", avgerrr)
        // println("\n\n\n\n\n\n\n\n\n\n\n\n")

    }
}