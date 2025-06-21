import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.sqrt

object FoodRecommendation{
    def main(args: Array[String]): Unit ={
        if(args.length != 2){
            println(">>> [ERROR] Expected arguments: Input and Output")
        }

        val conf = new SparkConf().setAppName("FoodRecommendation").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val user = 5
        val N = 2

        def cosineDistance(row1: (Array[Int], Array[Double]), row2: (Array[Int], Array[Double])): Double = {
            val (indices1, values1) = row1
            val (indices2, values2) = row2

            val map1 = indices1.zip(values1).toMap
            val map2 = indices2.zip(values2).toMap

            val dotProduct = indices1.intersect(indices2).map(i => map1(i) * map2(i)).sum

            val norm1 = math.sqrt(values1.map(v => v * v).sum)
            val norm2 = math.sqrt(values2.map(v => v * v).sum)

            val cosineSimilarity = if (norm1 != 0.0 && norm2 != 0.0) {
                dotProduct / (norm1 * norm2)
            } else {
                0.0 // Handle the case where one of the vectors is zero
            }
            1.0 - cosineSimilarity
        }

        val interactions = sc.textFile(args(0))
        .filter(line => !line.startsWith("user_id")) // Skips the header if it exists
        .map(line => line.split(","))
        .filter(parts => parts.length > 3 && parts(0).forall(_.isDigit) && parts(1).forall(_.isDigit) && parts(3).matches("""\d+(\.\d+)?"""))
        .map(parts => (parts(0).toInt, parts(1).toInt, parts(3).toDouble)) // (userId, itemId, rating)

        // Step 2: Center the data by user
        val itemRatings = interactions.map { case (userId, itemId, rating) => (itemId, (userId, rating)) }

        // Step 1: Calculate the total and count for each row (userId)
        val rowSumsAndCounts = itemRatings
        .map { case (userId, (itemId, rating)) => (userId, rating) }
        .aggregateByKey((0.0, 0))(
            { case ((sum, count), rating) => (sum + rating, count + 1) },
            { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
        )

        // Step 2: Compute the mean for each row (userId)
        val rowMeans = rowSumsAndCounts.mapValues { case (sum, count) => sum / count }

        // Step 3: Subtract the mean from each element in the row
        val centeredRatings = itemRatings
        .join(rowMeans)
        .map { case (userId, ((itemId, rating), mean)) => (userId, itemId, rating - mean) }


        // // Step 4: Prepare (item, (user, rating)) tuples for item-item CF
        // val itemUserRating = centeredRatings.map { case (userId, itemId, rating) =>
        // (itemId, (userId, rating))
        // }.groupByKey()

        // // Step 3: Calculate cosine similarity between items
        // val itemPairSimilarities = itemUserRating.cartesian(itemUserRating)
        // .filter { case ((item1, _), (item2, _)) => item1 < item2 }
        // .map { case ((item1, ratings1), (item2, ratings2)) =>
        //     val ratingsMap1 = ratings1.toMap
        //     val ratingsMap2 = ratings2.toMap
        //     val commonUsers = ratingsMap1.keySet.intersect(ratingsMap2.keySet)

        //     if (commonUsers.isEmpty) {
        //     ((item1, item2), 0.0)
        //     } else {
        //     val dotProduct = commonUsers.map(user => ratingsMap1(user) * ratingsMap2(user)).sum
        //     val norm1 = sqrt(ratingsMap1.values.map(r => r * r).sum)
        //     val norm2 = sqrt(ratingsMap2.values.map(r => r * r).sum)
        //     val cosineSimilarity = dotProduct / (norm1 * norm2)
        //     ((item1, item2), cosineSimilarity)
        //     }
        // }
        // .flatMap { case ((item1, item2), similarity) =>
        //     Seq((item1, (item2, similarity)), (item2, (item1, similarity)))
        // }

        // // Print the similarities for itemId 1 with all other items
        // val itemIdToCheck = 1
        // println("\n\n\n\n\n\n\n\n\n\n")
        // val similaritiesForItem1 = itemPairSimilarities.filter { case (item, _) => item == itemIdToCheck }
        // similaritiesForItem1.collect().foreach { case (item, (otherItem, similarity)) =>
        //     println(s"Similarity between item $itemIdToCheck and item $otherItem: $similarity")
        // }
        // println("\n\n\n\n\n\n\n\n\n\n")

        // // Step 4: Predict missing ratings for user 38094
        // val userRatings = interactions.filter(_._1 == user).map(x => (x._2, x._3)).collectAsMap()
        // val itemsNotRatedByUser = itemUserRating.keys.filter(!userRatings.contains(_)).collect()

        // val predictedRatings = itemsNotRatedByUser.map { itemId =>
        //     val similarItems = itemPairSimilarities.filter { case (item, _) => item == itemId }
        //         .sortBy(-_._2._2) // sort by similarity in descending order
        //         .take(N) // take top N similar items

        //     val numerator = similarItems.map {
        //         case (_, (otherItem, similarity)) =>
        //         similarity * userRatings.getOrElse(otherItem, 0.0)
        //     }.sum

        //     val denominator = similarItems.map(_._2._2).sum

        //     val predictedRating = if (denominator != 0) numerator / denominator else 0.0
        //     (itemId, predictedRating)
        // }


        // // Output the predicted ratings for user 38094
        // sc.parallelize(predictedRatings.toSeq).saveAsTextFile(args(1))

        // centeredRatings.map { case (userId, itemId, centeredRating) => s"$userId\t$itemId\t$centeredRating"}.saveAsTextFile(args(1))
        sc.stop()
    }
}