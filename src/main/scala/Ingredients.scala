import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Ingredients{
    def main(args: Array[String]): Unit ={
        if(args.length != 1){
            println(">>> [ERROR] Expected arguments: Input and Output")
        }

        val conf = new SparkConf().setAppName("Ingredients").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.appName("CSV Processing").getOrCreate()

        // Load the CSV file into a DataFrame
        val df = spark.read.option("header", "true").csv(args(0))

        val ingredientRDD = df.select("ingredient_ids").rdd

        // Extract unique ingredient IDs
        val uniqueIngredients = ingredientRDD.flatMap(row => {
        val ingredients = row.getString(0)
        ingredients.substring(1, ingredients.length - 1).split(", ")
        }).distinct().collect()

        // Print the unique ingredients
        uniqueIngredients.foreach(println)

        println("\n\n\n\n\n\n\n\n\n\n\n\n")
        // Print the count of unique ingredients
        println(s"Total unique ingredients: ${uniqueIngredients.length}")




        println("\n\n\n\n\n\n\n\n\n\n\n\n")
    
    }
}