# 🥗 Food Recommendation System (Big Data Praktikum - JGU Mainz)

This project implements a scalable, item-based collaborative filtering recommendation system using Apache Spark. It is designed to process large-scale user-recipe interaction data and generate personalized recipe recommendations.

## 🗂️ Dataset

This project uses the [Food.com Recipes and User Interactions dataset](https://www.kaggle.com/datasets/shuyangli94/food-com-recipes-and-user-interactions/data?select=PP_recipes.csv) from Kaggle.  
It contains millions of user-recipe interactions, ratings, and recipe metadata, making it suitable for large-scale recommendation system experiments.

## ✨ Features

- **Item-based Collaborative Filtering:** Recommends recipes based on similarities between items (recipes) rather than users.
- **Cosine Similarity:** Measures similarity between recipes using centered user ratings.
- **Rating Centralization:** Normalizes ratings by subtracting the mean rating for each recipe.
- **Top-K Prediction:** Predicts ratings for unrated recipes using the most similar items a user has already rated.
- **Distributed Processing:** Utilizes Spark RDDs for efficient, scalable computation.

## ⚙️ How the Code Works

This project implements an **item-based collaborative filtering** recommendation system using Apache Spark, designed for scalability and speed on large datasets.

### Main Steps and Algorithms

1. **Data Loading & Preprocessing**
   - Reads user-recipe-rating triples from a CSV file.
   - Filters and parses the data into `(userId, itemId, rating)` tuples.

2. **Rating Centralization**
   - The `center` method normalizes ratings for each recipe by subtracting the mean rating for that recipe.
   - This step removes popularity bias and improves similarity calculations.

3. **Cosine Similarity Calculation**
   - The `cosineSimilarityPerRow` method computes the cosine similarity between all pairs of recipes based on the centered ratings.
   - For each pair, it finds users who rated both, computes the dot product of their ratings, and divides by the product of their norms.

4. **Top-K Prediction**
   - The `predictTopK` method predicts ratings for recipes the user hasn’t rated.
   - For each candidate recipe, it finds the most similar recipes the user has rated, and predicts a rating using a weighted sum (by similarity).
   - Only the top-K most similar items are used for each prediction.

5. **Output**
   - The system outputs the top-K recommended recipes for the user, with predicted ratings, to the specified directory.

6. **(Optional) RMSE Calculation**
   - The code includes (commented) logic for evaluating prediction accuracy using Root Mean Squared Error.

### Combined Methods

- **Collaborative Filtering:** Recommends items based on item-to-item similarity.
- **Cosine Similarity:** Quantifies similarity between recipes using user ratings.
- **Rating Centralization:** Normalizes for item popularity.
- **Top-K Neighbors:** Uses only the most similar items for prediction.
- **Distributed Processing:** All steps use Spark RDDs for parallel, scalable computation.

### Performance

Because all major computations (grouping, joining, aggregating) are performed using Spark’s distributed RDDs, the system can efficiently process very large datasets—potentially millions of users and recipes—across multiple CPU cores or cluster nodes. On a modern multi-core machine or Spark cluster, even datasets with millions of interactions can be processed in minutes, depending on hardware and cluster size.

---

This combination of methods ensures both accuracy and scalability, making the system suitable for real-world, large-scale recommendation tasks.

## 📁 Project Structure

- `src/main/scala/FoodRecommendation.scala`  
  Main logic for data processing, similarity computation, and prediction.
- `datasets/`  
  Contains input data files such as `RAW_interactions.csv`, user and recipe metadata, and test splits.
- `old_files/`  
  Contains old output folders from previous runs (not used by the main program).

## 📖 Usage

Build the project using `sbt`:

```sh
sbt package
