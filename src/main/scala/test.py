import pandas as pd

# Defining the data based on the image provided
data = {
    "userId": [],
    "recipeId": [],
    "date": [],
    "rating": []
}

date_value = "2023-08-20"  # Specifying a common date

# Extracting data from the table
ratings_matrix = [
    [4, 5, 3, None, 5, 1, 1, 3, 2],
    [None, 3, 2, 3, 1, 2, 1, None],
    [2, None, 1, 3, None, 4, 5, 3],
    [5, 4, 1, None, 2, None, 3, None]
]

# Populating the dictionary
for user_id, row in enumerate(ratings_matrix, start=1):
    for recipe_id, rating in enumerate(row, start=1):
        data["userId"].append(user_id)
        data["recipeId"].append(recipe_id)
        data["date"].append(date_value)
        data["rating"].append(rating)

# Creating a DataFrame
df = pd.DataFrame(data)

# Saving to a CSV file
file_path = "ratings_data.csv"
df.to_csv(file_path, index=False)

file_path
