

import pandas as pd
import numpy as np
import mysql.connector
import warnings

# Suppress the RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Establish MySQL database connection
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="discovering-nepal"
)
cursor = db_connection.cursor()

# Function to fetch user-place ratings from the database and create the user-place matrix


def create_user_place_matrix_from_db():
    query = "SELECT `Names`, `Places`, `Ratings` FROM user_place_rating"
    cursor.execute(query)
    data = cursor.fetchall()

    user_place_matrix = pd.DataFrame(
        data, columns=['Names', 'Places', 'Ratings'])
    user_place_matrix = user_place_matrix.pivot(
        index='Names', columns='Places', values='Ratings')

    # Clean the matrix to contain only numeric values (replace non-numeric values with 0)
    user_place_matrix = user_place_matrix.apply(
        pd.to_numeric, errors='coerce').fillna(0)

    return user_place_matrix


# Create user-place matrix from the database
user_place_matrix = create_user_place_matrix_from_db()

# Function to get similar users based on Jaccard similarity


def get_similar_users(user, user_item_matrix, num_users=25):  # Reduce num_users to 10
    user_visited_places = user_item_matrix.loc[user]
    similar_users = user_item_matrix.drop(user).apply(lambda row: user_visited_places.dot(row) / (user_visited_places.sum() + row.sum(
    ) - user_visited_places.dot(row)) if (user_visited_places.sum() + row.sum() - user_visited_places.dot(row)) != 0 else 1e-6, axis=1)
    similar_users = similar_users.sort_values(ascending=False)[:num_users]
    return similar_users

# Function to get place recommendations for a user using user-based collaborative filtering


# Reduce num_recommendations to 3
def user_based_collaborative_filtering(user, user_item_matrix, min_rating=3, num_recommendations=5, num_similar_users=25):
    similar_users = get_similar_users(
        user, user_item_matrix, num_similar_users)
    recommendations = user_item_matrix.loc[similar_users.index].mean()
    visited_places = user_item_matrix.loc[user][user_item_matrix.loc[user] > 0]
    recommendations = recommendations[recommendations >=
                                      min_rating].index.difference(visited_places.index)
    return recommendations[:num_recommendations]


# Function to store recommendations in the database for a given user
def store_recommendations_in_db(user_name, recommendations, num_recommendations=5):
    insert_query = f"INSERT INTO user_recommendations (user_name, recommendation_1, recommendation_2, recommendation_3, recommendation_4, recommendation_5) VALUES ('{user_name}', {'%s, ' * (num_recommendations - 1)}%s)"
    cursor.execute(insert_query, tuple(recommendations))
    db_connection.commit()

# Function to get collaborative recommendations from the database or calculate them if not available


def get_collaborative_recommendations_from_db(user_name, num_recommendations=5):
    query = f"SELECT * FROM user_recommendations WHERE user_name = '{user_name}'"
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        recommendations = list(result[1:])
    else:
        recommendations = user_based_collaborative_filtering(
            user_name, user_place_matrix, num_recommendations=num_recommendations)
        if recommendations.empty:
            # If there are no recommendations, return an empty list
            return []

        # Fill in remaining recommendations with empty strings
        recommendations = list(recommendations) + \
            [''] * (num_recommendations - len(recommendations))

        # Store recommendations in the database
        store_recommendations_in_db(
            user_name, recommendations, num_recommendations=num_recommendations)

    return recommendations


# Loop through all users and calculate/store collaborative recommendations
for user in user_place_matrix.index:
    get_collaborative_recommendations_from_db(user)

print("Collaborative recommendations for all users have been calculated and stored in the database.")
