import mysql.connector
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Establish MySQL database connection
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # Replace with your database password
    database="discovering-nepal"
)
cursor = db_connection.cursor()

# Step 1: Fetch text features for all places from the database


def fetch_text_features_from_db():
    query = "SELECT Places, Description, Highlights, Keyword FROM final_list"
    cursor.execute(query)
    data = cursor.fetchall()
    return data


places_text_features = fetch_text_features_from_db()

# Check if the 'place_similarity' table exists and is not empty


def check_place_similarity_table():
    cursor.execute("SHOW TABLES LIKE 'place_similarity'")
    result = cursor.fetchone()

    if result:
        cursor.execute("SELECT COUNT(*) FROM place_similarity")
        count = cursor.fetchone()[0]
        if count > 0:
            return True

    return False

# Step 2: Calculate and store similarity between each pair of places


def calculate_and_store_similarity():
    places = [place_data[0] for place_data in places_text_features]
    text_features = [
        f"{data[1]} {data[2]} {data[3]}" for data in places_text_features]

    # Create TF-IDF vectorizer
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix = tfidf_vectorizer.fit_transform(text_features)

    # Calculate cosine similarity between each pair of places
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    # Clean column names by removing spaces and other special characters
    cleaned_places = [re.sub(r'\W+', '_', place) for place in places]

    # Check if 'place_similarity' table already contains data
    if not check_place_similarity_table():
        # Create a table to store the similarity matrix if it does not exist
        create_table_query = "CREATE TABLE place_similarity (Place VARCHAR(255)"

        for place in cleaned_places:
            create_table_query += f", `{place}` FLOAT"

        create_table_query += ")"
        cursor.execute(create_table_query)

        # Store the calculated similarity in the 'place_similarity' table
        for i, place1 in enumerate(places):
            similarity_values = [cosine_sim[i, j] for j in range(len(places))]
            # +1 for the 'Place' column
            placeholders = ", ".join(["%s"] * (len(places) + 1))
            insert_query = f"INSERT INTO place_similarity (Place, {', '.join([f'`{place}`' for place in cleaned_places])}) VALUES ({placeholders})"
            cursor.execute(insert_query, [place1] + similarity_values)

        # Commit changes to the database
        db_connection.commit()


# Calculate and store similarity matrix if needed
calculate_and_store_similarity()


# Step 3: Fetch information from 'place_similarity' table and store the top 5 most similar places for each place
def create_and_store_most_similar_places():
    cursor.execute("SHOW TABLES LIKE 'most_similar_places'")
    result = cursor.fetchone()

    if not result:
        create_table_query = "CREATE TABLE most_similar_places (Place VARCHAR(255)"

        # Add columns to store the top 5 similar places for each place
        for i in range(1, 6):
            create_table_query += f", `similar_place_{i}` VARCHAR(255)"

        create_table_query += ")"
        cursor.execute(create_table_query)

        # Fetch all the places from the 'place_similarity' table
        cursor.execute("SELECT Place FROM place_similarity")
        places = [place[0] for place in cursor.fetchall()]

        # Iterate through each place to find its top 5 similar places
        for place in places:
            cursor.execute(
                f"SELECT * FROM most_similar_places WHERE Place = '{place}'")
            existing_data = cursor.fetchone()

            if not existing_data:
                cursor.execute(
                    f"SELECT * FROM place_similarity WHERE Place = '{place}'")
                row = cursor.fetchone()

                # Get the similarity values for the current place
                similarity_values = row[1:]

                # Replace None similarity values with 0 for sorting
                similarity_values = [
                    0 if similarity is None else similarity for similarity in similarity_values]

                # Sort the similarity values in descending order and exclude the similarity to itself
                sorted_similarities = sorted(
                    enumerate(similarity_values), key=lambda x: x[1], reverse=True)[1:6]

                # Get the top 5 similar places and their similarity values (if they exist)
                top_similar_places = [places[index]
                                      for index, _ in sorted_similarities]

                # Create a placeholder string for the insert query
                # 6 placeholders for the 'Place' and the top 5 similar places
                placeholders = ", ".join(["%s"] * 6)

                # Create the insert query and execute it
                insert_query = f"INSERT INTO most_similar_places (Place, similar_place_1, similar_place_2, similar_place_3, similar_place_4, similar_place_5) VALUES ({placeholders})"
                cursor.execute(insert_query, [place] + top_similar_places)

        # Commit changes to the database
        db_connection.commit()


# Create and store the top 5 most similar places for each place
create_and_store_most_similar_places()

# Close the cursor and database connection
cursor.close()
db_connection.close()
