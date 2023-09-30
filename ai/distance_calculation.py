import mysql.connector
import numpy as np
from geopy.distance import geodesic
import re

# Establish MySQL database connection
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # Replace with your database password
    database="discovering-nepal"
)
cursor = db_connection.cursor()

# Step 1: Fetch latitude and longitude for all places from the database


def fetch_coordinates_from_db():
    query = "SELECT Places, Latitude, Longitude FROM final_list"
    cursor.execute(query)
    data = cursor.fetchall()
    return data


places_coordinates = fetch_coordinates_from_db()

# Check if the 'distance_matrix' table exists and is not empty


def check_distance_matrix_table():
    cursor.execute("SHOW TABLES LIKE 'distance_matrix'")
    result = cursor.fetchone()

    if result:
        cursor.execute("SELECT COUNT(*) FROM distance_matrix")
        count = cursor.fetchone()[0]
        if count > 0:
            return True

    return False

# Check if the 'nearby_places' table exists and is not empty


def check_nearby_places_table():
    cursor.execute("SHOW TABLES LIKE 'nearby_places'")
    result = cursor.fetchone()

    if result:
        cursor.execute("SELECT COUNT(*) FROM nearby_places")
        count = cursor.fetchone()[0]
        if count > 0:
            return True

    return False

# Step 2: Calculate and store distance matrix between each pair of places


def calculate_and_store_distance_matrix():
    places = [place_data[0] for place_data in places_coordinates]
    coordinates = [(place_data[1], place_data[2])
                   for place_data in places_coordinates]

    # Calculate distance matrix between each pair of places
    distance_matrix = np.zeros((len(places), len(places)))
    for i, (lat1, lon1) in enumerate(coordinates):
        for j, (lat2, lon2) in enumerate(coordinates):
            distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
            distance_matrix[i, j] = distance

    # Clean column names by removing spaces and other special characters
    cleaned_places = [re.sub(r'\W+', '_', place) for place in places]

    # Check if 'distance_matrix' table already contains data
    if not check_distance_matrix_table():
        # Create a table to store the distance matrix if it does not exist
        create_table_query = "CREATE TABLE distance_matrix (Place VARCHAR(255)"

        for place in cleaned_places:
            create_table_query += f", `{place}` FLOAT"

        create_table_query += ")"
        cursor.execute(create_table_query)

        # Store the calculated distance matrix in the 'distance_matrix' table
        for i, place1 in enumerate(places):
            # Convert numpy array to list
            distance_values = distance_matrix[i].tolist()
            # +1 for the 'Place' column
            placeholders = ", ".join(["%s"] * (len(places) + 1))
            insert_query = f"INSERT INTO distance_matrix (Place, {', '.join([f'`{place}`' for place in cleaned_places])}) VALUES ({placeholders})"
            cursor.execute(insert_query, [place1] + distance_values)

        # Commit changes to the database
        db_connection.commit()

# Step 3: Sort the places based on distance matrix and store nearby places for each place


def create_and_store_nearby_places():
    # Check if 'nearby_places' table exists and is not empty
    if not check_nearby_places_table():
        # Create a table to store nearby places if it does not exist
        create_table_query = "CREATE TABLE nearby_places (Place VARCHAR(255)"

        # Add columns to store the nearby places
        for i in range(1, 4):  # 3 nearby places for each place
            create_table_query += f", `nearby_place_{i}` VARCHAR(255)"

        create_table_query += ")"
        cursor.execute(create_table_query)

        # Fetch all the places from the 'distance_matrix' table
        cursor.execute("SELECT Place FROM distance_matrix")
        places = [place[0] for place in cursor.fetchall()]

        # Iterate through each place to find its nearby places
        for place in places:
            cursor.execute(
                f"SELECT * FROM nearby_places WHERE Place = '{place}'")
            existing_data = cursor.fetchone()

            if not existing_data:
                cursor.execute(
                    f"SELECT * FROM distance_matrix WHERE Place = '{place}'")
                row = cursor.fetchone()

                # Get the distance values for the current place
                distance_values = row[1:]

                # Sort the distance values in ascending order to get nearby places
                sorted_distances = sorted(
                    enumerate(distance_values), key=lambda x: x[1])[1:4]

                # Get the nearby places
                nearby_places = [places[index]
                                 for index, _ in sorted_distances]

                # Create a placeholder string for the insert query
                # 4 placeholders for the 'Place' and the nearby places
                placeholders = ", ".join(["%s"] * 4)

                # Create the insert query and execute it
                insert_query = f"INSERT INTO nearby_places (Place, nearby_place_1, nearby_place_2, nearby_place_3) VALUES ({placeholders})"
                cursor.execute(insert_query, [place] + nearby_places)

        # Commit changes to the database
        db_connection.commit()


# Calculate and store distance matrix if needed
calculate_and_store_distance_matrix()

# Create and store nearby places for each place if needed
create_and_store_nearby_places()

# Close the cursor and database connection
cursor.close()
db_connection.close()
