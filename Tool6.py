from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)
CORS(app)

# Database connection details
host = "dpg-cobrpren7f5s73ftpqrg-a.oregon-postgres.render.com"
database = "sheshank_sonji"
user = "sheshank_sonji_user"
password = "Lo2Ze5zVZSRPGxDLCg5WAKUXUfxo7rrZ"

# Establish a connection to the PostgreSQL database
engine = create_engine(f"postgresql://{user}:{password}@{host}/{database}")

@app.route('/crime_ranking/<int:year>/<string:district>', methods=['GET'])
def crime_ranking(year, district):
    # Load the dataset from PostgreSQL using a SQL query
    query = f"""
        SELECT "year", "district_name", "beat", COUNT(*) as "Number of crimes"
        FROM "tool6"
        WHERE "year" = {year} AND "district_name" = '{district}'
        GROUP BY "year", "district_name", "beat"
    """
    df_analyse = pd.read_sql_query(query, engine)

    # Check if any data was found
    if df_analyse.empty:
        return jsonify({"error": "No data found for the selected year and district."}), 404

    # Rank the beats based on the number of crimes reported
    ranked_beats = df_analyse.sort_values(by='Number of crimes', ascending=False)

    # Create the response list
    ranking_list = []
    for rank, (beat, num_crimes) in enumerate(zip(ranked_beats['beat'], ranked_beats['Number of crimes']), start=1):
        ranking_list.append({
            "rank": rank,
            "beat": beat,
            "num_crimes": num_crimes
        })

    # Return the ranking list as JSON
    return jsonify(ranking_list)

