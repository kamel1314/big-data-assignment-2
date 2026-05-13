import pandas as pd

# Read the original JSON file
df = pd.read_json("mds.json")

columns = [
    "name",
    "released_at",
    "genre",
    "streaming_on",
    "country",
    "type",
    "content_rating",
    "imdb_rating",
    "number_of_seasons",
    "cast_and_crew"
]

df = df[columns]

# Save as CSV with a header row
# Empty/null values are not removed here because cleaning is done in MapReduce
df.to_csv("mds.csv", index=False)

print("Conversion completed successfully.")
print("Rows:", len(df))
print("Columns:", len(df.columns))