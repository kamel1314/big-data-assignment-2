# CSCI461 Assignment #2 — MongoDB Commands (Person 1)
**Nile University | Spring 2026**
 
---
 
## Step 1 — Start the MongoDB Container
```bash
docker start mongo
```
 
---
 
## Step 2 — Open mongosh (MongoDB Shell)
```bash
docker exec -it mongo mongosh
```
 
---
 
## Step 3 — Exit mongosh to run next commands in PowerShell
```bash
exit
```
 
---
 
## Step 4 — Copy the Dataset into the Container
```bash
docker cp "D:\UNI\Third_second\CSCI461 Introduction to Big Data\Assignment #2\mds.json" mongo:/tmp/mds.json
```
> mds.json is on your Windows D: drive. The container cannot access it directly,
> so we copy it into the container's /tmp folder first.
 
---
 
## Step 5 — Import the Data into MongoDB
```bash
docker exec -it mongo mongoimport --jsonArray --db moviesDB --collection moviesColl --file /tmp/mds.json
```
> This command automatically:
> - Creates the database moviesDB
> - Creates the collection moviesColl
> - Imports all 1401 documents
 
---
 
## Step 6 — Open mongosh Again to Verify
```bash
docker exec -it mongo mongosh
```
 
---
 
## Step 7 — Verify the Import
```bash
use moviesDB
db.moviesColl.countDocuments()
show collections
```
> Expected output:
> - countDocuments() → 1401
> - show collections → moviesColl
 
---
 
## Query 1 — Index on name + Search by Name
```javascript
db.moviesColl.createIndex({ name: 1 })
db.moviesColl.findOne({ name: "Ice Airport Alaska" })
```
> Creates an index on the name field for fast lookups,
> then searches for a specific title by exact name.
 
---
 
## Query 2 — Movies with Drama or Documentary Genre
```javascript
db.moviesColl.find({
  type: "Movie",
  $or: [
    { genre: { $regex: "Drama", $options: "i" } },
    { genre: { $regex: "Documentary", $options: "i" } }
  ]
}).limit(3)
```
> Finds all Movies where genre contains Drama or Documentary.
> Uses $or and $regex for flexible string matching.
> .limit(3) shows first 3 results out of many.
 
---
 
## Query 3 — Count Titles per Type
```javascript
db.moviesColl.aggregate([
  { $group: { _id: "$type", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```
> Counts how many titles exist per type (Movie vs TV Show),
> sorted from highest to lowest.
> Result: Movie 1232, TV Show 169
 
---
 
## Query 4 — Top 5 Highest-Rated Movies
```javascript
db.moviesColl.aggregate([
  { $match: {
      type: "Movie",
      imdb_rating: { $exists: true, $nin: ["", null] },
      released_at: { $gt: "2010-12-31" },
      $or: [
        { streaming_on: { $regex: "Prime Video", $options: "i" } },
        { streaming_on: { $regex: "Netflix", $options: "i" } }
      ]
  }},
  { $addFields: {
      rating_numeric: {
        $toDouble: { $arrayElemAt: [{ $split: ["$imdb_rating", "/"] }, 0] }
      }
  }},
  { $sort: { rating_numeric: -1 } },
  { $limit: 5 },
  { $project: {
      _id: 0, name: 1, released_at: 1, genre: 1,
      streaming_on: 1, country: 1, imdb_rating: 1
  }}
])
```
> Finds top 5 highest-rated Movies after 2010 on Prime Video or Netflix.
> Uses $addFields + $toDouble to convert "7.8/10" string to a number for sorting.
 
---
 
## Query 5 (Bonus) — Advanced $facet Query
```javascript
db.moviesColl.aggregate([
  { $addFields: {
      rating_numeric: {
        $cond: {
          if: { $and: [{ $gt: ["$imdb_rating", null] }, { $ne: ["$imdb_rating", ""] }] },
          then: { $toDouble: { $arrayElemAt: [{ $split: ["$imdb_rating", "/"] }, 0] } },
          else: null
        }
      },
      genre_array: {
        $map: {
          input: { $split: ["$genre", ","] },
          as: "g",
          in: { $trim: { input: "$$g" } }
        }
      }
  }},
  { $facet: {
      "by_genre": [
        { $unwind: "$genre_array" },
        { $group: { _id: "$genre_array", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 5 },
        { $project: { _id: 0, genre: "$_id", count: 1 } }
      ],
      "rating_stats": [
        { $match: { rating_numeric: { $ne: null } } },
        { $group: {
            _id: null,
            avg_rating: { $avg: "$rating_numeric" },
            max_rating: { $max: "$rating_numeric" },
            min_rating: { $min: "$rating_numeric" },
            total_rated: { $sum: 1 }
        }},
        { $project: {
            _id: 0,
            avg_rating: { $round: ["$avg_rating", 2] },
            max_rating: 1, min_rating: 1, total_rated: 1
        }}
      ],
      "by_content_rating": [
        { $match: { content_rating: { $exists: true, $nin: ["", null] } } },
        { $group: { _id: "$content_rating", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $project: { _id: 0, content_rating: "$_id", count: 1 } }
      ]
  }}
])
```
> Advanced bonus query using $facet to run 3 aggregation pipelines in a single pass:
> - by_genre: top 5 genres by title count
> - rating_stats: average, min, max IMDb rating
> - by_content_rating: count per content rating
 
---