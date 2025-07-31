from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import json
import os
from pyspark.sql.functions import col, split, size, explode, array_sort, concat_ws, count, avg, collect_list, lit, asc
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("danceability", FloatType(), True),
    StructField("energy", FloatType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", FloatType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", FloatType(), True),
    StructField("acousticness", FloatType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("liveness", FloatType(), True),
    StructField("valence", FloatType(), True),
    StructField("tempo", FloatType(), True),
    StructField("time_signature", IntegerType(), True),
    StructField("track_genre", StringType(), True) 
])



def analiziraj_kolone(df, numericke_kolone, string_kolone, boolean_kolone, izlazni_folder):
    os.makedirs(izlazni_folder, exist_ok=True)

    for col in numericke_kolone:
        # Osnovna statistika
        opis_df = df.select(col).describe()
        opis = {row["summary"]: row[col] for row in opis_df.collect()}

        # Percentili
        percentili = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)
        q1, median, q3 = percentili
        iqr = q3 - q1

        null_count = df.filter(df[col].isNull()).count()

        rezultat = {
            "tip": "Numerička",
            "broj_null_vrijednosti": null_count,
            "osnovna_statistika": {
                "count": opis.get("count"),
                "mean": opis.get("mean"),
                "stddev": opis.get("stddev"),
                "min": opis.get("min"),
                "max": opis.get("max")
            },
            "percentili": {
                "Q1": q1,
                "median": median,
                "Q3": q3,
                "IQR": iqr
            }
        }

        with open(f"{izlazni_folder}/{col}.json", "w", encoding="utf-8") as f:
            json.dump(rezultat, f, indent=4, ensure_ascii=False)
            
# PERCENTIL - označava vrijednost ispod koje se nalazi određeni procenat podataka
# MEAN (Srednja vrijednost) - Suma svih vrijednosti podijeljena s brojem vrijednosti
# MEDIAN - Srednja vrijednost skupa podataka kada su sortirani
# Q1 - Prvi kvartil (25. percentil): 25% podataka je manjih ili jednakih od ove vrijednosti
# Q3 - Treći kvartil (75. percentil): 75% podataka je manjih ili jednakih od ove vrijednosti
# IQR - Interkvartilni raspon = Q3 - Q1. Mjera širine "srednjih" 50% vrijednosti


    for col in string_kolone:
        distinct_count = df.select(col).distinct().count()
        null_count = df.filter(df[col].isNull()).count()

        najcesce_rows = df.groupBy(col).count().orderBy("count", ascending=False).limit(5).collect()
        najcesce = [{"vrijednost": row[col], "count": row["count"]} for row in najcesce_rows]

        rezultat = {
            "tip": "String",
            "broj_null_vrijednosti": null_count,
            "broj_razlicitih_vrijednosti": distinct_count,
            "top_5_frekventnih": najcesce
        }

        with open(f"{izlazni_folder}/{col}.json", "w", encoding="utf-8") as f:
            json.dump(rezultat, f, indent=4, ensure_ascii=False)

    for col in boolean_kolone:
        null_count = df.filter(df[col].isNull()).count()

        frekvencije_rows = df.groupBy(col).count().orderBy("count", ascending=False).collect()
        frekvencije = [{"vrijednost": row[col], "count": row["count"]} for row in frekvencije_rows]

        rezultat = {
            "tip": "Boolean",
            "broj_null_vrijednosti": null_count,
            "frekvencije": frekvencije
        }

        with open(f"{izlazni_folder}/{col}.json", "w", encoding="utf-8") as f:
            json.dump(rezultat, f, indent=4, ensure_ascii=False)

            
def prvi_zadatak(df: DataFrame, k: int = 10, izlazni_folder: str = None):
 
    df_collab = df.withColumn("artist_array", F.split(F.col("artists"), ";")).filter(F.size("artist_array") > 1)

    df_collab = df_collab.withColumn("artist_key", F.array_sort("artist_array")).withColumn("artist_key", F.concat_ws(";", "artist_key"))
                         
    collab_stats = df_collab.groupBy("artist_key") \
        .agg(
            F.count("*").alias("count"),
            F.avg("popularity").alias("avg_collab_popularity")
        )

    df_solo = df.withColumn("artist_array", F.split(F.col("artists"), ";")) \
                .filter(F.size("artist_array") == 1) \
                .withColumn("artist", F.col("artist_array")[0]) \
                .groupBy("artist") \
                .agg(F.avg("popularity").alias("avg_solo_popularity"))

    df_collab_expanded = df_collab.select("artist_key", "artist_array") \
        .withColumn("artist", F.explode("artist_array")) \
        .join(df_solo, on="artist", how="left") \
        .groupBy("artist_key", "artist") \
        .agg(F.first("avg_solo_popularity").alias("avg_solo_popularity"))

    least_popular_artist_df = df_collab_expanded.groupBy("artist_key") \
        .agg(F.collect_list(F.struct("avg_solo_popularity", "artist")).alias("artist_list")) \
        .withColumn("filtered_artists", F.expr("filter(artist_list, x -> x.avg_solo_popularity is not null)")) \
        .withColumn("sorted_artists", F.sort_array("filtered_artists")) \
        .withColumn("least_popular_artist", F.col("sorted_artists")[0].getField("artist")) \
        .withColumn("avg_solo_popularity", F.col("sorted_artists")[0].getField("avg_solo_popularity")) \
        .drop("artist_list", "filtered_artists", "sorted_artists")

    final_df = collab_stats \
        .join(least_popular_artist_df, on="artist_key") \
        .withColumn("popularity_diff", F.col("avg_collab_popularity") - F.col("avg_solo_popularity")) \
        .withColumnRenamed("artist_key", "artists") \
        .select(
            "artists",
            "least_popular_artist",
            "count",
            "avg_collab_popularity",
            "avg_solo_popularity",
            "popularity_diff"
        )
        
    final_df = final_df.filter(F.col("least_popular_artist").isNotNull())

    final_df = final_df.orderBy(F.col("count").desc()).limit(k)

    if izlazni_folder:
        records = [row.asDict() for row in final_df.collect()]
        with open(f"{izlazni_folder}/prvi_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)


def drugi_zadatak(df: DataFrame, izlazni_folder: str = None):
    album_avg_pop = df.groupBy("album_name") \
        .agg(F.avg("popularity").alias("avg_album_popularity"))

    df_with_album_pop = df.join(album_avg_pop, on="album_name")

    breakthrough_tracks = df_with_album_pop \
        .filter((F.col("popularity") > 80) & (F.col("avg_album_popularity") < 50)) \
        .select("album_name", "track_id", "track_name", "popularity", 
                "avg_album_popularity", "energy", "danceability", "valence")

    other_tracks = df_with_album_pop \
        .join(breakthrough_tracks.select("track_id").withColumnRenamed("track_id", "bt_id"), 
              df_with_album_pop.track_id == F.col("bt_id"), 
              how="left_anti")
        #zapravo right join - sve pjesme sem one koja je breakthrough

    other_avg_audio = other_tracks.groupBy("album_name") \
        .agg(
            F.avg("energy").alias("avg_album_energy"),
            F.avg("danceability").alias("avg_album_danceability"),
            F.avg("valence").alias("avg_album_valence")
        )

    final = breakthrough_tracks.join(other_avg_audio, on="album_name", how="left") \
        .withColumn("energy_diff", F.col("energy") - F.col("avg_album_energy")) \
        .withColumn("danceability_diff", F.col("danceability") - F.col("avg_album_danceability")) \
        .withColumn("valence_diff", F.col("valence") - F.col("avg_album_valence")).dropDuplicates(["track_id"])

    final_result = final.select(
        "album_name","track_name", "popularity", "avg_album_popularity",
        "energy", "avg_album_energy", "energy_diff",
        "danceability", "avg_album_danceability", "danceability_diff",
        "valence", "avg_album_valence", "valence_diff"
    )

    if izlazni_folder:
        records = [row.asDict() for row in final_result.collect()]
        with open(f"{izlazni_folder}/drugi_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
            
def treci_zadatak(df, izlazni_folder=None):
    top_genres = df.groupBy("track_genre") \
        .agg(F.avg("popularity").alias("avg_genre_popularity")) \
        .orderBy(F.desc("avg_genre_popularity")) \
        .limit(5)

    df_top = df.join(top_genres, on="track_genre", how="inner")

    df_top = df_top.withColumn(
        "tempo_range",
        F.when(F.col("tempo") < 100, "low")
         .when((F.col("tempo") >= 100) & (F.col("tempo") <= 120), "mid")
         .otherwise("high")
    )

    results = df_top.groupBy("track_genre", "tempo_range", "avg_genre_popularity") \
        .agg(
            F.count("*").alias("song_count"),
            F.avg("popularity").alias("avg_song_popularity")
        ) \
        .orderBy("track_genre", "tempo_range")

    if izlazni_folder:
        records = [row.asDict() for row in results.collect()]
        with open(f"{izlazni_folder}/treci_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
# [-1,1]
def klasifikuj_efekat(korelacija):
    if korelacija is None:
        return "neutral"
    elif korelacija <= -0.25:
        return "strong_negative"
    elif korelacija < -0.05:
        return "weak_negative"
    elif korelacija < 0.05:
        return "neutral"
    elif korelacija < 0.25:
        return "weak_positive"
    else:
        return "strong_positive"

def cetvrti_zadatak(df, izlazni_folder=None):

    stats = df.groupBy("track_genre", "explicit") \
        .agg(
            F.count("*").alias("count"),
            F.avg("popularity").alias("avg_popularity")
        )

    pivot = stats.groupBy("track_genre") \
        .pivot("explicit", [0, 1]) \
        .agg(
            F.first("count").alias("count"),
            F.first("avg_popularity").alias("avg_popularity")
        ) \
        .withColumnRenamed("0_count", "non_explicit_count") \
        .withColumnRenamed("1_count", "explicit_count") \
        .withColumnRenamed("0_avg_popularity", "non_explicit_avg") \
        .withColumnRenamed("1_avg_popularity", "explicit_avg")
        
    korelacije = df.groupBy("track_genre") \
        .agg(F.corr(F.col("explicit").cast("double"), F.col("popularity").cast("double")).alias("explicit_popularity_corr"))

    result = pivot.join(korelacije, on="track_genre", how="left") \
        .withColumn("popularity_diff", F.col("explicit_avg") - F.col("non_explicit_avg"))

    records = result.orderBy("track_genre").collect()
    data = []

    for row in records:
        row_dict = row.asDict()
        row_dict["explicit_effect"] = klasifikuj_efekat(row_dict["explicit_popularity_corr"])
        data.append(row_dict)

    if izlazni_folder:
        with open(f"{izlazni_folder}/cetvrti_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)


def peti_zadatak (df, izlazni_folder=None):
    plesive = df.filter(F.col("danceability") > 0.8)
    
    top_10_dugih = plesive.orderBy(F.desc("duration_ms")).limit(10)
    
    avg_pop_genre = df.groupBy("track_genre").agg(F.avg("popularity").alias("avg_genre_popularity"))
    
    poredjenje = top_10_dugih.join(avg_pop_genre, on="track_genre", how="left")

    poredjenje = poredjenje.withColumn("duration_min", F.round(F.col("duration_ms") / 60000, 3)) \
                       .withColumn("popularity_diff", F.col("popularity") - F.col("avg_genre_popularity")) \
                       .withColumn("is_more_popular_than_genre_avg", F.col("popularity_diff") > 0)

    rezultat = poredjenje.select(
        "track_name",
        "track_genre",
        "duration_min",
        F.round("danceability", 3).alias("danceability"),
        "popularity",
        F.round("avg_genre_popularity", 3).alias("avg_genre_popularity"),
        "popularity_diff",
        "is_more_popular_than_genre_avg"
)

    if izlazni_folder:
        records = [row.asDict() for row in rezultat.collect()]
        with open(f"{izlazni_folder}/peti_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
            
def sesti_zadatak (df, izlazni_folder=None):
    df = df.withColumn("popularity_bin", (F.col("popularity") / 10).cast("int") * 10)

    grouped = df.groupBy("popularity_bin", "explicit").agg(
        F.count("*").alias("count"),
        F.avg("valence").alias("avg_valence")
    )

    pivot = grouped.groupBy("popularity_bin").pivot("explicit", [False, True]).agg(
        F.first("count").alias("count"),
        F.first("avg_valence").alias("avg_valence")
    )

    result = pivot \
        .withColumnRenamed("False_count", "non_explicit_count") \
        .withColumnRenamed("True_count", "explicit_count") \
        .withColumnRenamed("False_avg_valence", "avg_valence_non_explicit") \
        .withColumnRenamed("True_avg_valence", "avg_valence_explicit")

    result = result \
        .withColumn("valence_diff",
                    F.col("avg_valence_explicit") - F.col("avg_valence_non_explicit"))

    records = [row.asDict() for row in result.orderBy("popularity_bin").collect()]

    if izlazni_folder:
        with open(f"{izlazni_folder}/sesti_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
            
def sedmi_zadatak(df, izlazni_folder=None):
    df_clean = df.filter((F.col("popularity").isNotNull()) & (F.col("popularity") > 0))

    grouped_artists = df_clean.groupBy("artists").agg(
        F.avg("popularity").alias("avg_popularity"),
        F.stddev("popularity").alias("stddev_popularity"),
        F.count("*").alias("song_count")
    ).filter(F.col("song_count") >= 3)  # Zbog adekvatne std devijacije
    
    #grouped_artists.show(10)

    top_artists = grouped_artists.filter(F.col("stddev_popularity") < 2.0) 
    
    joined = top_artists.join(df_clean, on="artists", how="inner")
    #joined.show()
    
    genres_per_artist = joined.groupBy("artists", "avg_popularity", "stddev_popularity", "song_count") \
        .agg(
            F.collect_set("track_genre").alias("genres")
        ) \
        .withColumn("num_genres", F.size("genres")) \
        .orderBy(F.col("stddev_popularity").asc(), F.col("avg_popularity").desc())
        
    #genres_per_artist.show()

    results = genres_per_artist.select(
        "artists", "avg_popularity", "stddev_popularity", "song_count", "genres", "num_genres"
        ).toJSON().map(lambda x: json.loads(x)).collect()

    if izlazni_folder:
        with open(f"{izlazni_folder}/sedmi_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
            
def osmi_zadatak(df, izlazni_folder=None):
    
    df2 = df.withColumn("acousticness", col("acousticness").cast("double")) \
            .withColumn("instrumentalness", col("instrumentalness").cast("double")) \
            .withColumn("speechiness", col("speechiness").cast("double")) \
            .withColumn("popularity", col("popularity").cast("double")) \
            .filter(col("popularity").isNotNull()) \
            .filter(col("track_genre").isNotNull())

    genre_stats = df2.groupBy("track_genre").agg(
        avg("acousticness").alias("avg_acousticness"),
        avg("instrumentalness").alias("avg_instrumentalness"),
        avg("speechiness").alias("avg_speechiness"),
        avg("popularity").alias("avg_popularity")
    )


    instrumental_genres_df = genre_stats.filter(
        (col("avg_acousticness") > 0.8) | (col("avg_instrumentalness") > 0.8)
    )

    instrumental_genres_list = [row["track_genre"] for row in instrumental_genres_df.select("track_genre").collect()]
    avg_popularity_instrumental = instrumental_genres_df.agg(avg("avg_popularity")).collect()[0][0]

    vocal_genres_df = genre_stats.filter(
        col("avg_speechiness") > 0.66
    )

    vocal_genres_list = [row["track_genre"] for row in vocal_genres_df.select("track_genre").collect()]
    avg_popularity_vocal = vocal_genres_df.agg(avg("avg_popularity")).collect()[0][0]

    if avg_popularity_instrumental is None or avg_popularity_vocal is None:
        conclusion = "Nedovoljno podataka za jednu ili obje grupe."
    elif avg_popularity_instrumental > avg_popularity_vocal:
        conclusion = "Instrumentalni fokus utiče pozitivno na komercijalni uspjeh."
    elif avg_popularity_instrumental < avg_popularity_vocal:
        conclusion = "Vokalno teški žanrovi imaju veću prosječnu popularnost."
    else:
        conclusion = "Obje grupe imaju približno istu prosječnu popularnost."

    result = {
        "instrumental_genres": {
            "count": len(instrumental_genres_list),
            "genres": sorted(instrumental_genres_list),
            "average_popularity": round(avg_popularity_instrumental, 3) if avg_popularity_instrumental else None
        },
        "vocal_heavy_genres": {
            "count": len(vocal_genres_list),
            "genres": sorted(vocal_genres_list),
            "average_popularity": round(avg_popularity_vocal, 3) if avg_popularity_vocal else None
        },
        "conclusion": conclusion
    }


    if izlazni_folder:
        with open(f"{izlazni_folder}/osmi_zadatak.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)

            
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SpotifyTracksAnalysis").getOrCreate()  

    df = spark.read.csv("./dataset.csv", header=True, schema=schema,quote='"',escape='"', multiLine=True)
    sys.stdout = open("output.txt", "w")
    #df.show(100)
    #df.printSchema()

    sys.stdout = sys.__stdout__
    
    numericke_kolone = [
        "popularity", "duration_ms", "danceability", "energy", "key", "loudness",
        "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence",
        "tempo", "time_signature"
    ]

    string_kolone = ["track_id", "artists", "album_name", "track_name", "track_genre"]
    boolean_kolone = ["explicit"]

    #prvi_zadatak(df, k=10, izlazni_folder="rezultati")
    #drugi_zadatak(df, izlazni_folder="rezultati")
    #treci_zadatak(df, izlazni_folder="rezultati")
    #cetvrti_zadatak(df, izlazni_folder="rezultati")
    #peti_zadatak(df, izlazni_folder="rezultati")
    #sesti_zadatak(df, izlazni_folder="rezultati")
    #sedmi_zadatak(df, izlazni_folder="rezultati")
    #osmi_zadatak(df, izlazni_folder="rezultati")
    
    
    
