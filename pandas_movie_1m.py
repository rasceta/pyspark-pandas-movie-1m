import pandas as pd
import sqlalchemy
import time

url = "postgresql://postgres:postgres@localhost:5432/iykra" # Change this database url to your own database
engine = sqlalchemy.create_engine(url)

# Create a user-defined function to get rating in percent
def getRatingPercent(rating):
    return rating/5 * 100

start_time = time.time()

now = time.time()
ratings_df = pd.read_csv("ml-1m/ratings.dat", sep="::", names=['userID', 'movieID', 'rating', 'time'])
print(f"Loading ratings data: {time.time() - now} s")
print(f"Ratings data length: {len(ratings_df)}\n")

now = time.time()
movies_df = pd.read_csv("ml-1m/movies.dat", sep="::", names=['movieID', 'movieTitle', 'genres'])
print(f"Loading movies data: {time.time() - now} s")
print(f"Movies data length: {len(movies_df)}\n")

now = time.time()
ratings_df['ratingPercent'] = ratings_df['rating'].map(getRatingPercent).astype("int")
print(f"Creating rating percent column in ratings dataframe: {time.time() - now} s\n")

now = time.time()
joined_df = pd.merge(ratings_df, movies_df, how="inner", on="movieID")
print(f"Joining ratings and movies dataframes: {time.time() - now} s\n")

now = time.time()
rating_avg_df = joined_df[["movieID", "ratingPercent"]].groupby("movieID").mean()
print(f"Creating rating average per movieID dataframe: {time.time() - now} s\n")

now = time.time()
joined_df.to_sql('ratings_pandas', engine)
print(f"Writing joined dataframe to postgresql: {time.time() - now} s\n")

print(f"Total time: {time.time() - start_time} s")