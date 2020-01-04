# %%
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import warnings
import ast
import re
warnings.filterwarnings('ignore')

'''
INSPECT CSVS
'''

# %%
ratings = pd.read_csv('data/ratings.csv', index_col='timestamp')
print(ratings.shape)
ratings.head(5)

# %%
# Parition ratings into batch-layer input and speed-layer input (pre-2018 and post-2018)
# ratings[ratings.index <= 1514846688].to_csv(path_or_buf='ratings_batch_input.csv')
# ratings[ratings.index > 1514846688].to_csv(path_or_buf='ratings_stream.csv')

# %%
movies = pd.read_csv('data/movies.csv', index_col='movieId')
print(movies.shape)
movies.head(5)


# %%
metadata = pd.read_csv('data/movies_metadata.csv', index_col='imdb_id')
metadata.shape
metadata.columns
metadata.head(5)

# %%
credits = pd.read_csv('data/credits.csv', index_col='id')
credits.shape
credits.head(5)

# %%
links = pd.read_csv('data/links.csv', index_col='imdbId')
links.shape
links.head(5)

'''
DATA PARSING
update "movies" csv to indicate each genre type as hot-encoded boolean
also add "year" column for future table joins
'''
# find total set of genres
# %%
gset = set()
for _, row in movies.iterrows():
    genres = row['genres'].split('|')
    gset |= set(genres)
sorted(gset)


# %%
# Add columns for each genre type
gset.remove('(no genres listed)')  # will default to 0's everywhere
for g in gset:
    movies[g] = False

# %%
# Iterate through rows again and mark genre-columns
for i, row in movies.iterrows():
    genres = row['genres'].split('|')
    for g in genres:
        if g != '(no genres listed)':
            movies.at[i, g] = True

# %%
# Extract year from title
movies['year'] = 'na'
for i, row in movies.iterrows():
    match = re.search(r'(\(\d+\))', row['title'])
    if match:
        movies.at[i, 'year'] = match[1][1:-1]

# %% remove movies without a Year
movies[movies.year == 'na'].shape

# %%
movie_genres = movies.drop('genres', axis=1)[movies.year != 'na']
movie_genres.head(5)
# movie_genres.to_csv(path_or_buf='movie_genres.csv')

# %%
# new table
genres = pd.read_csv('movie_genres.csv', index_col='movieId')
print(genres.shape)
genres.head(5)
