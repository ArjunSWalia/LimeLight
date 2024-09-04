import pandas as pd
import random
import ast
import numpy as np
from datetime import datetime, timedelta


def generate_sql_insert_statement(csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    sql_template = "INSERT INTO movies (title, release_date, runtime, description, poster_link,credit_link_id, adult) VALUES\n"
    
    value_tuples = []

    for index, row in df.iterrows():
        description = row['description'].replace("'", "''")
        title = row['title'].replace("'", "''")
        
        value_tuple = "('{title}', {release_date}, {runtime}, '{description}', '{poster_link}', {credit_link_id}, {adult})".format(
            title=title,
            release_date=row['release_date'],
            runtime=row['runtime'],
            description=description,
            poster_link=row['poster_link'],
            credit_link_id = row['credit_link_id'],
            adult=str(row['adult']).lower()  
        )
        
        value_tuples.append(value_tuple)
    
    sql_statement = sql_template + ",\n".join(value_tuples) + ";"
    
    return sql_statement

def write_sql_to_file(sql_statement, output_file_path):
    with open(output_file_path, 'w') as file:
        file.write(sql_statement)

csv_file_path = ''
output_file_path = ''

sql_statement = generate_sql_insert_statement(csv_file_path)

write_sql_to_file(sql_statement, output_file_path)




reviews_df = pd.read_csv('', low_memory=False)

voter_uid_list = []
reviewer_uid_list = []
mid_list = []
vote_list = []

for index, row in reviews_df.iterrows():
    reviewer_uid = row['uid']
    mid = row['mid']
    
    if random.random() < 0.5:
        voter_uid = random.randint(1, 5)
    else:
        voter_uid = random.randint(100, 13880)
    
    vote = random.choice([0, 1])
    voter_uid_list.append(voter_uid)
    reviewer_uid_list.append(reviewer_uid)
    mid_list.append(mid)
    vote_list.append(vote)

votes_df = pd.DataFrame({
    'voter_uid': voter_uid_list,
    'reviewer_uid': reviewer_uid_list,
    'mid': mid_list,
    'vote': vote_list
})

votes_df.to_csv('', index=False)




csv_file_path = ''
df = pd.read_csv(csv_file_path, low_memory=False)

def extract_collection_name(collection):
    try:
        if pd.isna(collection):
            return None
        collection_dict = ast.literal_eval(collection)
        return collection_dict.get('name', None)
    except (ValueError, SyntaxError, AttributeError):
        return None

df['collection_name'] = df['belongs_to_collection'].apply(extract_collection_name)

selected_columns = ['title', 'release_date', 'runtime', 'overview', 'poster_path','id','adult']
result_df = df[selected_columns]


for i, row in result_df.iterrows():
    if row.isna().any():
        print("'",row[5],"',")
result_df = result_df.dropna(subset=selected_columns)
result_df['runtime'] = result_df['runtime'].astype(int)
result_df['release_date'] = result_df['release_date'].apply(lambda x: f"'{x}'")


result_df = result_df[selected_columns]
result_df.columns = ['title', 'release_date', 'runtime', 'description', 'poster_link','credit_link_id',
                     'adult']


result_df.to_csv('', index=False)



csv_file_path = ''

df = pd.read_csv(csv_file_path, low_memory=False)

genres_data = []

valid_genres = ["'War'", "'Science Fiction'", "'Crime'", "'Action'", "'Family'", "'TV Movie'", "'Mystery'", "'Adventure'", "'Fantasy'", "'Foreign'", "'Western'", "'Music'", "'Documentary'", "'Romance'", "'Thriller'", "'Drama'", "'History'", "'Horror'", "'Animation'", "'Comedy'"]
unique = []
def process_genres(genres, mid):
    genres_list = ast.literal_eval(genres)
    for genre in genres_list:
        genre_name = f"'{genre.get('name')}'"
        if genre_name in valid_genres:
            genres_data.append({'mid': mid, 'genre': genre_name})
            unique.append(genre_name)

for index, row in df.iterrows():
    mid = row['id']
    if pd.notna(row['genres']):
        process_genres(row['genres'], mid)

genres_df = pd.DataFrame(genres_data)
unique_set = set(unique)

genres_df.to_csv('', index=False)






csv_file_path = ''


df = pread_csv(csv_file_path, low_memory=False)

output_data = []

def process_credits(credits, mid, role):
    credits_list = ast.literal_eval(credits)
    for credit in credits_list:
        name = f"'{credit.get('name')}'"
        character = f"'{credit.get('character')}'" if role == 'actor' else 'NULL'
        formatted_role = f"'{role}'"
        output_data.append({'mid': mid, 'name': name, 'role': formatted_role, 'character': character})

for index, row in df.iterrows():
    mid = row['id']
    if pd.notna(row['cast']):
        process_credits(row['cast'], mid, 'actor')
    if pd.notna(row['crew']):
        process_credits(row['crew'], mid, 'crew')

output_df = pd.DataFrame(output_data)

output_df = output_df[['mid', 'name', 'role', 'character']]

print(output_df)
output_df.to_csv('', index=False)






uid_range = list(range(1, 6)) + list(range(100, 13982))
mid_range = range(1, 44326)
rank_range = range(1, 6)

np.random.seed(42) 
uids = np.random.choice(uid_range, size=10000, replace=True)
mids = np.random.choice(mid_range, size=10000, replace=True)
ranks = np.random.choice(rank_range, size=10000, replace=True)

favourites_df = pd.DataFrame({
    'uid': uids,
    'mid': mids,
    'rank': ranks
})

csv_path = ''
favourites_df.to_csv(csv_path, index=False)


def generate_sql_insert_statement(csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    sql_template = "INSERT INTO movies_genres (mid, genre) VALUES\n"
    
    value_tuples = []

    for index, row in df.iterrows():
        genre = row['genre'].strip("'")
        
        value_tuple = "({mid}, '{genre}')".format(
            mid=row['mid'],
            genre=genre
        )
        
        value_tuples.append(value_tuple)
    
    sql_statement = sql_template + ",\n".join(value_tuples) + ";"
    
    return sql_statement

def write_sql_to_file(sql_statement, output_file_path):
    with open(output_file_path, 'w') as file:
        file.write(sql_statement)

csv_file_path = ''
output_file_path = ''

sql_statement = generate_sql_insert_statement(csv_file_path)

write_sql_to_file(sql_statement, output_file_path)




uid_range = list(range(1, 6)) + list(range(100, 13982))
mid_range = range(1, 44326)

start_date = datetime(2023, 7, 1)
end_date = datetime.now()

def random_dates(start, end, n=10000):
    return [start + timedelta(days=np.random.randint(0, (end - start).days)) for _ in range(n)]

np.random.seed(42) 
uids = np.random.choice(uid_range, size=10000, replace=True)
mids = np.random.choice(mid_range, size=10000, replace=True)
dates_watched = random_dates(start_date, end_date, 10000)

watched_df = pd.DataFrame({
    'uid': uids,
    'mid': mids,
    'date_watched': dates_watched
})

csv_path = ''
watched_df.to_csv(csv_path, index=False)

