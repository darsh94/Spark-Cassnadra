import pandas as pd
from cassandra.cluster import Cluster
import re




KEYSPACE='movie_dataset'
def get_connection():
	cluster=Cluster()
	session=cluster.connect()
	return session

def create_tables(conn):
	conn.execute('''USE %s'''%KEYSPACE)
	#Creating movie_table
	conn.execute(''' CREATE TABLE IF NOT EXISTS movies(movie_id int PRIMARY KEY, movie_title text, genres text, year int)''')
	#Creating reviews table
	conn.execute('''CREATE TABLE IF NOT EXISTS ratings(user_id int, movie_id int, rating double, timestamp bigint, primary key((user_id), movie_id))''')
	

def get_year(x):
	
	temp=re.findall(r'[0-9]{4}',x)

		
	if(len(temp)>=1):
		temp_year=temp[0].replace(')','').replace('(','')
		return int(temp_year)
	else:
		return -99
def remove_year(x):
	temp=re.sub(r'[0-9]{4}','',x)
	if(temp):
		return temp.replace('(','').replace(')','')
	else:
		return x
		
def main():
	conn=get_connection()
	conn.execute("""
        CREATE  KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)
	create_tables(conn)
	#Loading movies.csv	
	#movie_df=pd.read_csv('/home/darsh/Tapad/ml-latest-small/movies.csv')
	movie_df=pd.read_csv('/home/darsh/Downloads/ml-latest-small/movies.csv')
	cols_movie=tuple(movie_df.columns)
	movie_df['year']=movie_df['title'].apply(get_year)
	movie_df['movie_title']=movie_df['title'].apply(remove_year)
	#print(movie_df.count())
	#exit()
	movie_df.drop('title',inplace=True,axis=1)

	#Checking if year retrieved properly
	#print(movie_df.head())
	#exit()
	

	#Inserting all movies into movies table
	for i,v in movie_df.iterrows():
		#print (int(v['movieId']),v['movie_title'],v['genres'])
		
		
		conn.execute('INSERT INTO movies (movie_id, movie_title, genres,year) VALUES(%s,%s,%s,%s)',(int(v['movieId']),v['movie_title'],v['genres'],int(v['year'])))
	print("Movie Db Inserted")
	#loading rating.csv	
	rating_df=pd.read_csv('/home/darsh/Downloads/ml-latest-small/ratings.csv')
	#inserting rating into db
	for i,v in rating_df.iterrows():
		conn.execute('INSERT INTO ratings (user_id,movie_id,rating,timestamp) VALUES(%s,%s,%s,%s)',(int(v['userId']),int(v['movieId']),float(v['rating']),int(v['timestamp'])))
		
		
	print("ratings db inserted")

	



if __name__=='__main__':
	main()
