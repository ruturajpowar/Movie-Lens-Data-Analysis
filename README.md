# Movie-Lens-Data-Analysis
The objective of the project is to analyze movies dataset and solve below mentioned KPIs. The dataset contains following files

<h3>Prerequisites</h3>
<ul>
 <li>hadoop-3.0.0</li>
<li>java-8</li>
</ul>


<b>Top ten most viewed movies with their movies Name (Ascending or Descending order)</b>
(code is in src/main/java/com/cdac/movielens/MostViewed package)

 1.For this analysis I have written two map reduce jobs.
 2.First map-reduce job output will be movies and their respective view count
 
 3.Second map-reduce job output will be top k (k is configurable) viewed movie name with their view count.
 4.Output of first map-reduce job is given as input to second map-reduce job. Also movies.dat file is provided to secon job         reducer through distributed cache.
 5. One map is constructed in the setup() method of reducer to get movie name corrosponding to movie id.

#Command to run this task-->
<p>
<code>
hadoop jar eclipse-hadoop/Movie-Lens-Data-Analysis/target/movielens-0.0.1-SNAPSHOT.jar com.cdac.movielens.MostViewed.Driver -    Dtopk=10  /movielens/ratings.dat /movielens/rating/output/mostviewed /movielens/rating/output/topviewed
 </code>
 </p>
 #Output of first mapreduce job(Command-  hadoop fs -head /movielens/rating/output/mostviewed/part-r-00000)
<code>
 <p>movieId viewCount</p>
<p>1	2077 </p>
<p>10	888 </p>
<p>100	128 </p>
<p>1000	20 </p>
<p>1002	8 </p>
<p>1003	121 </p>
<p>1004	101 </p>
<p>1005	142 </p>
<p>1006	78 </p>
<p>1007	232 </p>
<p>1008	97 </p>
<p>1009	291 </p>
<p>101	253 </p>
<p>1010	242 </p>
</code>

#Output of second map-reduce job(Command- hadoop fs -head /movielens/rating/output/topviewed/part-r-00000)
<code>
<P>Movie Name View Count</p>
<p>American Beauty (1999)	3428</p>
<p>Star Wars: Episode IV - A New Hope (1977)	2991</p>
<p>Star Wars: Episode V - The Empire Strikes Back (1980)	2990</p>
<p>Star Wars: Episode VI - Return of the Jedi (1983)	2883</p>
<p>Jurassic Park (1993)	2672</p>
<p>Saving Private Ryan (1998)	2653</p>
<p>Terminator 2: Judgment Day (1991)	2649</p>
<p>Matrix, The (1999)	2590</p>
<p>Back to the Future (1985)	2583</p>
<p>Silence of the Lambs, The (1991)	2578</p>
 </code>

 
 
<b>Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40
users)</b>
<p>
(code is in src/main/java/com/cdac/movielens/MostViewed package)
 </p>
 
 1.For this analysis I have written two map reduce jobs.
 2.First map-reduce job output will be <b>movieId</b> and their respective <b>average Rating count</b>
   in the reducer check has been applied to check weither movieId is rated by more than 40 users.And only when tis condition    satisfies output is written to hdfs.
 3.Second map-reduce job output will be top k (k is configurable) viewed <b>movie name</b> with their <b>avg rating count.      </b>
  <p>
 <b>Here one custom sort class is provided to job (SortDoubleComparator) in order to sort avg rating in descending order so that we can easily pick up top k rated movies</b>
 </p>
 4.Output of first map-reduce job is given as input to second map-reduce job. Also movies.dat file is provided to secon job         reducer through distributed cache.
 5. One map is constructed in the setup() method of reducer to get movie name corrosponding to movie id.
 
 #Command to run this task-->
<p>
<code>
hadoop jar eclipse-hadoop/Movie-Lens-Data-Analysis/target/movielens-0.0.1-SNAPSHOT.jar com.cdac.movielens.MostRated.Driver -Dtopk=25  /movielens/ratings.dat /movielens/rating/output/mostrated /movielens/rating/output/toprated
 </code>
 </p>
 #Output of first mapreduce job(Command-  hadoop fs -head /movielens/rating/output/mostrated/part-r-00000)
 <code>
<p>movieId avgRating</p>
 <p>1	4.146846413095811</p>
 <p>10	3.5405405405405403</p>
 <p>100	3.0625</p>
 <p>1003	2.9421487603305785</p>
 <p>1004	2.6633663366336635</p>
 <p>1005	2.3732394366197185</p>
 <p>1006	3.08974358974359</p>
 <p>1007	2.978448275862069</p>
 <p>1008	3.329896907216495</p>
 <p>1009	3.185567010309278</p>
 <p>101	3.869565217391304</p>
 <p>1010	3.268595041322314</p>
 <p>1011	2.725925925925926</p>
 <p>1012	3.7043189368770766</p>
</code>


#Output of second map-reduce job(Command- hadoop fs -head /movielens/rating/output/toprated/part-r-00000)
<code>
<P>Movie-Name Avg-Rating</p>
<P>Sanjuro (1962)	4.608695652173913</p>
<P>Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.560509554140127</p>
<P>Shawshank Redemption, The (1994)	4.554557700942973</p>
<P>Godfather, The (1972)	4.524966261808367</p>
<P>Close Shave, A (1995)	4.52054794520548</p>
<P>Usual Suspects, The (1995)	4.517106001121705</p>
<P>Schindler's List (1993)	4.510416666666667</p>
<P>Wrong Trousers, The (1993)	4.507936507936508</p>
<P>Sunset Blvd. (a.k.a. Sunset Boulevard) (1950)	4.491489361702127</p>
<P>Raiders of the Lost Ark (1981)	4.477724741447892</p>
<P>Rear Window (1954)	4.476190476190476</p>
<P>Paths of Glory (1957)	4.473913043478261</p>
<P>Star Wars: Episode IV - A New Hope (1977)	4.453694416583082</p>
<P>Third Man, The (1949)	4.452083333333333</p>
<P>Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)	4.4498902706656915</p>
<P>Wallace & Gromit: The Best of Aardman Animation (1996)	4.426940639269406</p>
<P>To Kill a Mockingbird (1962)	4.</p>
<P>Double Indemnity (1944)	4.415607985480944</p>
<P>Casablanca (1942)	4.412822049131217</p>
<P>World of Apu, The (Apur Sansar) (1959)	4.410714285714286</p>
</code>


<b>Genres ranked by Average Rating, for each profession and
age group.</b>
(code is in src/main/java/com/cdac/movielens/genresRanking package)

1. First job joins the users and rating dataset. And also we are doing a replicate join in the users mapper class to join the profession id to profession mapping data into the output.
2. Second job joins the previous output with the movies dataset and also split one record per genre so that we can calculate ranking based on genre.
3. Third job aggregates the data based on the genre and the age group and ranks the data based on genre and age group as per the solution required.


#Output of third map-reduce job(Command- hadoop fs -head /movielens/genresRating/output/thirdjob/part-r-00000)
<code>
<p>K-12 student::18-35::War::Drama::Action::Thriller::Comedy</p>
<p>academic/educator::18-35::Western::Crime::Musical::Sci-Fi::Film-Noir</p>
<p>academic/educator::36-50::Western::Western::Crime::Musical::Crime</p>
<p>academic/educator::50+::Western::Western::Crime::Musical::Sci-Fi</p>
<p>artist::18-35::Western::Western::Crime::Documentary::Western</p>
<p>artist::36-50::Western::Western::Western::Crime::Documentary</p>
<p>artist::50+::Western::Western::Western::Crime::Documentary</p>
<p>clerical/admin::18-35::Western::Western::Western::Western::Crime</p>
<p>clerical/admin::36-50::Western::Western::Western::Western::Crime</p>
<p>clerical/admin::50+::Western::Western::Western::Western::Western</p>
<p>college/grad student::18-35::Western::Western::Western::Western::Western</p>
<p>college/grad student::36-50::Western::Western::Western::Western::Western</p>
<p>customer service::18-35::Western::Western::Western::Western::Western</p>
<p>customer service::36-50::Western::Western::Western::Western::Western</p>
<p>doctor/health care::18-35::Western::Western::Western::Western::Wester</p>
 </code>
