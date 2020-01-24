# Movie-Lens-Data-Analysis
The objective of the project is to analyze movies dataset and solve below mentioned KPIs. The dataset contains following files


<b>Top ten most viewed movies with their movies Name (Ascending or Descending order)</b>
(code is in src/main/java/com/cdac/movielens/MostViewed package)

 1.For this analysis I have written two map reduce jobs.
 2.First map-reduce job output will be movies and their respective view count
 
 3.Second map-reduce job output will be top k (k is configurable) viewed movie name with their view count.
 4.Output of first map-reduce job is given as input to second map-reduce job. Also movies.dat file is provided to secon job         reducer through distributed cache.
 5. One map is constructed in the setup() method of reducer to get movie name corrosponding to movie id.

#Command to run this task-->
<code>
hadoop jar eclipse-hadoop/Movie-Lens-Data-Analysis/target/movielens-0.0.1-SNAPSHOT.jar com.cdac.movielens.MostViewed.Driver -    Dtopk=10  /movielens/ratings.dat /movielens/rating/output/mostviewed /movielens/rating/output/topviewed
 </code>
 #Output of first mapreduce job(Command-  hadoop fs -head /movielens/rating/output/mostviewed/part-r-00000)
<code>
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

 
 
 
 
