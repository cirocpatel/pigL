# pigL
pigLatin

---------------Realtion named "ratings" with a given schema-----------------------


ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS
(userID:int, movieID:int, rating:int, ratingTime:int);


---------------Use PigStorage if you need a different delimiter--------------------


metadata = LOAD '/user/maria_dev/ml-100k/u7.item' USING
           PigStorage('|')AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealse:chararray, imdbLink:chararray);
DUMP metadata;


----------------Creating a realation from another relation; FOREACH / GENERATE---------

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToData(releaseDate, 'dd-MMM-YYYY;)) AS releaseTime;

--------------------------------------Group BY----------------------------------------------

ratingByMovie = GROUP ratings BY movieID;
DUMP ratingsByMovie;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRatings;
DUMP avgRatings;

DESCRIBE ratings;
DESCRIBE ratingsByMovie;
DESCRIBE avgRatings;

ratings:{userID:int, movieiD:int, rating:int, ratingTime:int}
ratingsByMovie:{group:int,ratings:{(userID:int,movieID:int,rating:int,ratingTime:int)}}
avgRatings:{movieID:int,avgRating:double}

---------------------------------------------------FILTER ----------------------------------

fiveStarMovies = Filter avgRatings BY avgRating > 4.0;

----------------------------------------- JOIN ---------------------------------------------

DESCRIBE fiveStarMovies;
DESCRIBE nameLookup;
fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;
DESCRIBE fiveStarsWithData;
DUMP fiveStarsWithData;

        fiveStarMovies:{movieID:int,avgRating:double]
        nameLookup:{movieID:int,movieTitle:chararray,releaseTime:long}
        
        fiveStarWithData:{fiveStarMovies::movieID:int,fiveStarMovies::avgRating:double,
        nameLookup::movieID:int,namelookup::movieTitle:chararray,nameLookup::releaseTime:long}
        
------------------------------------ORDER BY ------------------------------

oldestFiveStarMovies = ORDER fiveStarWithData BY
                nameLookup::releaseTime;
                
                DUMP oldestFiveStarMovies;
                
-------------------------------------RECAP (ALL Together)-----------------------------------------------

ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') 
AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, 
          ToUnixTime(ToDate(releaseDate, 'dd-MMM-YYYY')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

fiveStarMovies = Filter avgRatings BY avgRating > 4.0;

fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;

    

