REGISTER /home/cloudera/Desktop/piggybank-0.17.0.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
moviesBag = LOAD '/home/cloudera/Desktop/DataLab5_2/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);
ratingsBag = LOAD '/home/cloudera/Desktop/DataLab5_2/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:chararray);
moviesGenresBag = FOREACH moviesBag GENERATE movieId, title, TOKENIZE(genres, '|');
moviesGenresFlatBag = FOREACH moviesGenresBag GENERATE movieId, title, FLATTEN($2) AS genre;
moviesAdvBag = FILTER moviesGenresFlatBag BY (genre == 'Adventure');
moviesRated5Bag = FILTER ratingsBag BY rating==5;
Rated5Grouped = GROUP moviesRated5Bag BY movieId;
Rated5Distinct = FOREACH Rated5Grouped GENERATE group, 5 AS maxScore;
moviesRatedBag = JOIN moviesAdvBag BY movieId, Rated5Distinct BY $0;
moviesProjection = FOREACH moviesRatedBag GENERATE movieId as MovieId, genre as Genre, maxScore as Rating, title as Title;
moviesProjectionSorted = ORDER moviesProjection BY MovieId;
moviesRated5Top20 = limit moviesProjectionSorted 20;
STORE moviesRated5Top20 INTO '/home/cloudera/Desktop/DataLab5_2/Top20Rated5Headers.out' USING CSVExcelStorage;
