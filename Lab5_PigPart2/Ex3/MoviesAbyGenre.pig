REGISTER /home/cloudera/Desktop/piggybank-0.17.0.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
moviesBag = LOAD '/home/cloudera/Desktop/DataLab5_2/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);
moviesABag = FILTER moviesBag BY STARTSWITH(title, 'a') OR STARTSWITH(title,'A');
moviesGenBag = FOREACH moviesABag GENERATE TOKENIZE(genres, '|');
moviesGenFlatBag = FOREACH moviesGenBag GENERATE FLATTEN($0) AS genre;
moviesGroupBag = GROUP moviesGenFlatBag BY genre;
countbyGenBag = FOREACH moviesGroupBag GENERATE group, COUNT(moviesGenFlatBag.genre);
STORE countbyGenBag INTO '/home/cloudera/Desktop/DataLab5_2/MoviesAbyGenre.out';

