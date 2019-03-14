fileBag = LOAD '/home/cloudera/Desktop/Lab2-WC-Input.txt' AS (line:chararray);
tokenBag = FOREACH fileBag GENERATE FLATTEN (TOKENIZE(line));
groupBag = GROUP tokenBag BY $0;
wordcount = FOREACH groupBag GENERATE group, COUNT($1);
wordcountsorted = ORDER wordcount BY $1 DESC;
STORE wordcountsorted INTO '/home/cloudera/Desktop/countwords' USING PigStorage(',');
