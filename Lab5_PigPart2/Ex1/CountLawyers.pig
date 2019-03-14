usersBag = LOAD '/home/cloudera/Desktop/DataLab5_2/users.txt' USING PigStorage('|') AS (userId:int,age:int,gender:chararray,occupation:chararray,zipCode:charArray);
layersBag = FILTER usersBag by (gender=='M') AND (occupation=='lawyer');
groupBag = GROUP layersBag ALL;
countLayers = FOREACH groupBag GENERATE group, COUNT(layersBag);
STORE countLayers INTO '/home/cloudera/Desktop/DataLab5_2/CountLayers.out';

