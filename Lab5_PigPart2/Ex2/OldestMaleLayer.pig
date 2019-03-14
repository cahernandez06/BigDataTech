usersBag = LOAD '/home/cloudera/Desktop/DataLab5_2/users.txt' USING PigStorage('|') AS (userId:int,age:int,gender:chararray,occupation:chararray,zipCode:charArray);
layersBag = FILTER usersBag BY (gender=='M') AND (occupation=='lawyer');
lawyersByAge = ORDER layersBag by age DESC;
layersTransf = FOREACH lawyersByAge GENERATE userId;
oldestMaleLayer = LIMIT layersTransf 1;
STORE oldestMaleLayer INTO '/home/cloudera/Desktop/DataLab5_2/OldestMaleLayer.out';
