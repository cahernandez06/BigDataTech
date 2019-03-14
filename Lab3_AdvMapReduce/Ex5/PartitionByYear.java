import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionByYear extends Partitioner<SortDesc, pairTC>{

	@Override
	public int getPartition(SortDesc arg0, pairTC arg1, int arg2) {
		if (arg2 == 0){
			return 0;
		} else if(Integer.parseInt(arg0.toString()) < 1930){
			return 0;
		} else {
			return 1;
		}
	}

}

