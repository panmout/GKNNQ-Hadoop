package gr.uth.ece.dsel.hadoop.phase1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import gr.uth.ece.dsel.hadoop.util.Metrics;
import java.io.IOException;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum = 0; // sum of training points in this cell
		
		for (IntWritable val: values) // sum all 1's for each cell_id (key)
		{
			sum += val.get();
		}
		// increment cells number
		context.getCounter(Metrics.NUM_CELLS).increment(1);
		
		context.write(key, new IntWritable(sum));
	}
}
