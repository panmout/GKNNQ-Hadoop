package gr.uth.ece.dsel.hadoop.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import gr.uth.ece.dsel.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class Reducer3 extends Reducer<Text, Text, Text, Text>
{
	private int K; // user defined (k-nn)
	private PriorityQueue<IdDist> neighbors3; // new neighbors list
	private String mode; // bf or ps
	private BfNeighbors bfn;
	private PsNeighbors psn;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//int cell = key.get(); // key is cell_id (mappers' output)

		// list containing training points
		ArrayList<Point> tpoints = new ArrayList<>(); // list of tpoints in this cell

		boolean proceed = false;
		
		for (Text value: values) // run through value of mapper output
		{
			String line = value.toString(); // read a line
			
			if (line.trim().split("\t")[0].equals("true")) // proceed only if Mapper output contains "true" (otherwise the cell was pruned)
			{
				proceed = true;
			}
			else
			{
				tpoints.add(GnnFunctions.stringToPoint(line, "\t")); // create point and add to tpoints list
			}
		}
		
		if (proceed)
		{
			// set TOTAL_POINTS metrics variable
			context.getCounter(Metrics.TOTAL_TPOINTS).increment(tpoints.size());
			
			// max heap of K neighbors (IdDist)
			if (mode.equals("bf"))
			{
				bfn.setTpoints(tpoints);
				neighbors3.addAll(bfn.getNeighbors());
			}
			else if (mode.equals("ps"))
			{
				psn.setTpoints(tpoints);
				neighbors3.addAll(psn.getNeighbors());
			}
			else
				throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		// write output
		// outKey = null
		// outValue is {tpoint1_id, dist1, tpoint2_id, dist2,...,tpointK_id, distK}
		
		PriorityQueue<IdDist> neighbors = new PriorityQueue<>(K, new IdDistComparator("min")); // min heap
		
		while (!neighbors3.isEmpty())
		{
			IdDist neighbor = neighbors3.poll();
			if (!GnnFunctions.isDuplicate(neighbors, neighbor))
				neighbors.add(neighbor);
		}
		
		String outValue = (neighbors.isEmpty()) ? null: GnnFunctions.pqToString(neighbors, K, "min"); // get PQ as String
		
		if (outValue != null)
			context.write(null, new Text(outValue));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		K = Integer.parseInt(conf.get("K"));

		// hostname
		String hostname = conf.get("namenode"); // get namenode name
		// username
		String username = System.getProperty("user.name"); // get user name

		// HDFS dir containing query file
		String queryDatasetDir = conf.get("queryDir"); // get query dataset dir
		// query file name in HDFS
		String queryDatasetFileName = conf.get("queryFileName"); // get query dataset filename
		// full HDFS path to query file
		String queryDatasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, queryDatasetDir, queryDatasetFileName); // full HDFS path to query dataset file

		// HDFS dir containing mbrCentroid file
		String mbrCentroidDir = conf.get("mbrCentroidDir"); // get mbrCentroid dir
		// mbrCentroid file name in HDFS
		String mbrCentroidFileName = conf.get("mbrCentroidFileName"); // get mbrCentroid filename
		// full HDFS path to tree file
		String mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, mbrCentroidDir, mbrCentroidFileName); // full HDFS path to mbrCentroid file

		// HDFS dir containing gnn25 file
		String gnn25Dir = conf.get("gnn25Dir"); // HDFS directory containing gnn25 file
		// gnn25 file name in HDFS
		String gnn25FileName = conf.get("gnn25FileName"); // get gnn25 filename
		// full HDFS path to gnn25 file
		String gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file

		// break sumDist loops on (true) or off (false)
		boolean fastSums = conf.getBoolean("fastSums", false); // default : false (normal mode)

		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration

		// array of doubles to put MBR, centroid, sumdist(centroid, Q)
		double[] mbrC = ReadHdfsFiles.getMbrCentroid(mbrCentroidFile, fs); // read mbrCentroid array

		// Phase 2.5 neighbors
		PriorityQueue<IdDist> neighbors2 = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
		neighbors2.addAll(ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, K)); // add Phase 2.5 neighbors
		
		neighbors3 = new PriorityQueue<>(K, new IdDistComparator("max"));
		
		mode = conf.get("mode");

		// list containing query points
		ArrayList<Point> qpoints;
		if (mode.equals("bf"))
		{
			qpoints = new ArrayList<>(ReadHdfsFiles.getQueryPoints(queryDatasetFile, fs)); // read querypoints
			bfn = new BfNeighbors(K, mbrC, qpoints, neighbors2, fastSums, context);
		}
		else if (mode.equals("ps"))
		{
			qpoints = new ArrayList<>(ReadHdfsFiles.getSortedQueryPoints(queryDatasetFile, fs)); // read querypoints
			psn = new PsNeighbors(K, mbrC, qpoints, neighbors2, fastSums, context);
		}
	}
}
