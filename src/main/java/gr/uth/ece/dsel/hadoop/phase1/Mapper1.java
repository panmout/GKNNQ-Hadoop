package gr.uth.ece.dsel.hadoop.phase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop.util.*;
import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private String partitioning; // bf or qt
	private Node root; // create root node
	private int N; // N*N cells
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line
		
		Point p = GnnFunctions.stringToPoint(line, "\t");
		
		String cell = null;
		
		if (partitioning.equals("qt")) // quadtree cell
			cell = GnnFunctions.pointToCellQT(p.getX(), p.getY(), root);
		else if (partitioning.equals("gd")) // grid cell
			cell = GnnFunctions.pointToCellGD(p, N);	
		
		context.write(new Text(cell), new IntWritable(1));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		partitioning = conf.get("partitioning");
		
		if (partitioning.equals("qt"))
		{
			// hostname
			String hostname = conf.get("namenode"); // get namenode name
			// username
			String username = System.getProperty("user.name"); // get user name
			// HDFS dir containing tree file
			String treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			// tree file name in HDFS
			String treeFileName = conf.get("treeFileName"); // get tree filename
			// full HDFS path to tree file
			String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file
			FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
			
			root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (partitioning.equals("gd"))
			N = Integer.parseInt(conf.get("N")); // get N
	}
}
