package gr.uth.ece.dsel.hadoop.phase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import java.util.HashSet;
import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private Node root; // create root node
	private int N; // N*N cells
	private HashSet<String> overlaps;
	
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
		
		if (overlaps.contains(cell))
		{
			String outValue = String.format("%d\t%11.10f\t%11.10f", p.getId(), p.getX(), p.getY());
			context.write(new Text(cell), new Text(outValue));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		partitioning = conf.get("partitioning");

		// hostname
		String hostname = conf.get("namenode"); // get namenode name
		// username
		String username = System.getProperty("user.name"); // get user name

		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		if (partitioning.equals("qt"))
		{
			// HDFS dir containing tree file
			String treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			// tree file name in HDFS
			String treeFileName = conf.get("treeFileName"); // get tree filename
			// full HDFS path to tree file
			String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file

			root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (partitioning.equals("gd"))
			N = Integer.parseInt(conf.get("N")); // get N

		// HDFS dir containing overlaps file
		String overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		// overlaps file name in HDFS
		String overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		// full HDFS path to overlaps file
		String overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file

		overlaps = new HashSet<>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
	}
}
