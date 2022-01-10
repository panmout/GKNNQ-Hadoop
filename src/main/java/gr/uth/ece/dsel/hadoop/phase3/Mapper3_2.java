package gr.uth.ece.dsel.hadoop.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import java.util.HashSet;
import java.io.IOException;

public class Mapper3_2 extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String treeDir; // HDFS dir containing tree file
	private String treeFileName; // tree file name in HDFS
	private String treeFile; // full HDFS path to tree file
	private String overlapsDir; // HDFS dir containing overlaps file
	private String overlapsFileName; // overlaps file name in HDFS
	private String overlapsFile; // full HDFS path to overlaps file
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
		
		if (!overlaps.contains(cell)) // only output non-intersected cells
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
		
		hostname = conf.get("namenode"); // get namenode name
		username = System.getProperty("user.name"); // get user name
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		if (partitioning.equals("qt"))
		{
			treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			treeFileName = conf.get("treeFileName"); // get tree filename
			treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file
			
			root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (partitioning.equals("gd"))
			N = Integer.parseInt(conf.get("N")); // get N
		
		overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file
		overlaps = new HashSet<String>();
		
		overlaps = new HashSet<String>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
	}
}
