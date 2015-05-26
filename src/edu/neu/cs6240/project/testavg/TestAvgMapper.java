package edu.neu.cs6240.project.testavg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestAvgMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private HashSet<String> testPageNames = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Path[] paths = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (paths == null || paths.length <= 0) {
			System.err.println("Empty Test Records File!");
			return;
		}
		testPageNames = new HashSet<String>();
		loadCachedTestRecords(paths[0]);
	}

	private void loadCachedTestRecords(Path cacheFilePath) {
		try {
			System.out.println("File path: " + cacheFilePath.toString());
			BufferedReader bufferReader = new BufferedReader(new FileReader(
					cacheFilePath.toString()));
			String pageName = "";
			while ((pageName = bufferReader.readLine()) != null) {
				testPageNames.add(pageName);
			}
			bufferReader.close();
			System.out.println("Test page names size: " + testPageNames.size());
		} catch (Exception ex) {
			System.err.println(ex.toString());
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(" ");
		if (parts.length != 4)
			return;
		try {
			String pageName = parts[1];
			int views = Integer.parseInt(parts[2]);
			if (testPageNames.contains(pageName)) {
				context.write(new Text(pageName), new IntWritable(views));
			}
		} catch (Exception ex) {
			return;
		}
	}
}
