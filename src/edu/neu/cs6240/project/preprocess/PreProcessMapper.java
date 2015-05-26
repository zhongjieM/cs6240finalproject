package edu.neu.cs6240.project.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreProcessMapper extends
		Mapper<LongWritable, Text, NullWritable, InterValue> {

	private static final String SPLITER = " ";

	private long maxViews = 0;
	private long longestWordLength = 0;
	private String longestWord = "";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		maxViews = 0;
		longestWordLength = 0;
		longestWord = "";
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(SPLITER);
		if (parts.length != 4)
			return;
		try {
			long views = Long.parseLong(parts[2]);
			maxViews = Math.max(maxViews, views);
			for (String word : PreProcessUtils.splitPageName(parts[1])) {
				if (word.length() > 20)
					continue;
				if (word.length() > longestWordLength) {
					longestWord = word;
					longestWordLength = word.length();
				}
			}
		} catch (Exception ex) {
			return;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(NullWritable.get(), new InterValue(maxViews, longestWord,
				longestWordLength));
	}
}
