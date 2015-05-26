package edu.neu.cs6240.project.calculate;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.neu.cs6240.project.preprocess.PreProcessUtils;

public class CalculationMapper extends
		Mapper<LongWritable, Text, MapOutputKey, LongWritable> {

	private static final String SPLITER = " ";
	// 10 is a good granularity for an hour.
	private static int GRANULARITY = 100;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(SPLITER);
		if (parts.length != 4 || !PreProcessUtils.isPageName(parts[1]))
			return;
		try {

			int views = Integer.parseInt(parts[2]);
			int rangeIndex = views / GRANULARITY;
			String[] words = PreProcessUtils.splitPageName(parts[1]);
			context.write(new MapOutputKey(rangeIndex, "dummy"),
					new LongWritable(1));
			HashSet<String> existed = new HashSet<String>();
			for (String word : words) {
				if (word.length() > 10 || word.length() == 0
						|| existed.contains(word))
					continue;
				existed.add(word);
				context.write(new MapOutputKey(rangeIndex, word),
						new LongWritable(1));
			}
		} catch (Exception ex) {
			return;
		}
	}
}
