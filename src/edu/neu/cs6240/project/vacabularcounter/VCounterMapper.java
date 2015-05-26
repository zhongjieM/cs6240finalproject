package edu.neu.cs6240.project.vacabularcounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.neu.cs6240.project.preprocess.PreProcessUtils;

public class VCounterMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(" ");
		if (parts.length != 4 || !PreProcessUtils.isPageName(parts[1]))
			return;
		String[] words = PreProcessUtils.splitPageName(parts[1]);
		for (String word : words) {
			if (word.length() > 10 || word.length() == 0)
				continue;
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
