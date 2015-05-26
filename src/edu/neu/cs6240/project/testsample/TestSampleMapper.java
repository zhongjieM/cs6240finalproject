package edu.neu.cs6240.project.testsample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.neu.cs6240.project.preprocess.PreProcessUtils;

public class TestSampleMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(" ");
		if (parts.length != 4 || !PreProcessUtils.isPageName(parts[1]))
			return;
		int views = Integer.parseInt(parts[2]);
		if (views > 100)
			context.write(NullWritable.get(), new Text(parts[1]));
	}

}
