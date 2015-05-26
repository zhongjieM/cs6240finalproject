package edu.neu.cs6240.project.testavg;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestAvgReducer extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String pageName = key.toString();
		int count = 0;
		int total = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			count++;
			IntWritable value = iterator.next();
			total += value.get();
		}
		context.write(new Text(pageName), new DoubleWritable(total * 1.0
				/ count));
	}
}
