package edu.neu.cs6240.project.vacabularcounter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VCounterReducer extends
		Reducer<Text, IntWritable, Text, NullWritable> {

	private long localCounter = 0;
	private long globalCounter = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		localCounter = 0;
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String pre = "";
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			if (!key.toString().equals(pre)) {
				pre = key.toString();
				localCounter++;
				context.write(new Text(key.toString()), NullWritable.get());
			}
			globalCounter++;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println("Global counter : " + globalCounter);
		context.getCounter("COUNTER_GROUP", "COUNT_VAC")
				.increment(localCounter);
	}
}
