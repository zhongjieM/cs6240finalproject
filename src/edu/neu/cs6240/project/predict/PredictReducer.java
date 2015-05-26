package edu.neu.cs6240.project.predict;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictReducer extends
		Reducer<MapOutputKey, IntWritable, NullWritable, IntWritable> {

	@Override
	protected void reduce(MapOutputKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			IntWritable value = iterator.next();
			context.write(null, new IntWritable(value.get()));
		}
	}
}
