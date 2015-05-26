package edu.neu.cs6240.project.preprocess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreProcessReducer extends
		Reducer<NullWritable, InterValue, NullWritable, Text> {

	@Override
	protected void reduce(NullWritable arg0, Iterable<InterValue> values,
			Context context) throws IOException, InterruptedException {
		long maxViews = 0;
		String longestWord = "";
		long longestWordLength = 0;

		Iterator<InterValue> iterator = values.iterator();
		while (iterator.hasNext()) {
			InterValue value = iterator.next();
			maxViews = Math.max(value.getMaxViews(), maxViews);
			if (longestWordLength < value.getLongestWordLength()) {
				longestWord = value.getLongestWord();
				longestWordLength = value.getLongestWordLength();
			}
		}

		context.write(NullWritable.get(), new Text(maxViews + " " + longestWord
				+ " " + longestWordLength));
	}
}
