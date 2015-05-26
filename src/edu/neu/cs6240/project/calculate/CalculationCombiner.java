package edu.neu.cs6240.project.calculate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculationCombiner extends
		Reducer<MapOutputKey, LongWritable, MapOutputKey, LongWritable> {

	@Override
	protected void reduce(MapOutputKey key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		int rangeIndex = key.getRangeIndex();
		HashMap<String, Long> cache = new HashMap<String, Long>();
		Iterator<LongWritable> iterator = values.iterator();

		while (iterator.hasNext()) {
			LongWritable value = iterator.next();
			long views = 0;
			if (cache.containsKey(key.getWord())) {
				views = cache.get(key.getWord());
			}
			views += value.get();
			cache.put(key.getWord(), views);
		}

		for (Entry<String, Long> entry : cache.entrySet())
			context.write(new MapOutputKey(rangeIndex, entry.getKey()),
					new LongWritable(entry.getValue()));
	}
}
