package edu.neu.cs6240.project.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CalculationPartitioner extends
		Partitioner<MapOutputKey, LongWritable> {

	@Override
	public int getPartition(MapOutputKey key, LongWritable value,
			int numOfReducers) {
		return key.getRangeIndex() % numOfReducers;
	}

}
