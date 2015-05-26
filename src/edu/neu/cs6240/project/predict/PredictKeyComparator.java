package edu.neu.cs6240.project.predict;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PredictKeyComparator extends WritableComparator {

	protected PredictKeyComparator() {
		super(MapOutputKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MapOutputKey key1 = (MapOutputKey) a;
		MapOutputKey key2 = (MapOutputKey) b;
		return key1.compareTo(key2);
	}
}
