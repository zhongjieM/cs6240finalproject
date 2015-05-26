package edu.neu.cs6240.project.calculate;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CalculationGrouper extends WritableComparator {

	protected CalculationGrouper() {
		super(MapOutputKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MapOutputKey key1 = (MapOutputKey) a;
		MapOutputKey key2 = (MapOutputKey) b;
		return key1.groupCompareTo(key2);
	}

}
