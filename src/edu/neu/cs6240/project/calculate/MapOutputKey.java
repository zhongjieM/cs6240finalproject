package edu.neu.cs6240.project.calculate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MapOutputKey implements WritableComparable<MapOutputKey> {
	private int rangeIndex;
	private String word;

	public MapOutputKey() {
	}

	public MapOutputKey(int rangeIndex, String word) {
		this.rangeIndex = rangeIndex;
		this.word = word;
	}

	public int getRangeIndex() {
		return rangeIndex;
	}

	public void setRangeIndex(int rangeIndex) {
		this.rangeIndex = rangeIndex;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@Override
	public int compareTo(MapOutputKey key2) {
		if (rangeIndex != key2.rangeIndex)
			return Integer.compare(rangeIndex, key2.rangeIndex);
		if (word.equals(key2.word))
			return 0;
		if (word.equals("dummy"))
			return -1;
		if (key2.word.equals("dummy"))
			return 1;
		return word.compareTo(key2.word);
	}

	public int groupCompareTo(MapOutputKey key2) {
		return Integer.compare(rangeIndex, key2.rangeIndex);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		rangeIndex = input.readInt();
		word = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(rangeIndex);
		output.writeUTF(word);
	}

}
