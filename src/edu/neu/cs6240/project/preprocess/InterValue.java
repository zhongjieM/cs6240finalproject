package edu.neu.cs6240.project.preprocess;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InterValue implements Writable {
	private long maxViews;
	private String longestWord;
	private long longestWordLength;

	public InterValue() {
	}

	public InterValue(long maxViews, String longestWord, long longestWordLength) {
		this.maxViews = maxViews;
		this.longestWord = longestWord;
		this.longestWordLength = longestWordLength;
	}

	public long getMaxViews() {
		return maxViews;
	}

	public void setMaxViews(long maxViews) {
		this.maxViews = maxViews;
	}

	public String getLongestWord() {
		return longestWord;
	}

	public void setLongestWord(String longestWord) {
		this.longestWord = longestWord;
	}

	public long getLongestWordLength() {
		return longestWordLength;
	}

	public void setLongestWordLength(long longestWordLength) {
		this.longestWordLength = longestWordLength;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		maxViews = input.readLong();
		longestWord = input.readUTF();
		longestWordLength = input.readLong();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(maxViews);
		output.writeUTF(longestWord);
		output.writeLong(longestWordLength);
	}

}
