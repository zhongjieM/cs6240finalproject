package edu.neu.cs6240.project.populate;

import java.nio.ByteBuffer;

public class RowKey {
	// 4 bytes represents the range index, 10 bytes represents the word length
	private static final int KEY_LENGTH = 14;
	private static final int MAX_WORD_LENGTH = 10;
	private int rangeIndex = 0;
	private String word = "";

	public RowKey(int rangeIndex, String word) {
		this.rangeIndex = rangeIndex;
		StringBuilder sb = new StringBuilder();
		sb.append(word);
		for (int i = 0; i < MAX_WORD_LENGTH - word.length(); i++) {
			sb.append(" ");
		}
		this.word = sb.toString();
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

	public byte[] buildKey() {
		ByteBuffer bbuf = ByteBuffer.allocate(KEY_LENGTH);
		bbuf.putInt(rangeIndex);
		bbuf.put(word.getBytes());
		return bbuf.array();
	}
}
