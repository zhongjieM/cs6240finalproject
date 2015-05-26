package edu.neu.cs6240.project.test;

import java.math.BigDecimal;
import java.math.MathContext;

public class Test_S3 {
	// private static final String awsAccessKey = "AKIAJ4Y7RNVFRZMMMPFA";
	// private static final String awsSecretAccessKey =
	// "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";
	// private static final String BUCKET_NAME = "kevin017-cs6240";
	// private static final String PREFIX = "pagecounts";

	public static void main(String[] args) {
		// long size = 0;
		// String directory =
		// "s3://kevin017-cs6240/cs6240/project/data/input/raw/";
		// String s3_bucket_name = directory.substring(0, 21);
		// String prefix = directory.substring(21) + PREFIX;
		// AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey,
		// awsSecretAccessKey);
		// AmazonS3 s3Client = new AmazonS3Client(credentials);
		// ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		// listObjectsRequest.withBucketName(BUCKET_NAME);
		// System.out.println(prefix);
		// listObjectsRequest.withPrefix(prefix);
		// ObjectListing objects;
		// do {
		// objects = s3Client.listObjects(listObjectsRequest);
		//
		// for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
		// size += objectSummary.getSize() / (1024 * 1024);
		// System.out.println(s3_bucket_name + objectSummary.getKey());
		// }
		// listObjectsRequest.setMarker(objects.getNextMarker());
		// } while (objects.isTruncated());
		// System.out.println("Total size: " + size);
		BigDecimal b1 = new BigDecimal("15425432543254325432543254325432");
		BigDecimal b2 = new BigDecimal("2421543265436543");
		System.out.println(b1.divide(b2, MathContext.DECIMAL128).toPlainString());
	}
}
