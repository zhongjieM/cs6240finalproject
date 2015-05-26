package edu.neu.cs6240.project.vacabularcounter;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class VCounterDriver extends Configured implements Tool {

	private static final String awsAccessKey = "AKIAJ4Y7RNVFRZMMMPFA";
	private static final String awsSecretAccessKey = "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";
	private static final String BUCKET_NAME = "kevin017-cs6240";
	private static final String PREFIX = "pagecounts-";

	private ArrayList<String> getFilePaths(String directory) {
		// directory format is:
		// "s3://kevin017-cs6240/[other sub paths]/"
		AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey,
				awsSecretAccessKey);
		AmazonS3 s3Client = new AmazonS3Client(credentials);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		listObjectsRequest.withBucketName(BUCKET_NAME);
		listObjectsRequest.withPrefix(directory.substring(21) + PREFIX);
		ObjectListing objects;
		ArrayList<String> filePaths = new ArrayList<String>();
		do {
			objects = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
				filePaths.add(objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objects.getNextMarker());
		} while (objects.isTruncated());
		return filePaths;
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		Job job = new Job(config, "Vacabulary Counter");

		String s3_bucket = args[0].substring(0, 21);
		for (String filePath : getFilePaths(args[0])) {
			System.out.println(s3_bucket + filePath);
			FileInputFormat.addInputPath(job, new Path(s3_bucket + filePath));
		}

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(VCounterDriver.class);
		job.setMapperClass(VCounterMapper.class);
		job.setReducerClass(VCounterReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		boolean result = job.waitForCompletion(true);
		if (result) {
			long counter = job.getCounters().getGroup("COUNTER_GROUP")
					.findCounter("COUNT_VAC").getValue();
			System.out.println(counter);
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new VCounterDriver(),
				args);
		System.exit(result);
	}
}
