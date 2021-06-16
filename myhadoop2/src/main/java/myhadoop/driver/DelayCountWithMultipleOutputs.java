package myhadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// 여러 개의 출력 데이터를 쉽게 생성 --> 기능 제공 
// 여러 개의 OutputCollectors 를 만들고 각 OutputCollectors 에 대한 출력 경로, 출력 포맷, 키와 값 유형을 설정
// ==> addNamedOutput
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import myhadoop.mapper.DelayCountMapperWithMultipleOutputs;
import myhadoop.reducer.DelayCountReducerWithMultipleOutputs;


public class DelayCountWithMultipleOutputs extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new DelayCountWithMultipleOutputs(), args);
		System.out.println("MR-Job Result: " + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("Usage: DelayCountWithMultipleOutputs <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(), "DelayCountWithMultipleOutputs");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(DelayCountWithMultipleOutputs.class);
		job.setMapperClass(DelayCountMapperWithMultipleOutputs.class);
		job.setReducerClass(DelayCountReducerWithMultipleOutputs.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// MultipleOutputs.addNamedOutput
		// 여러 개의 OutputCollectors 를 만들고 
		// 각 OutputCollectors 에 대한 출력 경로, 출력 포맷, 키와 값 유형을 설정
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		
		job.waitForCompletion(true);
		
		return 0;
	}
}