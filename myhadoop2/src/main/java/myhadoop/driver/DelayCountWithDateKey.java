// 7. Driver 구현 
// 앞서 구현한 클래스들을 구동

package myhadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import myhadoop.common.DateKey;
import myhadoop.common.DateKeyComparator;
import myhadoop.common.GroupKeyComparator;
import myhadoop.common.GroupKeyPartitioner;
import myhadoop.mapper.DelayCountMapperWithDateKey;
import myhadoop.reducer.DelayCountReducerWithDateKey;

public class DelayCountWithDateKey extends Configured implements Tool {
	
	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), args);
	    System.out.println("MR-Job Result:" + res);
	  }

	@Override
	public int run(String[] args) throws Exception {
	    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

	    if (otherArgs.length != 2) {
	      System.out.println("Usage: DelayCountWithDateKey <in> <out>");
	      // System.err ? 
	      System.exit(2);
	    }
	    
	    
	    Job job = Job.getInstance(getConf(), "DelayCountWithDateKey");

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.setJarByClass(DelayCountWithDateKey.class);
	    
	    // 그룹키 파티셔너, 그룹키 비교기, 복합키 비교기 => 잡에 등록
	    // Job에 등록 (추가)
	    job.setPartitionerClass(GroupKeyPartitioner.class);
	    job.setGroupingComparatorClass(GroupKeyComparator.class);
	    job.setSortComparatorClass(DateKeyComparator.class);
	    
	    // 추가
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setMapperClass(DelayCountMapperWithDateKey.class);
	    job.setReducerClass(DelayCountReducerWithDateKey.class);
	    
	    // 출력 데이터 포맷에 복합키DateKey와 지연 횟수IntWritable를 설정
	    job.setMapOutputKeyClass(DateKey.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    job.setOutputKeyClass(DateKey.class);
	    job.setOutputValueClass(IntWritable.class);

		// MultipleOutputs.addNamedOutput
		// 여러 개의 OutputCollectors 를 만들고
		// 각 OutputCollectors 에 대한 출력 경로, 출력 포맷, 키와 값 유형을 설정
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);

	    job.waitForCompletion(true);
	    
	    return 0;
	  }
	
	}

