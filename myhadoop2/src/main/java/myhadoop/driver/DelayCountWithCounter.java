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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import myhadoop.mapper.DelayCountMapperWithCounter;
import myhadoop.reducer.DelayCountReducerWithCounter;

public class DelayCountWithCounter extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCountWithCounter(), args);
		System.out.println("MR-Job Result: " + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// GenericOptionParser에서 제공하는 parameter 제외 나머지 parameter 가져오기
		String[] otherArgs = new GenericOptionsParser (getConf(), args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.out.println("Usage: DelayCountWithCounter <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(), "DelayCountWithCounter");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 입력 소스 경로
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// 실행을 위한 클래스 등록
		job.setJarByClass(DelayCountWithCounter.class);	// 드라이버 클래스 명시
		job.setMapperClass(DelayCountMapperWithCounter.class);	// 매퍼 클래스 등록
		job.setReducerClass(DelayCountReducerWithCounter.class);	// 리듀서 클래스 등록
				
		// 입출력 포맷 등록
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		// 출력의 키와 값의 타입 등록
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);;
					
		// 수행
		job.waitForCompletion(true);
		
		return 0;
		
}

}
