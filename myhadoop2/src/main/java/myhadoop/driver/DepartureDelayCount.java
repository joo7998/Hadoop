package myhadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mapper.DepartureDelayCountMapper;
import myhadoop.reducer.DelayCountReducer;

public class DepartureDelayCount {
	public static void main(String[] args) throws Exception {
		// 파라미터 체크, 부족하면 종료
		if (args.length != 2) {
			System.err.println("Usage: DepartureDelayCount <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DepartureDelayCount");
		
		// 실행을 위한 클래스 등록
		job.setJarByClass(DepartureDelayCount.class);	// 드라이버 클래스 명시
		job.setMapperClass(DepartureDelayCountMapper.class);	// 매퍼 클래스 등록
		job.setReducerClass(DelayCountReducer.class);	// 리듀서 클래스 등록
		
		// 입출력 포맷 등록
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 출력의 키와 값의 타입 등록
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);;
		
		// 입출력 경로를 등록
		FileInputFormat.addInputPath(job, new Path(args[0])); // 입력 소스 경로
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 수행
		job.waitForCompletion(true);
	}
}
