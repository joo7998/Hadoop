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

import myhadoop.mapper.ArrivalDelayCountMapper;
import myhadoop.reducer.ArrivalCountReducer;

public class DelayCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCount(), args);
		System.out.println("MR-Job Result: " + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// GenericOptionParser���� �����ϴ� parameter���� ������ parameter ��������
		String[] otherArgs = new GenericOptionsParser (getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("Usage: DelayCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(), "DelayCount");
		
		// ����� ��θ� ��� (input, output dir)
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // �Է� �ҽ� ���
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// ������ ���� Ŭ���� ���
		job.setJarByClass(ArrivalDelayCount.class);	// ����̹� Ŭ���� ���
		job.setMapperClass(ArrivalDelayCountMapper.class);	// ���� Ŭ���� ���
		job.setReducerClass(ArrivalCountReducer.class);	// ���༭ Ŭ���� ���
				
		// ����� ���� ���
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		// ����� Ű�� ���� Ÿ�� ���
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);;
				
		
				
		// ����
		job.waitForCompletion(true);
		
		return 0;
		
}

}
