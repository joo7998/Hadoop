package myhadoop;

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
import myhadoop.mappers.DeptDelayCountMapper;
import myhadoop.reducer.DelayCountReducer;

public class DelayCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		// Tool �������̽� ������ ���ؼ��� ToolRunner Ŭ������ ���
		
		int res = ToolRunner.run(new Configuration(), new DelayCount(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
			// GenericOptionsParser�� �̿� �ɼ� ó�� �� �����ִ� �Ű������� ȹ��
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		// ����� ��� Ȯ��
		if (otherArgs.length != 2) {
			System.err.println("Usage : DelayCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(),"DelayCount");
		//����� ���
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Ŭ���� ���
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DeptDelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		// ����� ������ ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// ��� Ű, �� Ÿ�� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// MR ����
		job.waitForCompletion(true);
		return 0;
	}

}