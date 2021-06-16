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


import myhadoop.mappers.DelayCountMapperWithCounter;
import myhadoop.reducer.DelayCountReducer;


// ����� ���� �ɼ��� ����ϴ� ����̹� Ŭ����
// configured�� ���, Tool �������̽��� ������ Ŭ����
// Tool �������̽��� ������ ����̹� Ŭ������ ToolRunner�� �̿� ����
public class DelayCountWithCounter 
				extends Configured 
				implements Tool{
	
				public static void main(String[] args) throws Exception {
					//	Tool �������̽� ����
					int res  = ToolRunner.run(new Configuration(), new DelayCountWithCounter(), args);
				}
				// ���� Tool �������̽��� ���� �ؾ� �� ����
				@Override
				public int run(String[] args) throws Exception{
					// ����� ���� �ɼ��� ó��
					String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
					
					// ����� ��� Ȯ��
					if (otherArgs.length != 2) {
						System.err.println("Usage : DelayCountWithCounter <input> <output>");
						System.exit(2);
					}
					
					
					// �� ����
					Job job = Job.getInstance(getConf(), "DelayCountWithCounter");
					
					//����� ��� ����
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
					
					
					// �� Ŭ���� ����
					job.setJarByClass(DelayCountWithCounter.class);
					//���� Ŭ���� ���
					job.setMapperClass(DelayCountMapperWithCounter.class);
					//���ེ Ŭ���� ���
					job.setReducerClass(DelayCountReducer.class);
					
					//����� ������ ����
					job.setInputFormatClass(TextInputFormat.class);
					job.setOutputFormatClass(TextOutputFormat.class);
					
					// ��� Ű/�� ����
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					// MR ����
					job.waitForCompletion(true);
					return 0;
				}
}
