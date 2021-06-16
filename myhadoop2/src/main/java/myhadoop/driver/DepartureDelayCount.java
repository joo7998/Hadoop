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
		// �Ķ���� üũ, �����ϸ� ����
		if (args.length != 2) {
			System.err.println("Usage: DepartureDelayCount <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DepartureDelayCount");
		
		// ������ ���� Ŭ���� ���
		job.setJarByClass(DepartureDelayCount.class);	// ����̹� Ŭ���� ���
		job.setMapperClass(DepartureDelayCountMapper.class);	// ���� Ŭ���� ���
		job.setReducerClass(DelayCountReducer.class);	// ���༭ Ŭ���� ���
		
		// ����� ���� ���
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// ����� Ű�� ���� Ÿ�� ���
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);;
		
		// ����� ��θ� ���
		FileInputFormat.addInputPath(job, new Path(args[0])); // �Է� �ҽ� ���
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// ����
		job.waitForCompletion(true);
	}
}
