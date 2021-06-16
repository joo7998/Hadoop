package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DeptDelayCountMapper;
import myhadoop.reducer.DelayCountReducer;

// DeptDelayCount <input> <output>
public class DeptDelayCount {

	public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.err.println("Ussage : DeptDelayCount <input> <output>");
			System.exit(2);
	}
		Configuration conf = new Configuration();
		//output ��ΰ� �̹� ������ ��������
		FileSystem hdfs = FileSystem.get(conf);
		// output ��� Ȯ��
		Path outPath = new Path(args[1]); // ��� ���
		if (hdfs.exists(outPath)) {
			//������ ������
			hdfs.delete(outPath, true);
		}
		
		// job ����
		
		Job job = Job.getInstance(conf, "DeptDelayCount");
		
		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		
		//���� Ŭ���� ���
		job.setJarByClass(DeptDelayCount.class);
		//���� Ŭ���� ����
		job.setMapperClass(DeptDelayCountMapper.class);
		
		// ���༭ Ŭ���� ���
		job.setReducerClass(DelayCountReducer.class);
		
		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// ��� Ű, ��� �� Ÿ�� ����
		job.setOutputKeyClass(Text.class);  // zl: 1987,1 
		job.setOutputValueClass(IntWritable.class); // ��: ���� ��ġ 
		
		// ���� 
		job.waitForCompletion(true);;
	}
}