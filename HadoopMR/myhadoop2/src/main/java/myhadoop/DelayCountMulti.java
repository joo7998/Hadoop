package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import myhadoop.mappers.DelayCountMapperMulti;
import myhadoop.reducer.DelayCountReducerMulti;

public class DelayCountMulti {

	public static void main(String[] args) throws Exception {
		// �Ű����� Ȯ��
		if(args.length !=2) {
			System.err.println("Usage: DelayCountMulti <input> <output>");
			System.exit(2);
		}
		
		
		// ���� ���� �ʿ�
		Configuration conf = new Configuration();
		
		// Job����
		Job job = Job.getInstance(conf, "DelayCountMulti");
		
		// ����� ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// �� Ŭ����, ����, ���༭, ���
		job.setJarByClass(DelayCountMulti.class);
		job.setMapperClass(DelayCountMapperMulti.class);
		job.setReducerClass(DelayCountReducerMulti.class);
		
		//	����� ������ ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// ��� Ű, �� Ÿ�� ����
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//	��ɵ� ��� ��θ� ���
		MultipleOutputs.addNamedOutput(job, 
								"departure",  //��� ����� �̸�
							TextOutputFormat.class, //  ����� ����
							Text.class,
							IntWritable.class);
			MultipleOutputs.addNamedOutput(job, 
										"arrival",
										TextOutputFormat.class,
										Text.class,
										IntWritable.class);
			
			//����
			job.waitForCompletion(true);

}
}