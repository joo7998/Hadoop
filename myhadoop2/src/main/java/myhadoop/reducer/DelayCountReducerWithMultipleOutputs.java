// Reducer �ܰ迡�� part-r-xxxxx ��� ������ ���� 
// block < 128MB --> part-r-00000 �ϳ��� ���� 

// mapreduce input&output = key, value ���� --> serialize ���� ���·� 
// WritableComparable Interface�� ���� = Writable + Comparable interface ���
// ==> override : ����ȭ write() & ������ȭ readFields() �� compareTo()

package myhadoop.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> mos;
	private Text outputKey = new Text();
	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		String[] columns = key.toString().split(",");
		outputKey.set(columns[1] + "," + columns[2]);
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		if (columns[0].equals("D"))
			mos.write("departure", outputKey, result);
		else
			mos.write("arrival", outputKey, result);
	}

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	// �ݺ� ������ ��� close�� �����ָ� --> �޸𸮿� ��� �����Ǿ� ������ �߻� 
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
	
	
}