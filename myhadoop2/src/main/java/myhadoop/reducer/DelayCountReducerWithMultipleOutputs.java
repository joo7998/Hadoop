// Reducer 단계에서 part-r-xxxxx 출력 데이터 생성 
// block < 128MB --> part-r-00000 하나만 생성 

// mapreduce input&output = key, value 형태 --> serialize 가능 형태로 
// WritableComparable Interface를 구현 = Writable + Comparable interface 상속
// ==> override : 직렬화 write() & 역직렬화 readFields() 와 compareTo()

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
	
	// 반복 실행의 경우 close를 안해주면 --> 메모리에 계속 생성되어 누수가 발생 
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
	
	
}