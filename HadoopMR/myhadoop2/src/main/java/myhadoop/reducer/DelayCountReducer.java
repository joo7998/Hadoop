package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// �Է� : 1987,1->Ű�� 1-> ��
// ��� : 1987,1 -> Ű, �Է°��� ���� ��� -> ��
public class DelayCountReducer 
extends Reducer<Text,  // ���༭ �Է� Ű�� Ÿ��
				IntWritable,  // ���༭ �Է� ���� Ÿ��
				Text, //���༭ ��� Ű�� Ÿ��
				IntWritable>{ // ���༭ ��� ���� Ÿ��
	
	// ���༭ ��� ���� ��ü
	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text Key, // Ű���� ����� ������ Ÿ��
			Iterable<IntWritable> values, // �Է� ����  ��ȭ ��ü
			Context context) throws IOException, InterruptedException {
		// values�� �ִ� ��� ���� �ջ� - > ����� ���
		// �Է� Ű�� ��� Ű�� ������ ��Ȱ��
		int sum = 0;
		
		// ����
		for (IntWritable value : values) {
			sum += value.get();
		}
		// ��� ��ü�� ����
		result.set(sum);
		// ���
		context.write(Key, result);
	}
	
	
	

}
