package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// ���༭ : ���� ���� �����͸� �����ϴ� �ܰ�
// reducer Ŭ������ ���
public class WordCountReducer 
	extends Reducer<Text, // ���༭ �Է� Ű Ÿ�� = ������ ��� Ű Ÿ��
					IntWritable, // ���༭ �Է� ���� Ÿ�� = ������ ��� �� Ÿ��
					Text, // ���༭ ��� Ű Ÿ��
					IntWritable>{ //���༭ ��� ���� Ÿ��
	
	// �Է� (word, 1), (word, 1) -> ���÷��� ���� ���� Ű�� ���� ��ü�� ���� Iterable Ÿ������
	//reduce �ż��忡 ���޵ȴ�.
	
	private IntWritable result = new IntWritable();

	
	// ���� �۾��� �����ϴ� �ż���
	@Override
	protected void reduce(Text key, //���ེ �Է�Ű
			Iterable<IntWritable> values, // ���ེ �Է°��� ��ȭ��� ��ü
			Context context) throws IOException, InterruptedException {
			// (word, 1) -> (word, (1, 1, 1, 1, 1))
			int sum = 0;  //�ջ� ����
			for (IntWritable value: values) {
				// ���� �̾Ƽ� �ջ�
				sum += value.get();
			}
			
			result.set(sum); // ��� ����
			context.write(key, result); // (�ܾ�, �������) -> ���
	
	}
	
	

}
