package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

				// �Ѿ�� Ű D, ��, ��, or A, ��, ���� ���¿� ���� �ٸ� ��� ���
				public class DelayCountReducerMulti 
				extends Reducer<Text,
				IntWritable,
				Text,
				IntWritable>{

			// ���� ����� ���� MultipleOutputs
		
			private MultipleOutputs<Text, IntWritable> mos;
			// ��� Ű
			private Text outputKey =  new Text();
			// ��� ��
			private IntWritable result = new IntWritable();
		
			@Override
			protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				// ���� ����� ���� ��ü �ʱ�ȭ
				mos = new MultipleOutputs<Text, IntWritable>(context);
			}
		
			@Override
			protected void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				// D, ��, ��
				// A, ��, ��
				// output key ��, ��
				// Ű�� �޸��� �и�
				String[] columns = key.toString().split(","); 	// String[]
				// ���� ��� Ű �� ��
				outputKey.set(columns[1] + "," + columns[2]);  // ��, ��
		
				// ������ (D, A)�� �������� ��� ������ ��ȯ
		
				if(columns[0].equals("D")) {
					// ��� ������ departure
					// ���� Ƚ�� �ջ�
					int sum = 0;
					for(IntWritable value: values) {
						sum += value.get();
					}
		
					//��� �� ����
					result.set(sum);
					//��� -> ���� ����� departure
					mos.write("Departure", outputKey, result);
				} else if(columns[0].equals("A")) {
					// ��� ������ Arrival
					int sum = 0;
					for(IntWritable value : values) {
						sum += value.get();		
					}
		
					result.set(sum);
					mos.write("arribal", outputKey, result);
		}
	}


}
