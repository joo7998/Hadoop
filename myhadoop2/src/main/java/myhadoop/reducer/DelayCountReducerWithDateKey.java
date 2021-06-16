// 6. Reducer ����
// Mapper(DelayCountMapperWithDateKey) ��� ������ --> ������ ���� Ƚ���� �ջ�


package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import myhadoop.common.DateKey;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {
	// ������ Ÿ���� DateKey�� �ٲ�
	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();

	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}

	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] columns = key.getYear().split(",");
		
		int sum = 0;     
		Integer bMonth = key.getMonth();	
		
		if (columns[0].equals("D")) {
			// ������ ����Ƚ�� �ջ� --> iterable ��ü�� ��ȸ 
			
			//����� ���� ���� �������� ���� ��ġ���� ������ 
			//����� ���� Ƚ���� ��� & �ջ� ������ 0���� �ʱ�ȭ
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(key.getYear().substring(2));
					outputKey.setMonth(bMonth);
					mos.write("departure", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			//Iterable ��ü�� ��� ��ȸ�ǰ� �� �����Ͱ� ��ġ--> �ջ� �� ���
			if (key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				result.set(sum);
				mos.write("departure", outputKey, result);
			}
			
		} else {
			//����� ���� ���� �������� ���� ��ġ���� ������ 
			//����� ���� Ƚ���� ��� --> �ջ� ������ 0���� �ʱ�ȭ
			for (IntWritable value : values) {
		        if (bMonth != key.getMonth()) {
		          result.set(sum);
		          outputKey.setYear(key.getYear().substring(2));
		          outputKey.setMonth(bMonth);
		          mos.write("arrival", outputKey, result);
		          sum = 0;
		        }
		        sum += value.get();
		        bMonth = key.getMonth();
		      }
			//Iterable ��ü�� ��� ��ȸ�ǰ� �� �����Ͱ� ��ġ-> �ջ� �� ���
		      if (key.getMonth() == bMonth) {
		        outputKey.setYear(key.getYear().substring(2));
		        outputKey.setMonth(key.getMonth());
		        result.set(sum);
		        mos.write("arrival", outputKey, result);
		      }
		    }
		  }

		  @Override
		  public void cleanup(Context context) throws IOException,
		    InterruptedException {
		    mos.close();
		  }
		}