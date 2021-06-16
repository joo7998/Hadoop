// 6. Reducer 구현
// Mapper(DelayCountMapperWithDateKey) 출력 데이터 --> 월별로 지연 횟수를 합산


package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import myhadoop.common.DateKey;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {
	// 데이터 타입을 DateKey로 바꿔
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
			// 월별로 지연횟수 합산 --> iterable 객체가 순회 
			
			//백업된 월과 현재 데이터의 월이 일치하지 않으면 
			//백업된 지연 횟수를 출력 & 합산 변수를 0으로 초기화
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
			//Iterable 객체가 모두 순회되고 월 데이터가 일치--> 합산 값 출력
			if (key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				result.set(sum);
				mos.write("departure", outputKey, result);
			}
			
		} else {
			//백업된 월과 현재 데이터의 월이 일치하지 않으면 
			//백업된 지연 횟수를 출력 --> 합산 변수를 0으로 초기화
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
			//Iterable 객체가 모두 순회되고 월 데이터가 일치-> 합산 값 출력
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