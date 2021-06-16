// 5. ���ձ�(Composite Key/DateKey)�� ���� Mapper ���� 


package myhadoop.mapper;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.common.AirlinePerformanceParser;
import myhadoop.common.DateKey;
import myhadoop.counter.DelayCounters;

//������ Ÿ���� DateKey�� �ٲ�
public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1);
	private DateKey outputKey = new DateKey();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				// ��� ������ ���� ������ �����ϱ� ���� ���� �տ� D(Departure)�� A(Arrive)�� ��
				outputKey.setYear("D," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
				
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
				
			} else if (parser.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
			
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
		
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				// ��� ������ ���� ������ ���� ���� �տ� A(Arrive) �ٿ�
				outputKey.setYear("A," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
				
			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
			
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}
}