// 5. 복합기(Composite Key/DateKey)를 통한 Mapper 구현 


package myhadoop.mapper;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.common.AirlinePerformanceParser;
import myhadoop.common.DateKey;
import myhadoop.counter.DelayCounters;

//데이터 타입을 DateKey로 바꿔
public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1);
	private DateKey outputKey = new DateKey();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				// 출발 지연과 도착 지연을 구분하기 위해 연도 앞에 D(Departure)와 A(Arrive)를 붙
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
				// 출발 지연과 도착 지연을 구분 연도 앞에 A(Arrive) 붙여
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