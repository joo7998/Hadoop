package myhadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.common.AirlinePerformanceParser;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		outputKey.set(parser.getYear() + "," + parser.getMonth());  //"2012,01"
		
		if(workType.equals("departure")) {	
			if (parser.getDepartureDelayTime() > 0) {	
				context.write(outputKey, outputValue);
			}
		} else if(workType.equals("arrival")) {
			 if (parser.getDepartureDelayTime() > 0) {	
				context.write(outputKey, outputValue);
		}
	}


}
}

	
