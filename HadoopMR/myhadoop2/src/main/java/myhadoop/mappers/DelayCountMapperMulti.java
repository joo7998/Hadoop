package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.UsaAirLinePerformence;

public class DelayCountMapperMulti 
						extends Mapper<LongWritable,
											Text, 
											Text, 
											IntWritable>{
		// ��� ��
	private final static IntWritable outputValue = new IntWritable(1);
		//���  Ű
	private Text outputKey =  new Text();
	@Override
	protected void map(LongWritable key,
							Text value,
							Context context)
			throws IOException, InterruptedException {
		UsaAirLinePerformence parser =  new UsaAirLinePerformence(value);
		
		// ��� ���� �ð��� > 0 -> Ű�� D, ��, ��
		
		if (parser.getDepartureDelayTime() > 0 ) {
			// ��� Ű(������� ���� �߰�)
			outputKey.set("D," + parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		}
		// ���� ���� �ð� > 0 ->  Ű�� A,��,��
		if(parser.getArrivalDelayTime() > 0) {
			// ��� Ű (�������� ���� �߰�)\
			outputKey.set("A," + parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		}
		// ��� 1: D,�⵵,�� - > ���༭���� ��� ���� MultipleOutputs�� ���
		// ��� 2: A,�⵵,�� - > ���༭���� ���� ���� MultipleOutputs�� ���
	}

}
