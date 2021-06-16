package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhadoop.support.UsaAirLinePerformence;

// �Է� Ű : LongWritable(���ȣ), �� :(Text) csv ���� year, month..
 // ��� Ű : ��,�� -> (Text), ��: InWritable 1
public class DeptDelayCountMapper 
	extends Mapper<LongWritable, 
						Text, 
						Text,
						IntWritable>{
	// ���� ��� ��
	private final static IntWritable outputValue = new IntWritable(1);
	// �� ��� Ű�� �����ϱ� ���� ��ü
	private Text outputKey = new Text();
	@Override
	protected void map(LongWritable key, // �Է��� �� ��ȣ
			Text value, // �� ���, csv ��Ȳ (,�� ���е� ���ڿ�)
			Context context) // MR�� ��ü ����
			throws IOException, InterruptedException {
		//�� ��� ù��° ���� header�� �ǳ� ����
		if(key.get() == 0 && value.toString().contains("YEAR")) {
			//��� �����̹Ƿ� �ߴ� -> ���༭�� ���޵��� ����
			return;
		}
		
		UsaAirLinePerformence parser = new UsaAirLinePerformence(value);
		
		//����, department_delay > 0 ���� ũ�� ��� Ű:��,�� ��:1 -> ���
		
		if(parser.getDepartureDelayTime() > 0 ) {
			//��� ���� ����
			//��� Ű ����
			outputKey.set(parser.getYear() + "," + parser.getMonth()); // ��) 2010,1
			context.write(outputKey, outputValue);
		}
	}
	

}
