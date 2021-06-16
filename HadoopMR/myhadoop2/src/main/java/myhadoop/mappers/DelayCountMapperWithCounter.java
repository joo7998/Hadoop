package myhadoop.mappers;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import myhadoop.support.DelayCounter;
import myhadoop.support.UsaAirLinePerformence;


public class DelayCountMapperWithCounter 
						extends Mapper<LongWritable,
											Text, 
											Text,
											IntWritable>{
	// ����� �ɼ� workType�� üũ
	private String workType;
	// (1987,1 1)
	private final static IntWritable outputValue = new IntWritable(1);
	//��� Ű
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	//����� �ɼ� workType�� ����
		workType =  context.getConfiguration().get("workType");
	}
	
	
	@Override
	protected void map(LongWritable key, 
								Text value, 
								Context context)
			throws IOException, InterruptedException {
		// ù��° ������ ����� ���ɼ�
		if (key.get() == 0 && value.toString().contains("YEAR")) {
			return;
		}
	
		UsaAirLinePerformence parser = new UsaAirLinePerformence(value);
			
	// workType �� ���� ���� �帧�� �б�
		if(workType.equals("departure")) {
			// ��� ���� ���� ����
			
			if(parser.getDepartureDelayTime() > 0 ) { //��� ����
				// ���
				outputKey.set(parser.getYear() + "," + parser.getMonth());
				context.write(outputKey, outputValue);
			} else if (parser.getDepartureDelayTime() == 0) { //���� ���
				// ���� ��� ī���� ����
				context.getCounter(DelayCounter.scheduled_departure).increment(1);
			} else if (parser.getDepartureDelayTime() < 0) {	//���� ���
				// ���� ��� ī���� 1 ����
				context.getCounter(DelayCounter.early_departure).increment(1);
			}
		}else if (workType.equals("arrival")) {
			// ���� ���� ���� ����
			if (parser.getArrivalDelayTime() > 0 ) { 
				// ���
				outputKey.set(parser.getYear()+ "," + parser.getMonth());
				context.write(outputKey, outputValue);
			}else if (parser.getArrivalDelayTime() == 0) {
				// ���� ���� ī���� ����
				context.getCounter(DelayCounter.scheduled_arrival).increment(1);
			} else if (parser.getArrivalDelayTime() < 0) {
				// ���� ���� ī���� ����
			}
		}

	}
}
