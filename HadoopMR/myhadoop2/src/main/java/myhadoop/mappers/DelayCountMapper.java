package myhadoop.mappers;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import myhadoop.support.UsaAirLinePerformence;

public class DelayCountMapper 
						extends Mapper<LongWritable,
											Text, 
											Text,
											IntWritable>{
	
	// ����� ���� �ɼ� -D�� ���޵Ǵ� workType (departure, arrival)
		private String workType;
		// �� ��� ��
		private final static IntWritable outputValue = new IntWritable(1);
		// �� ��� Ű
		private Text outputkey = new Text();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
			workType = context.getConfiguration().get("workType");
			
	
	}

	@Override
	protected void map(LongWritable key,
							Text value,
							Context context)
			throws IOException, InterruptedException {
	
		if (key.get() == 0 && value.toString().contains("YEAR")){
			// ��� �̹Ƿ� �ߴ�
			return;
			
	}		
			//cvs �ǽ�(�м�)
		   UsaAirLinePerformence parser = new UsaAirLinePerformence(value);
			// �ɼ� workType�� ���� ���� ���� �б�
			// ������ �и�
			if(workType.equals("departure")) {
				// ��� �����ð��� �˻�
				if(parser.getDepartureDelayTime()>0) {
					//���Ű ����
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					//���
					context.write(outputkey, outputValue);
				}
			}else if(workType.equals("arrival")) {
				
				if(parser.getArrivalDelayTime() > 0) {
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputkey, outputValue);
				}
				
			}
	
	}
	
	
}
