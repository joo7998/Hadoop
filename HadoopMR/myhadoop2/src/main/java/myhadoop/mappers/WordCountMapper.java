package myhadoop.mappers;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//���۴� Mapper Ŭ�󽺸� ��� �޾ƾ���

public class WordCountMapper extends Mapper<LongWritable, //�Է�Ű�� Ÿ��(�������� ���� �ѹ�)
												Text, // �Է� ���� Ÿ��
												Text, // ��� Ű�� Ÿ��
												IntWritable> { // ����� ���� Ÿ��
		
		// I am going to home
		// - > k: 0, v: I am going to home
		//���:(I,1) (am, 1),(going, 1), (home, 1)
		private final static IntWritable outputValue = new IntWritable(1);
		private Text word = new Text(); // ����� Ű�� ����� ��ü
		
		//���� ���� �۾��� ������ �ż���
		@Override
		protected void map(LongWritable key,
				Text value,
				Context context)
				throws IOException, InterruptedException{
			// k:0,  v : I am going to home
			// value ����
			StringTokenizer st = new StringTokenizer(value.toString()); //�Է°��� ���� ���ڷ� ����
			// ������ ���(�ܾ�, 1)
			while(st.hasMoreElements()) {
				word.set(st.nextToken()); // Ű�� ����� �ܾ�
				//  context �� ���
				
				context.write(word, outputValue); // ���(�ܾ�, 1)
			}
		}
			
		


}
