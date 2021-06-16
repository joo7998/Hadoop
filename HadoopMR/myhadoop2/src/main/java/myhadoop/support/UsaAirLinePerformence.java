package myhadoop.support;

import org.apache.hadoop.io.Text;

// �װ� ������ csv�� �����ؼ� �ʿ��� �÷� ������ �м��ϴ� Ŭ����
public class UsaAirLinePerformence {
		// ��) 1987,1, ................. -> �� �����Ͽ� Ŭ���� �ʵ忡 ����
		// ����
	private int year;
	private int month;
	
	private String uniqueCarier;
	private float departureDelayTime = 0;
	private float arrivalDelayTime = 0;
	private float distance = 0;
	
	
	//������
	
	public UsaAirLinePerformence(Text line) {  // a, b, c
		// �� ���� csv�� �о �м� �� �ʵ忡 ����
		try {
			String[] columns = line.toString().split(","); // [a, b, c]
			year = Integer.parseInt(columns[0]);  // year
			month = Integer.parseInt(columns[1]); // month
			uniqueCarier = columns[5]; // �װ��� �ڵ� 
			
			if(columns[16].length() != 0) 
				departureDelayTime = Float.parseFloat(columns[16]); // ��� ���� �ð� ����
			
			if(columns[26].length() != 0)
				arrivalDelayTime = Float.parseFloat(columns[26]); // ���� �ð� ����
			
			if(columns[37].length() != 0)
				distance = Float.parseFloat(columns[37]); // ���� �Ÿ�
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public int getYear() {
		return year;
		
		}
	public int getMonth() {
		return month;
	}
	public String getUniqueCarier() {
		return uniqueCarier;
	}
	public float getDepartureDelayTime() {
		return departureDelayTime;
	}
	public float getArrivalDelayTime() {
		return arrivalDelayTime;
	}
	public float getDistance() {
		return distance;
	}
	
	
}
