// 1.복합기 Composite Key
//		복합키를 적용해 연 / 월이 각각 변수로
			
package myhadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

//복합키를 사용하기 위해 WritableComparable 인터페이스를 구현, 
// 					파라미터는 자기 자신인 DateKey로 설정
public class DateKey implements WritableComparable<DateKey>{
	private String year;
	private Integer month;
	
	// 빈 생성자 만들어줘야 new DateKey(다른파일) --> error x 
	public DateKey() {
	}
	
	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}
	
	public String getYear() {
		return year;
	}
	
	public void setYear(String year) {
		this.year = year;
	}
	
	public Integer getMonth() {
		return month;
	}
	
	public void setMonth(Integer month) {
		this.month = month;
	}
	
	// WritableComparable 인터페이스의 메소드로 readFields, write, compareTo 메소드를 구현
	
	// mapreduce input&output = key, value 형태 --> serialize 가능 형태로 
	// WritableComparable Interface = Writable + Comparable interface 상속
	// ==> override : 직렬화 write() & 역직렬화 readFields() 와 compareTo()
	
	//compareTo 메소드는 복합키끼리 순서를 비교할 때 사용하기 위한 메소드로, 미리 만들어 둡
	@Override
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year);
		
		if(result == 0) {
			result = month.compareTo(key.month);
		}	
		return result;		
	}
	
	// write 메소드는 출력 스트림에 연 / 월을 출력
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out,year);
		out.writeInt(month);
	}
	
	// readFields는 입력 스트림에서 연 / 월을 읽어
	@Override
	public void readFields(DataInput in) throws IOException {
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	
	@Override
	public String toString() {
		return new StringBuilder().append(year).append(",").append(month).toString();	
		// StringBuilder 가 메모리에 더 효율적 (문자열 + + 보다) 		
	}
	
	
}