// 2.복합기 비교기 Comparator
//  Composite Key(common-DateKey)의 compareTo 메소드를 이용해 복합키끼리 순서를 비교하는 compare 메소드를 구현

package myhadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
	protected DateKeyComparator() {
		super(DateKey.class, true);
		
	}
	
	
	// Composite Key(DateKey)끼리 순서비교 
	// 비교 기준 1순위는 ‘연도’, 2순위는 ‘월’
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;	
		// 2 개의 DateKey 객체가 들어오면 
		// bMonth 나중에
		
		//DateKey의 compareTo
		int cmp = k1.getYear().compareTo(k2.getYear());	 
		if (cmp != 0) {
			return cmp;
		}
		
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
		// = 0 , 아니면 -1 
	}

}
