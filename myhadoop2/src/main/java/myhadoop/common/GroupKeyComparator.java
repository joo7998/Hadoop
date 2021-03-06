// 4. Comparator 비교기
// GroupKeyPartitioner에서 정한 연도를 비교
package myhadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupKeyComparator extends WritableComparator{
	protected GroupKeyComparator() {
		super(DateKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey)b;
		
		return k1.getYear().compareTo(k2.getYear());
	}
	
	
}
