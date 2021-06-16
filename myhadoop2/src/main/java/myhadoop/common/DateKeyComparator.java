// 2.���ձ� �񱳱� Comparator
//  Composite Key(common-DateKey)�� compareTo �޼ҵ带 �̿��� ����Ű���� ������ ���ϴ� compare �޼ҵ带 ����

package myhadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
	protected DateKeyComparator() {
		super(DateKey.class, true);
		
	}
	
	
	// Composite Key(DateKey)���� ������ 
	// �� ���� 1������ ��������, 2������ ������
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;	
		// 2 ���� DateKey ��ü�� ������ 
		// bMonth ���߿�
		
		//DateKey�� compareTo
		int cmp = k1.getYear().compareTo(k2.getYear());	 
		if (cmp != 0) {
			return cmp;
		}
		
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
		// = 0 , �ƴϸ� -1 
	}

}
