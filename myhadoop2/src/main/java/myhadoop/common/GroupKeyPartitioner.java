// 3. Partitioner 
// Map �½�ũ�� ��� ������--> Reduce �׽�ũ�� �Է� �����ͷ� ������ ����
// partitioning �����ʹ� Map �½�ũ�� ��� ������ key ���� ���� ����
// ���⼭�� year�� �׷�Ű�� ����ϹǷ�, getPartition �޼���� ������ ���� hashCode�� ��ȸ�� numPartitions�� ����

package myhadoop.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable>{

	@Override
	public int getPartition(DateKey key, IntWritable value, int numPartitions) {
		// hashcode() = ���� ��ü�ΰ� Ȯ��
		// equals() == ������ ������
		int hash = key.getYear().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}
	
	

}
