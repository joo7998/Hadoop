// 3. Partitioner 
// Map 태스크의 출력 데이터--> Reduce 테스크의 입력 데이터로 보낼지 결정
// partitioning 데이터는 Map 태스크의 출력 데이터 key 값에 따라 정렬
// 여기서는 year를 그룹키로 사용하므로, getPartition 메서드는 연도에 대한 hashCode를 조회해 numPartitions를 생성

package myhadoop.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable>{

	@Override
	public int getPartition(DateKey key, IntWritable value, int numPartitions) {
		// hashcode() = 같은 객체인가 확인
		// equals() == 내용이 같은지
		int hash = key.getYear().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}
	
	

}
