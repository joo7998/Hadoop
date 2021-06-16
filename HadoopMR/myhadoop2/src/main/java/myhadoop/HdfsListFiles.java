package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

//���� : HdfsListFiles
public class HdfsListFiles {

	public static void main(String[] args) {
		//������ �Ű������� ������ ����
		if(args.length != 1) {
			System.err.println("Usage : HdfsListFiles <dir>");
			System.exit(2);
		}
		// ���� �ҷ��ͼ� ���� �ý��� ����
		Configuration conf = new Configuration();
		//1��° �Ű��������� ������ dir Ȯ��
		String dir = args[0];
		
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path path = new Path(dir);
			
			RemoteIterator<LocatedFileStatus> iter = hdfs.listFiles(path, true);
			
			
			// Iterator�� ��ȯ�ϸ鼭 ���� ��ü�� ����
			while(iter.hasNext()) {
				LocatedFileStatus status = iter.next();
				System.out.printf("%s, %s%n", status.isDirectory() ? "Directory" : "Files",
					status.getPath());
			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
