package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//����: HdfsWriteMessage<filename> <message>
public class HdfsWriteMessage {

		public static void main(String[] args) {
			//�Է� �Ű� ������ ����
			if (args.length !=2) {
				System.out.println("Usage: HdfsWriteMessage<filename><message>");
				System.exit(2);
			}
			try {
				//HDFS ���� �ý��� ��ü ������~
				Configuration conf = new Configuration(); //  ���� ������ �ϵӼ����� �о�´�.
				FileSystem hdfs = FileSystem.get(conf);
				
				String target = args[0]; // ���ϸ�
				String message = args[1]; //������ �޼���
				
				//�̹� target ������ ������ ��������
				Path path = new Path(target); // ������ ����, ���͸��� ��θ� ��� �ִ� ��ü
				if(hdfs.exists(path)) {
					// �����ϸ� ������
					hdfs.delete(path, false); // delete(path, boolean recursive)
				}
				
				//���� ������ ���� ��� ��Ʈ���� Ȯ��
				FSDataOutputStream os = hdfs.create(path);
				os.writeUTF(message);
				os.close();
				
				System.out.println("�޼����� ���� �Ǿ����ϴ�.");
					
		} catch(IOException e) {
			e.printStackTrace();
		}
				
			
		}
		
}