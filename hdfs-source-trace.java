package cn.itheima.bigdata.hadoop.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsSourceTrace {

	public static void main(String[] args) throws Exception {
		//怎样决定创建哪一种具体文件系统的客户端实例对象？
		//根据配置文件中相应参数的配置值，根据所配置的文件系统uri，来获取到具体文件系统的客户端类class，饭后反射出实例
		//接着为fs实例中的成员初始化赋值
		//hdfs的客户端实例对象应该具备哪些成员？
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://yun12-01:9000/");
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream is = fs.open(new Path("/jdk.tgz.rename"));
		
		FileOutputStream os = new FileOutputStream("d:/jdk.tgz.rename");
		
		IOUtils.copy(is, os);
		
	}
	
	
}
