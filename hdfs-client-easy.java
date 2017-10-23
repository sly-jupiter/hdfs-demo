package cn.itheima.bigdata.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientEasy {

	private FileSystem fs = null;

	@Before
	public void getFs() throws Exception {

		// 拿到一个配置参数的封装对象，构造函数中就会对classpath下的xxx-site.xml文件进行解析
		// 真实的项目工程中就应该把xxx-site.xml文件加入到工程中来
		Configuration conf = new Configuration();
		// to set a parameter, figure out the filesystem is hdfs
		conf.set("fs.defaultFS", "hdfs://yun12-01:9000/");
		conf.set("dfs.replication", "1");

		// 获取到一个具体文件系统的客户端实例对象，产生的实例究竟是哪一种文件系统的客户端，是根据conf中的相关参数来决定
//		fs = FileSystem.get(conf);
		
		//这种获取fs的方法可以指定访问hdfs的客户端身份
		fs = FileSystem.get(new URI("hdfs://yun12-01:9000/"), conf, "hadoop");

	}

	/**
	 * 上传文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {

		fs.copyFromLocalFile(new Path("c:/jdk.win"), new Path("/"));

	}

	/**
	 * 删除文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRmfile() throws IllegalArgumentException, IOException {

		boolean res = fs.delete(new Path("/aa/bb"), true);

		System.out.println(res ? "delete is successfully :)"
				: "it is failed :(");

	}

	/**
	 * 创建文件夹
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {

		fs.mkdirs(new Path("/aa/bb"));

	}

	/**
	 * 重命名文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException {

		fs.rename(new Path("/jdk.tgz"), new Path("/jdk.tgz.rename"));

	}

	/**
	 * 列出目录下的文件信息
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException,
			IllegalArgumentException, IOException {

		// 递归列出文件
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(
				new Path("/"), true);

		while (listFiles.hasNext()) {

			LocatedFileStatus file = listFiles.next();

			System.out.println(file.getPath().getName());

		}

		System.out.println("--------------------------------------------");

		// 列出文件及文件夹
		FileStatus[] status = fs.listStatus(new Path("/"));
		for (FileStatus file : status) {

			System.out.println(file.getPath().getName() + "   "
					+ (file.isDirectory() ? "d" : "f"));

		}

	}

	/**
	 * 从hdfs中下载数据到本地
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException {

		fs.copyToLocalFile(new Path("/jdk.tgz.rename"), new Path("c:/jdk.win"));

	}

}
