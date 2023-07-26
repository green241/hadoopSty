package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URL;

public class HdfsOperationDemo {
    /**
     * 判断路径是否存在
     */
    public static boolean exits(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * 复制文件到指定路径 若路径已存在，则进行覆盖
     */
    public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        // fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖
        fs.copyFromLocalFile(false, true, localPath, remotePath);
        fs.close();
    }

    /**
     * 下载文件到本地 判断本地路径是否已存在，若已存在，则自动进行重命名
     */
    public static void copyToLocal(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        File f = new File(localFilePath);
        // 如果文件名存在，自动重命名(在文件名后面加上 _0, _1 ...)
        if (f.exists()) {
            System.out.println(localFilePath + " 已存在.");
            int i = 0;
            while (true) {
                f = new File(localFilePath + "_" + i);
                if (!f.exists()) {
                    localFilePath = localFilePath + "_" + i;
                    break;
                }
            }
            System.out.println("将重新命名为: " + localFilePath);
        }

        // 下载文件到本地
        Path localPath = new Path(localFilePath);
        fs.copyToLocalFile(remotePath, localPath);
        fs.close();
    }

    /**
     * 追加文件内容
     */
    public static void appendToFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        // 创建一个文件读入流
        FileInputStream in = new FileInputStream(localFilePath);
        // 创建一个文件输出流，输出的内容将追加到文件末尾
        FSDataOutputStream out = fs.append(remotePath);
        // 读写文件内容
        byte[] data = new byte[1024];
        int read = -1;
        while ((read = in.read(data)) > 0) {
            out.write(data, 0, read);
        }
        out.close();
        in.close();
        fs.close();
    }

    /**
     * 测试使用URL来访问hdfs
     */
    public static void useURLConnectHDFS(Configuration conf, String remoteFilePath) throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
        URL url = new URL("hdfs", "hadoop1", 8020, remoteFilePath);

        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        // hadoop伪分布式搭建所在IP
        conf.set("fs.default", "hdfs://hadoop1:8020");
        // conf.set("fs.default", "hdfs://nameservice1");

        // 本地路径
        String localFilePath = "E:\\tmp\\wc.txt";
        // HDFS路径
        String remoteFilePath = "/wc.txt";

        boolean exist = exits(conf, remoteFilePath);

        if (exist) {
            // 如果存在，就尝试读取远端文件
            System.out.println("存在，文件内容: ");
            useURLConnectHDFS(conf, remoteFilePath);
            // 测试hdfs删除、拷贝
            // FileSystem fs = FileSystem.get(conf);
            // boolean delFlag = fs.deleteOnExit(new Path(remoteFilePath));
            // System.out.println("已删除：\t delFlag=" + delFlag);
            // copyToLocal(conf,remoteFilePath,"E:\\tmp\\hdp.txt");
        } else {
            // 如果不存在，就上传本地文件
            copyFromLocalFile(conf, localFilePath, remoteFilePath);
        }

    }
}
