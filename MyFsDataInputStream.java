import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
public class MyFsDataInputStream extends FSDataInputStream{
    public MyFsDataInputStream(InputStream in) {
        super(in);
    }
    public static String readline(Configuration conf,String filename) throws IOException
    {
        Path filename1=new Path(filename);
        FileSystem fs=FileSystem.get(conf);
        FSDataInputStream in=fs.open(filename1);
        BufferedReader d=new BufferedReader(new InputStreamReader(in));
        String line=d.readLine();
        if (line!=null) {
            d.close();
            in.close();
            return line;
        }
        else {
            return null;
        }
    }
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.80.133:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs=FileSystem.get(conf);
        String filename="/user/hadoop/hello.txt";
        System.out.println("读取文件："+filename);
        String o=MyFsDataInputStream.readline(conf, filename);
        System.out.println(o+"\n"+"读取完成");
    }
}
