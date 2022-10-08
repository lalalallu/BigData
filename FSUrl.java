

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
public class FSUrl {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void cat(String filename) throws MalformedURLException, IOException
    {
        InputStream in=new URL("hdfs","192.168.80.133",9000,filename).openStream();
        IOUtils.copyBytes(in, System.out,4096,false);
        IOUtils.closeStream(in);
    }
    public static void main(String[] args) throws MalformedURLException, IOException {
        String filename="/user/hadoop/hello.txt";
        System.out.println("读取文件"+filename);
        FSUrl.cat(filename);
        System.out.println("\n读取完成");
    }
}

