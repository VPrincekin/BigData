package hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FSObj {
    private static String uriStr = "hdfs://hadoop2:9000";
    private static String user = "hadoop";
    private static FileSystem fs = null;
    private static URI uri = null;
    static{
        try {
            Configuration conf = new Configuration();
            uri = new URI(uriStr);
            fs = FileSystem.get(uri,conf,user);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static FileSystem getFileSystem() {
        return fs;
    }
    public static void release(FileSystem fs){
        if(fs != null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}


