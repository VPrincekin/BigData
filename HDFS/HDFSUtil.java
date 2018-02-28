package hdfs.util;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSUtil {
    // 获得一个客户端实例。URI--统一资源表示符 ；user--当前操作用户
    public static FileSystem getFs(URI uri, String user) throws Exception{
        //cof里面管理的是集群的所有配置信息，包括我们在配置文件里面的配置信息
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf, user);
        return fs;
    }

    // 从Windows本地上传文件到hdfs. src--原路径，dst--目标路径。
    public static void upload(URI uri, String user, Path src, Path dst)throws Exception {
        getFs(uri, user).copyFromLocalFile(src, dst);
        close(uri, user);
    }

    // 从hdfs下载文件。 第一个boolean参数表示是否删除原文件。第二个boolean参数表示中转的时候需不需要使用本地文件系统。
    public static void download(URI uri, String user, Path src, Path dst)throws Exception{
        getFs(uri, user).copyToLocalFile(false, src, dst, true);
        close(uri, user);
    }

    // 删除文件或者文件夹。true和false表示是否级联删除，主要针对文件夹。
    public static void delete(URI uri, String user, Path path, boolean boo)throws Exception{
        getFs(uri, user).delete(path, boo);
        close(uri, user);
    }

    // 重命名。
    public static void rename(URI uri, String user, Path oldname, Path newname)throws Exception{
        getFs(uri, user).rename(oldname, newname);
        close(uri, user);
    }

    // 查看目录信息，只显示该文件夹下的文件信息
    public static void listFiles(URI uri, String user, Path path, boolean boo)throws Exception {
        // 返回的是指定目录下的所有文件信息。true和false表示是否级联。
        RemoteIterator<LocatedFileStatus> listFiles = getListFiles(uri, user,path, boo);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath());// 绝对路径
            System.out.println(fileStatus.getPath().getName());// 文件名
            System.out.println(fileStatus.getBlockSize());// Block大小
            System.out.println(fileStatus.getPermission());// 文件权限
            System.out.println(fileStatus.getReplication());// 副本数
            System.out.println(fileStatus.getLen());// 文件字节长度
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();// Block的信息
            for (BlockLocation bl : blockLocations) {
                System.out.println("Block Length:" + bl.getLength()
                        + "   Block Offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();// 每个Block所分配的主机
                for (String str : hosts) {
                    System.out.print(str + "\t");
                }
                System.out.println();
            }
            System.out.println("---------------------------");
        }
        close(uri, user);
    }

    public static RemoteIterator<LocatedFileStatus> getListFiles(URI uri,String user, Path path, boolean boo) throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = getFs(uri, user).listFiles(path, boo);
        return listFiles;
    }

    // 查看文件及文件夹信息
    public static void listStatus(URI uri, String user, Path path)throws Exception {
        // 返回的是文件以及文件夹的信息
        FileStatus[] listStatus = getListStatus(uri, user, path);
        String flag = "";
        for (FileStatus status : listStatus) {
            if (status.isDirectory()) {// 判断是文件还是文件夹
                flag = "Directory";
            } else {
                flag = "File";
            }
            System.out.println(flag + "\t" + status.getPath().getName());
        }
        close(uri, user);
    }

    public static FileStatus[] getListStatus(URI uri, String user, Path path)throws Exception {
        FileStatus[] listStatus = getFs(uri, user).listStatus(path);
        return listStatus;
    }

    // 清理close
    public static void close(URI uri, String user) throws Exception{
        getFs(uri, user).close();
    }
}
