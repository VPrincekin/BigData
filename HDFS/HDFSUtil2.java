package hdfs.util;

import org.apache.hadoop.fs.*;

import java.io.IOException;

public class HDFSUtil2 {

    //从Windows本地上传文件到hdfs. src--原路径，dst--目标路径。
    private static FileSystem fs = null;
    public static void upload(Path src , Path dst){
        try {
            fs = FSObj.getFileSystem();
            fs.copyFromLocalFile(src,dst);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }


    //从hdfs下载文件。 第一个boolean参数表示是否删除原文件。第二个boolean参数表示中转的时候需不需要使用本地文件系统。
    public static void download(Path src,Path dst){
        try {
            fs = FSObj.getFileSystem();
            fs.copyToLocalFile(src,dst);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }


    // 删除文件或者文件夹。true和false表示是否级联删除，主要针对文件夹。
    public static void delete(Path path,boolean boo){
        try {
            fs = FSObj.getFileSystem();
            fs.delete(path,boo);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }


    // 重命名。
    public static void rename(Path oldname,Path newname){
        try {
            fs = FSObj.getFileSystem();
            fs.rename(oldname, newname);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }


    // 查看目录信息，只显示该文件夹下的文件信息
    public static void listFiles(Path path,boolean boo){
        //返回的是指定目录下的所有文件信息。true和false表示是否级联。
        try {
            fs = FSObj.getFileSystem();
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path,boo);
            while(listFiles.hasNext()){
                LocatedFileStatus fileStatus = listFiles.next();
                System.out.println("绝对路径："+fileStatus.getPath());
                System.out.println("文件名："+fileStatus.getPath().getName());
                System.out.println("Block大小："+fileStatus.getBlockSize());
                System.out.println("文件权限："+fileStatus.getPermission());
                System.out.println("副本数："+fileStatus.getReplication());
                System.out.println("文件字节长度："+fileStatus.getLen());
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();//Block的信息
                for(BlockLocation bl : blockLocations){
                    System.out.println("Block Length:" + bl.getLength() + "   Block Offset:" + bl.getOffset());
                    String[] hosts = bl.getHosts();// 每个Block所分配的主机
                    for (String str : hosts) {
                        System.out.print(str + "\t");
                    }
                    System.out.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }


    // 查看文件及文件夹信息
    public static void listStatus(Path path){
        try {
            fs = FSObj.getFileSystem();
            // 返回的是文件以及文件夹的信息
            FileStatus[] listStatus= fs.listStatus(path);
            String flag = "";
            for(FileStatus status:listStatus){
                if(status.isDirectory()){
                    flag="Directory";
                }else{
                    flag="File";
                }
                System.out.println(flag+"\t"+status.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            FSObj.release(fs);
        }
    }

}
