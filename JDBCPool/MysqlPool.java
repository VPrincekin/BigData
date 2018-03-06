package JDBCPool;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;

public class MysqlPool{
    /**
     * 使用Vector来存放数据库连接，Vector具备线程安全性
     * */
    private static Vector Connections = new Vector();
    /**
     * 静态代码块中加载db.properties数据库配置文件
     * */
    static{
        InputStream in = MysqlPool.class.getClassLoader().getResourceAsStream("db.properties");
        Properties prop = new Properties();
        try {
            prop.load(in);
            String driver = prop.getProperty("driver");
            String url = prop.getProperty("url");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            //数据库连接池的初始化连接大小
            int size= Integer.parseInt(prop.getProperty("jdbcPoolInitSize"));
            //加载数据库驱动
            Class.forName(driver);
            for(int i=0;i<size;i++){
                Connection conn = DriverManager.getConnection(url, username, password);
                System.out.println("获取到了连接"+conn);
                //将获取到的数据库连接加入到Connections集合中，Connections此时就是一个存放了数据库连接的连接池
                Connections.addElement(conn);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static Connection getConnection() throws SQLException {
        //如果数据库连接池中的连接对象个数大于0，直接获取
        if(Connections.size()>0){
            System.out.println("Connections数据库连接池的大小是："+Connections.size());
            Connection conn = (Connection) Connections.remove(0);
            System.out.println("从连接池中取走了一个连接");
            System.out.println("Connections数据库连接池的大小是" + Connections.size());
            return conn;
        }else{
            throw new RuntimeException("对不起，数据库忙");
        }
    }

    public static void release(Connection conn, Statement st,ResultSet rs){
        if(conn!=null){
            try {
                System.out.println(conn + "被还给Connections数据库连接池了");
                Connections.addElement(conn);
                System.out.println("Connections数据库连接池大小为" + Connections.size());
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(st!=null){
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

