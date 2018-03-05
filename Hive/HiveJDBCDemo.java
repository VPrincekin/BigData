package Hive;

import org.apache.commons.lang.StringUtils;

import java.sql.*;

/**
 * 使用 Hive API 的方式操作Hive数据仓库
 * 使用注意：
 * 1. 要导入 Hive 依赖jar包
 * 2. 要导入 mysql 链接驱动jar包
 * 3. 要导入操作 hdfs 的jar包
 * */
public class HiveJDBCDemo {
    //JDBC方式操作hive的驱动
    private static final String HIVE_DRIVER="org.apace.hive.jdbc.HiveDriver";
    //JDBC方式操作mysql驱动(这里没有用到,写在这儿为了做个对比)
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    //hive连接url(格式：jdbc:hive2://hostname:port/database_name)
    //hostname是启动 hiveserver2 的机器, port 是默认端口 10000, database_name是数据库名
    private static final String HIVE_CONNECT_URL = "jdbc:hive2://hadoop04:10000/dbtest";
    // mysql链接字符串（没有用到,同上）
    private static final String MYSQL_CONNECT_URL = "jdbc:mysql://hadoop02:3306/mydb";

    //用户名和密码
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    // 数据库
    private static final String HIVE_DATABASE = "hive_test_db";

    //表
    private static final String[][] HIVE_TABLE_ATTR_ARRAY = new String[][]{
            new String[]{"id","name","sex","age","department"},
            new String[]{"int","string","string","int","string"}
    };

    //表分隔符
    private static final String HIVE_TABLE_DELIMITER_DEFAULT = ",";
    // 集合分隔符
    private static final String HIVE_TABLE_COLLECTION_DELIMITER_DEFAULT = ":";
    // map分隔符
    private static final String HIVE_TABLE_MAP_DELIMITER_DEFAULT = "-";

    //文件存储格式
    private static final String HIVE_TABLE_FILE_FORMAT_DEFAULT = "textfile";

    //文件路径
    private static final String FILE_PATH = "/home/hadoop/studentss.txt";

    private static Connection connection = null;
    private static Statement statement = null;

    /**
     * 获取hive连接
     * */
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        //注册驱动
        Class.forName(HIVE_DRIVER);
        //获取hive连接
        Connection connection = DriverManager.getConnection(HIVE_CONNECT_URL,USER,PASSWORD);
        return connection;
    }
    /**
     * 获取statement
     * */
    public static Statement getStatement() throws SQLException {

        return connection.createStatement();
    }

    /**
     * 执行完成，关闭链接
     * */
    public static void release(Connection connection) throws SQLException {
        if(connection!=null){
            connection.close();
        }
    }

    /**
     * 创建库
     * */
    public static void createDatabase(String database) throws SQLException {
        String create_database_sql = "create database" + database;
        int excuteUpdate = statement.executeUpdate(create_database_sql);
        System.out.println(excuteUpdate ==0 ? "数据库" + database+ "创建成功":"数据库" + database + "创建失败");
    }


    /**
     * 删除库
     */
    public static void dropDatabase(String database) throws Exception{
        String drop_database_sql = "drop database "+database;
        int executeUpdate = statement.executeUpdate(drop_database_sql);
        System.out.println(executeUpdate == 0 ? "数据库" + database+ "删除成功" :" 数据库" + database + "删除失败");
    }

    /**
     * 创建表
     * */
    public static void createHiveTable(String table,String[][] attArray,String delimiter,String fileFormat) throws SQLException {
        // 直接手写全方式
		/*String create_hive_table_sql = "create table "+table+"(id int, name string, sex string, "
				+ "age int, department string) "
				+ "row format delimited fields terminated by ','";*/

		//拼建表语句
        StringBuffer create_hive_table_sql = new StringBuffer("create table");
        create_hive_table_sql.append(table);
        create_hive_table_sql.append("(");
        int n = attArray[0].length;
        for(int i = 0 ;i<n;i++){
            create_hive_table_sql.append(attArray[0][i]+""+attArray[1][i]);
            if(i != n-1){
                create_hive_table_sql.append(",");
            }
        }
        create_hive_table_sql.append(") row format delimited fields terminated by '");
        create_hive_table_sql.append(delimiter);
        create_hive_table_sql.append("' stroed as");
        create_hive_table_sql.append(fileFormat);

        int executeUpdate = statement.executeUpdate(create_hive_table_sql.toString());
        System.out.println(executeUpdate == 0?"创建" + table + "表成功" :"创建" + table + "表失败");
    }

    /**
     * 删除表
     */
    public static void dropHiveTable(String table) throws Exception{
        String drop_table_sql = "drop table "+table;
        int executeUpdate = statement.executeUpdate(drop_table_sql);
        System.out.println(executeUpdate+"  ---  "+(executeUpdate == 0 ? "删除"+table+"表成功":"删除"+table+"表失败"));
    }

    /**
     * 往hive表导入数据
     * 注意： path是启动 hiveserver2 服务的那台机器的那个用户名的家目录
     * 也就是说， path = hadoop02(我的hiveserver2服务启动机器)的用户hadoop的家目录
     * 第一种方式：/home/hadoop/student.txt(绝对路径写法)
     * 第二种方式：student.txt(相对路径写法，也就是相对/home/hadoop/这个目录而言)
     * */
    public static void loaddata(String table,String path) throws SQLException {
        String loaddata_sql = "load data local inpath '"+ path +"' into table" + table;
        int executeUpdate = statement.executeUpdate(loaddata_sql);
        System.out.println((executeUpdate == 0 ? "往"+table+"表导入数据"+path+"成功":"往"+table+"表导入数据"+path+"失败"));

    }

    /**
     * 查看表
     * */

    public static void showTables(String table) throws Exception{
        String show_tables_sql = "";
        if(!StringUtils.isBlank(table)){
            show_tables_sql = "show tables '"+table+"'";
        }else{
            show_tables_sql = "show tables";
        }
        ResultSet rs = statement.executeQuery(show_tables_sql);
        while(rs.next()){
            System.out.println("表名："+rs.getString(1));
        }
    }
    /**
     * 获取表的描述信息
     */
    public static boolean describ_table(Statement statement, String tableName) {
        String sql = "DESCRIBE " + tableName;
        ResultSet res = null;
        try {
            res = statement.executeQuery(sql);
            System.out.print(tableName + "描述信息:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        connection = getConnection();
        statement = getStatement();

        //创建数据库
        createDatabase(HIVE_DATABASE);

        release(connection);
    }
}
