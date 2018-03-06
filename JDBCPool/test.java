package JDBCPool;



import java.sql.Connection;
import java.sql.SQLException;

public class test {
    public static void main(String[] args) throws SQLException {
        Connection conn = MysqlPool.getConnection();
        Connection conn2 = MysqlPool.getConnection();
        MysqlPool.release(conn,null,null);
        MysqlPool.release(conn2,null,null);
        Connection conn3 = MysqlPool.getConnection();
        MysqlPool.release(conn3,null,null);

    }
}
