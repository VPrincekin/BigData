package wdh.crystal.dao;
/**
 * DAO工厂类
 */
public class DAOFactory {
    //获取任务管理DAO
    public static TaskDaoImp getTaskDAO(){
        return new TaskDaoImp();
    }
}
