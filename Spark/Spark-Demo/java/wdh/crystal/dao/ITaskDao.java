package wdh.crystal.dao;

import wdh.crystal.entity.Task;

/**任务管理DAO接口
 */
public interface ITaskDao {
    /**根据主键查询任务*/
    Task findById(long taskid);
}
