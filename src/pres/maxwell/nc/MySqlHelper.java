/**********************************************************************
 * 	MySqlHelper.java
 * 
 *  主要封装了MYSQL操作，基于JDBC驱动
 *  另外利用集合实现了一个可设置大小的连接池
 * 
 *  一般来说，一次完整的数据库查询过程，只需要创建一个MySqlHelper的实例
 *  这个实例有ThreadPoolPack负责创建，由ThreadPoolPack控制线程池
 *  用户一般不应该操作此类，只需要操作ThreadPoolPack
 *  
 **********************************************************************/

package pres.maxwell.nc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySqlHelper {
	
	/* 用于连接数据库 */
    private String url = "";
    private String userName = "";
    private String password = "";
    
    /* 连接池 */
	private ArrayList<Connection> connectionPool = new ArrayList<Connection>();
	private int connectionPoolSize ;				//连接池最大连接数
	private boolean connectionPoolIdleArr[];		//连接池空闲标记数组,若为true则表示对应集合中的Connection空闲
	
	/* 有参构造函数 */
	public MySqlHelper(String url,String userName,String password,int connectionPoolSize){
		
    	this.url = url;
    	this.userName = userName;
    	this.password = password;
    	
    	/* 创建连接池 */
    	this.connectionPoolSize = connectionPoolSize;
    	connectionPoolIdleArr = new boolean[connectionPoolSize];
    	
    	for(int i=0;i<connectionPoolIdleArr.length;i++){
    		connectionPoolIdleArr[i] = false;
    	}
    	
    }
    
    /* 直接请求获取新的连接,私有方法：防止不利用连接池直接获取新的连接 */
	private Connection getConnection() {
		
        Connection conn = null;		//返回null表示创建不成功
		
        try {
            conn = DriverManager.getConnection(url, userName, password);
        } 
        catch (SQLException e) {
            e.printStackTrace();
        }
		
        return conn;
    }
	

	/* 从连接池中获取连接,如果连接池没满则获取新的连接,否则返回空闲的连接 */
	public Connection getConnectionFromPool() {

		synchronized (connectionPool) {
			Connection conn = null;
			
			
			if(connectionPool.size()<connectionPoolSize){		//连接池还没满
			
				/* 注意这里要设置同步锁 */
				synchronized (connectionPool) {
					conn = getConnection();
					if(conn!=null){
						connectionPool.add(conn);
						System.out.println("MySqlHelper：添加 " + Integer.toHexString(conn.hashCode()) + " 到连接池");
						return conn;
					}
				}
			}
			else{	//连接池已满
				
				/* 由于线程池活动线程数<=连接池最大连接数（一个线程控制一个连接），所以当线程需要连接时必定有连接空闲 */
		    	for(int i = 0;i < connectionPoolIdleArr.length;i++){
		    		if(connectionPoolIdleArr[i] == true){
		    			conn = (Connection)connectionPool.get(i);
		    			connectionPoolIdleArr[i] = false;
		    			return conn;
		    		}
		    	}	
			}
			return conn;
			
		}		
	}
	
	/* 设置连接状态为空闲 */
	public void setConnectionStateToIdle(Connection conn) {

		synchronized (connectionPoolIdleArr) {
			
			int index = connectionPool.indexOf(conn);
			if(index == -1){
				throw new RuntimeException("MySqlHelper：connection不在集合中");			
			}
			else{
				connectionPoolIdleArr[index] = true;				
			}
		}		
	}	
	
	/* 关闭连接池中所有的连接 */	
	public void closeAllConnections() {
		
		synchronized (connectionPool) {
			
			for(Connection conn:connectionPool){
				
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				System.out.println("MySqlHelper：关闭链接 " + Integer.toHexString(conn.hashCode()));	
			}
			
			connectionPool.clear();
		}
		
		/* 设置连接池空闲标记数组中的连接不为空闲 */
		synchronized (connectionPoolIdleArr) {
			
	    	for(int i=0;i<connectionPoolIdleArr.length;i++){
	    		connectionPoolIdleArr[i] = false;
	    	}    	
		}	
	
	}	
	
	
	/* 执行查询SQL语句，一般用于执行Select SQL语句 */		
    public ResultSet executeQuery(Connection conn, String sql, String... parameters) {
    	
        ResultSet rs = null;
        PreparedStatement ps = null;
        
        try {
        	
        	/* 预编译查询语句 */
            ps = conn.prepareStatement(sql);
            if (parameters != null) {
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            }
            
            rs = ps.executeQuery();
        } catch (SQLException e) {
            //e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } 
        
        return rs;
    }
    
    
	/* 执行更新操作的SQL语句，可用于执行Update/Delete/Insert/Create SQL语句 */		
    public Integer executeUpdate(Connection conn, String sql, String... parameters) {
    	
        PreparedStatement ps = null;
        Integer ret;
        
        try {
            ps = conn.prepareStatement(sql);

            /* 预编译SQL语句 */
            if (parameters != null)
                for (int i = 0; i < parameters.length; i++) {
                    ps.setString(i + 1, parameters[i]);
                }
            
            ret = ps.executeUpdate();
        } 
        catch (SQLException e) {
            //e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }  
        
        return ret;
    }
    
    
    /* 执行多句更新操作的SQL语句，可用于执行Update/Delete/Insert组合，这个过程是一个事务 */	
    public void executeTransaction(Connection conn, String[] sql, String[]... parameters){
    	
    	PreparedStatement ps = null;
    	
        try {
            //使连接可以执行一个事务
            conn.setAutoCommit(false);
            
            for (int i = 0; i < sql.length; i++) {
            	
            	//预编译SQL语句
                if (parameters[i] != null) {
                    ps = conn.prepareStatement(sql[i]);
                  
                    for (int j = 0; j < parameters[i].length; j++)
                        ps.setString(j + 1, parameters[i][j]);
                }
                
                ps.executeUpdate();
            }
            
            //提交到连接，此时才开始执行
            conn.commit();
            
        } 
        catch (Exception e) {
        	
        	System.out.println("MySqlHelper：事务出现异常！");
        	
            try {
            	//出现异常回滚事务
                conn.rollback();
            } 
            catch (SQLException e1) {
                //e1.printStackTrace();
            	throw new RuntimeException("MySqlHelper：事务回滚失败！");
            }
            
            //e.printStackTrace();
            System.out.println("MySqlHelper：事务回滚成功！");
            throw new RuntimeException(e.getMessage());
            
        } 
        
        System.out.println("MySqlHelper：事务正常执行完毕 ");	
    }
    
}
