/**********************************************************************
 * 	ThreadPoolPack.java
 * 
 *  public ThreadPoolPack类：主要封装线程池，和操作MySqlHelper实例的方法
 *  default ExecuteQueryThread类：封装查询SQL语句操作
 *  default ExecuteUpdateThread类：封装含更新操作的SQL语句操作
 *  default CommitTransactionThread类：封装含更新操作的SQL事务操作
 *  
 **********************************************************************/

package pres.maxwell.nc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolPack {
	
	/* 条件编译：Java编译器优化机制中有这么一条：对于条件表达式中永远为false的语句，编译器将不对条件覆盖的代码段生成字节码。 
	 * REFUSE_WHEN_FULL控制当请求线程时线程池队列已满时是否拒绝，true为拒绝，false为等待线程池空闲  */
	private final boolean REFUSE_WHEN_FULL = false;  
	
	private ThreadPoolExecutor executor;
	private int threadPoolCoreSize;
	
	/* 一般来说，一个线程池只需要一个MySqlHelper类的实例，里面带有一个连接池，可以创建多个连接 */
	private MySqlHelper mysqlHelperObj = null;

	
	/* 构造方法，创建MySqlHelper实例 */
	public ThreadPoolPack(String filePath,int threadPoolCoreSize){
				
		if(0 == threadPoolCoreSize){
			throw new RuntimeException("ThreadPoolPack：错误，线程池最小执行线程为1");
		}
		
		String url = "";
		String userName = "";
		String password = "";
		
		FileInputStream fis = null;
		
        try {
            /* 从配置文件中读取配置信息 */
        	Properties pp = new Properties();
        	fis = new FileInputStream(filePath);
            pp.load(fis);
            url = pp.getProperty("url");
            userName = pp.getProperty("userName");
            password = pp.getProperty("password");

        } 
        catch (Exception e) {
            e.printStackTrace();
        } 
        finally {
            if (fis != null){
            	try {
                    fis.close();
                } 
            	catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        this.threadPoolCoreSize = threadPoolCoreSize;
		this.mysqlHelperObj = new MySqlHelper(url, userName, password, threadPoolCoreSize);	
	}

	
	/* 创建线程池，maxPoolSize：线程池最大的线程数（含活动线程），keepAliveTime：线程空闲时保留时间（毫秒） */
	public void createThreadPool(int maxPoolSize,long keepAliveTime) {
		
		if(maxPoolSize <= this.threadPoolCoreSize){
			throw new RuntimeException("ThreadPoolPack：线程池最大线程数必须比连接池最大活跃连接数大");
		}
		else{	
			executor = new ThreadPoolExecutor(
					this.threadPoolCoreSize,
					maxPoolSize,
					keepAliveTime,
					TimeUnit.MILLISECONDS,
					new ArrayBlockingQueue<Runnable>(maxPoolSize - this.threadPoolCoreSize));//线程池缓冲队列 = 最大线程数 - 最大活动线程数
		}
	}

	
	/* 关闭所有连接 */
	public void closeAllConnections(){
		
		if(executor.isShutdown()==false){
			System.out.println("ThreadPoolPack：警告：线程池还没关闭，不能关闭连接池");
			return;		
		}
		
		while(executor.getPoolSize()!=0){
			//等待所有非线程退出
		}
		
		mysqlHelperObj.closeAllConnections();	
	}
	
	
	/* 关闭指定的结果集 */
	public void closeResultSet(ResultSet rs){
		
		try {
			rs.close();
			System.out.println("ThreadPoolPack：关闭结果集 "+ Integer.toHexString(rs.hashCode()) );
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	/* 判断线程池是否达到最大线程数 */
	public boolean isThreadPoolFull(){
		
		int currentPoolSize = executor.getPoolSize()+ executor.getQueue().size();
		if(currentPoolSize < executor.getMaximumPoolSize() ){
			return false;
		}
		else{
			return true;
		}
	}
	
	
	/* 关闭线程池，不在接收新的线程提交请求，不会终止还在队列中或者活动中的线程 */
	public void shutdownThreadPool(){
		
		executor.shutdown();
	}

	
	/* 返回线程池已经完成的任务数 */
	public long getCompletedTaskCount(){
	
		return executor.getCompletedTaskCount();
	}	
	
	
	/* 检查线程池是否运行请求新线程 */
	public boolean checkThreadPool(){
		
		if(REFUSE_WHEN_FULL){
			if(isThreadPoolFull()==true){		
				System.out.println("ThreadPoolPack：出错，队列已满！请求被拒绝！");
				return false;
			}
		}
		
		if(!REFUSE_WHEN_FULL){
			
			while(isThreadPoolFull()==true){
				//队列已满，等待的队列空闲
			}
		}
		return true;
	}
	
	
	/* 提交查询SQL语句线程，返回的是封装 */
	public Future<ResultSet> submitSqlQuery(String sql,String... parameters) {
		
		if(!checkThreadPool()){
			return null;
		}

		/* 创建线程类并提交到线程池 */
		ExecuteQueryThread queryThread = new ExecuteQueryThread(mysqlHelperObj,sql,parameters);	
		Future<ResultSet> future = executor.submit(queryThread);
		
		return future;
	}
	
	
	/* 获得结果集 */
	public <V> Object getResult(Future<V> future) {
		
		Object rs = null;
		
		try {
			/* get方法：阻塞直至到线程返回结果集 */
			rs = future.get();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	
		return rs;	
	}

	
	/* 打印结果集 */
	public void printResultSet(ResultSet rs,String... args){
		
		try {	
			while (rs.next()) {
				for(int i=0;i<args.length;i++){	
					System.out.println(args[i]+":" + rs.getString(args[i]));
				}	
				
				System.out.println();
			}
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}	
	}
		 
	
	/* 提交更新操作SQL语句线程 */
	public Future<Integer> submitSqlUpdate(String sql,String... parameters) {
		
		if(!checkThreadPool()){
			return null;
		}

		/* 创建线程类并提交到线程池 */
		ExecuteUpdateThread updateThread = new ExecuteUpdateThread(mysqlHelperObj,sql,parameters);	
		Future<Integer> future = executor.submit(updateThread);
		
		return future;
	}
	
	
	/* 打印更新操作的结果 */
	public void printUpdateResult(Future<Integer> future){
		
		Integer rs = (Integer) getResult(future);
		System.out.println("ThreadPoolPack：操作成功，并影响了数据库表的行数为 " + rs.intValue() + " 行");	
	}	
	
	
	/* 提交更新操作SQL事务线程 */
	public void submitSqlUpdateTransaction(String[] sql,String[]... parameters) {
		
		if(!checkThreadPool()){
			return;
		}

		/* 创建线程类并提交到线程池 */
		CommitTransactionThread transactionThread = new CommitTransactionThread(mysqlHelperObj,sql,parameters);	
		executor.submit(transactionThread);

	}	
	
	
	
	
	
}//TreadPoolPack




class ExecuteQueryThread implements Callable<ResultSet> {
	
	private Connection conn;
	private String sql = "";
	private String[] parameters;
	private ResultSet rs;
	
	private MySqlHelper mysqlHelperObj;
	
	
	/* 构造方法，获得要执行的SQL语句和操作的MySqlHelper对象 */
	public ExecuteQueryThread(MySqlHelper mysqlHelperObj,String sql,String... parameters){
		this.sql = sql;
		this.parameters = parameters;
		this.mysqlHelperObj = mysqlHelperObj;
	}	

	/* 从线程池中获取连接 */
	private void getConnection(){
		conn = mysqlHelperObj.getConnectionFromPool();
		
		/* 获取连接失败，重新获取 */
		while(conn == null){
			conn = mysqlHelperObj.getConnectionFromPool();
		}
		
		System.out.println("ThreadPoolPack：获得连接"+ Integer.toHexString(conn.hashCode()));
	}
	
	@Override
	public ResultSet call() {	

		getConnection();
		
		try {
			rs = mysqlHelperObj.executeQuery(conn,sql,parameters);		
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			/* 设置连接为空闲状态 */
			mysqlHelperObj.setConnectionStateToIdle(conn);
		}

		return rs;
	}

}//ExecuteQueryThread



class ExecuteUpdateThread implements Callable<Integer>{
	
	private Connection conn;
	private String sql = "";
	private String[] parameters;
	private MySqlHelper mysqlHelperObj;
	
	
	/* 构造方法，获得要执行的SQL语句和操作的MySqlHelper对象 */
	public ExecuteUpdateThread(MySqlHelper mysqlHelperObj,String sql,String... parameters){
		this.sql = sql;
		this.parameters = parameters;
		this.mysqlHelperObj = mysqlHelperObj;
	}	

	/* 从线程池中获取连接 */
	private void getConnection(){
		conn = mysqlHelperObj.getConnectionFromPool();
		
		/* 获取连接失败，重新获取 */
		while(conn == null){
			conn = mysqlHelperObj.getConnectionFromPool();
		}
		
		System.out.println("ThreadPoolPack：获得连接"+ Integer.toHexString(conn.hashCode()));
	}
	
	@Override
	public Integer call() {	

		getConnection();
		
		Integer ret = null;
		try {	
			ret = mysqlHelperObj.executeUpdate(conn,sql, parameters);
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			/* 设置连接为空闲状态 */
			mysqlHelperObj.setConnectionStateToIdle(conn);
		}
		
		return ret;
	}
	
	
}//ExecuteUpdateThread



class CommitTransactionThread implements Runnable{
	
	private Connection conn;
	private String[] sql;
	private String[][] parameters;
	private MySqlHelper mysqlHelperObj;
	
	
	/* 构造方法，获得要执行的SQL语句和操作的MySqlHelper对象 */
	public CommitTransactionThread(MySqlHelper mysqlHelperObj,String[] sql,String[]... parameters){
		this.sql = sql;
		this.parameters = parameters;
		this.mysqlHelperObj = mysqlHelperObj;
	}	

	/* 从线程池中获取连接 */
	private void getConnection(){
		conn = mysqlHelperObj.getConnectionFromPool();
		
		/* 获取连接失败，重新获取 */
		while(conn == null){
			conn = mysqlHelperObj.getConnectionFromPool();
		}
		
		System.out.println("ThreadPoolPack：获得连接"+ Integer.toHexString(conn.hashCode()));
	}
	
	@Override
	public void run() {	

		getConnection();
		
		try {	
			mysqlHelperObj.executeTransaction(conn,sql, parameters);
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			/* 设置连接为空闲状态 */
			mysqlHelperObj.setConnectionStateToIdle(conn);
		}
		
		
	}
	
	
}//CommitTransactionThread


