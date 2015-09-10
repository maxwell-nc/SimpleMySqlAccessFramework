package pres.nc.maxwell.test;
/**********************************************************************
 * 	TestClient.java
 * 
 *  主要封装一系列可用于JUnit测试的方法，验证框架正确性
 *  
 *  包含以下可用于JUnit测试的方法：
 *  
 *  testSelectInSingleTask：测试SQL查询单任务
 *  testSelectInMultiTask：测试SQL查询多任务（每个任务一个线程）
 *  
 *  testUpdateInSingleTask：测试SQL更新操作Update单任务
 *  testInsertInSingleTask：测试SQL更新操作Insert单任务 
 *  testDeleteInSingleTask：测试SQL更新操作Delete单任务
 *  
 *  testInsertInMultiTask：测试SQL更新操作Insert多任务（每个任务一个线程）
 *  testUpdateInMultiTask：测试SQL更新操作Update多任务（每个任务一个线程）
 *  testDeleteInMultiTask：测试SQL更新操作Delete多任务（每个任务一个线程） 
 *  
 *  testMixUpdateInMultiTask：测试SQL更新操作Insert/Update/Delete多任务混合（每个任务一个线程）
 *  
 *  testTransactionInSingleTask：测试SQL更新事务单任务
 *  
 *  testCreateInSingleTask：测试SQL更新操作Create单任务
 *  
 **********************************************************************/


import java.sql.ResultSet;
import java.util.concurrent.Future;
import org.junit.Test;

import pres.nc.maxwell.ThreadPoolPack;


public class TestClient {

	private final String SETTING_FILE =  "databaseSetting.properties";
	
	/* 测试SQL查询单任务 */
	@Test
	public void testSelectInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交请求到线程池 */
		String sql = "SELECT * FROM userinfo";
		Future<ResultSet> future1 = tp.submitSqlQuery(sql);		
		
		/* 获得结果集并打印 */
		ResultSet rs = (ResultSet) tp.getResult(future1);
		tp.printResultSet(rs,"userName","password","gender","salary");
		tp.closeResultSet(rs);		

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL查询多任务（每个任务一个线程） */
	@Test
	public void testSelectInMultiTask(){
		
		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 重复提交次数 */
		int repeatTimes = 200;
		
		/* 创建用于保存不同线程得到的结果集 */
		@SuppressWarnings("unchecked")
		Future<ResultSet>[] future = new Future[repeatTimes];
		
		for(int i = 0;i < repeatTimes;i++){
			String sql = "SELECT * FROM userinfo";
			future[i] = tp.submitSqlQuery(sql);
		}
		
		/* 打印结果集
		 * 注意这里使用的判断条件是getCompletedTaskCount(),部分线程可能由于被线程池拒绝而无法执行 */
		for(int i=0;i<tp.getCompletedTaskCount();i++){			
			ResultSet rs = (ResultSet) tp.getResult(future[i]);
			tp.printResultSet(rs,"userName","password","gender","salary");
			tp.closeResultSet(rs);
		}

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Update单任务 */
	@Test
	public void testUpdateInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交请求到线程池 */
		String sql = "UPDATE userinfo SET password=?,salary=? WHERE username = 'maxwell'";
        String[] parameters = { "nc", "888888.00" };
        Future<Integer> future = tp.submitSqlUpdate(sql,parameters);		
        
        tp.printUpdateResult(future);
		
		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Insert单任务 */
	@Test
	public void testInsertInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交请求到线程池 */
		String sql = "INSERT INTO userinfo (username,password,gender,salary) VALUES (?,?,?,?)";
        String[] parameters = { "xiaoming", "123123", "male", "5000.00" };
        Future<Integer> future = tp.submitSqlUpdate(sql,parameters);		
        
        tp.printUpdateResult(future);
		
		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Delete单任务 */
	@Test
	public void testDeleteInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交请求到线程池 */
		String sql = "DELETE FROM userinfo WHERE username = ?";
        String[] parameters = { "xiaoming" };
        Future<Integer> future = tp.submitSqlUpdate(sql,parameters);		
        
        tp.printUpdateResult(future);
		
		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Insert多任务（每个任务一个线程） */
	@Test
	public void testInsertInMultiTask(){
		
		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 重复提交次数 */
		int repeatTimes = 200;
		
		/* 创建用于保存不同线程得到的结果集 */
		@SuppressWarnings("unchecked")
		Future<Integer>[] future = new Future[repeatTimes];
		
		for(int i = 0;i < repeatTimes;i++){
			String sql = "INSERT INTO userinfo (username,password,gender,salary) VALUES (?,?,?,?)";
	        String[] parameters = { "xiaoming" + i, "123123", "male", "5000.00" };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
		
		/* 打印结果集
		 * 注意这里使用的判断条件是getCompletedTaskCount(),部分线程可能由于被线程池拒绝而无法执行 */
		for(int i=0;i<tp.getCompletedTaskCount();i++){			
			tp.printUpdateResult(future[i]);
		}

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Update多任务（每个任务一个线程） */
	@Test
	public void testUpdateInMultiTask(){
		
		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 重复提交次数 */
		int repeatTimes = 200;
		
		/* 创建用于保存不同线程得到的结果集 */
		@SuppressWarnings("unchecked")
		Future<Integer>[] future = new Future[repeatTimes];
		
		for(int i = 0;i < repeatTimes;i++){
			String sql = "UPDATE userinfo SET password=?,salary=? WHERE username = 'xiaoming" + i + "'";
	        String[] parameters = { "ceshi", "6500.00" };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
		
		/* 打印结果集
		 * 注意这里使用的判断条件是getCompletedTaskCount(),部分线程可能由于被线程池拒绝而无法执行 */
		for(int i=0;i<tp.getCompletedTaskCount();i++){			
			tp.printUpdateResult(future[i]);
		}

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Delete多任务（每个任务一个线程） */
	@Test
	public void testDeleteInMultiTask(){
		
		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 重复提交次数 */
		int repeatTimes = 200;
		
		/* 创建用于保存不同线程得到的结果集 */
		@SuppressWarnings("unchecked")
		Future<Integer>[] future = new Future[repeatTimes];
		
		for(int i = 0;i < repeatTimes;i++){
			String sql = "DELETE FROM userinfo WHERE username = ?";
	        String[] parameters = { "xiaoming" + i };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
		
		/* 打印结果集
		 * 注意这里使用的判断条件是getCompletedTaskCount(),部分线程可能由于被线程池拒绝而无法执行 */
		for(int i=0;i<tp.getCompletedTaskCount();i++){			
			tp.printUpdateResult(future[i]);
		}

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Insert/Update/Delete多任务混合（每个任务一个线程）
	 * 注意要操作不同的记录，以免冲突抛出异常
	 * */
	@Test
	public void testMixUpdateInMultiTask(){
		
		/* 准备数据，防止数据不存在抛出异常 */
		testDeleteInMultiTask();
		testInsertInMultiTask();
		
		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 重复提交次数 */
		int repeatTimes = 30;
		
		/* 创建用于保存不同线程得到的结果集 */
		@SuppressWarnings("unchecked")
		Future<Integer>[] future = new Future[repeatTimes*3];
		
		for(int i = 0;i < repeatTimes;i++){
			String sql = "INSERT INTO userinfo (username,password,gender,salary) VALUES (?,?,?,?)";
	        String[] parameters = { "daming" + i, "123123", "male", "5000.00" };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
		
		for(int i = repeatTimes;i < repeatTimes*2 ;i++){
			String sql = "UPDATE userinfo SET password=?,salary=? WHERE username = 'xiaoming" + i + "'";
	        String[] parameters = { "ceshiMix", "6500.00" };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
		for(int i = repeatTimes*2 ;i < repeatTimes*3 ;i++){
			String sql = "DELETE FROM userinfo WHERE username = ?";
	        String[] parameters = { "xiaoming" + i };
			future[i] = tp.submitSqlUpdate(sql,parameters);	
		}
	
		/* 打印结果集
		 * 注意这里使用的判断条件是getCompletedTaskCount(),部分线程可能由于被线程池拒绝而无法执行 */
		for(int i=0;i<tp.getCompletedTaskCount();i++){			
			tp.printUpdateResult(future[i]);
		}

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新事务单任务 */
	@Test
	public void testTransactionInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交事务到线程池 */
		String sql1 = "UPDATE userinfo SET salary=salary-1000 WHERE username = ?";
        String sql2 = "UPDATE userinfo SET salary=salary+1000000 WHERE username = ?";
        String[] sql = { sql1, sql2 };
        
        String[] sql1_params = { "xiaomi" };
        String[] sql2_params = { "fgn" };
        String[][] parameters = { sql1_params, sql2_params };
        
		tp.submitSqlUpdateTransaction(sql,parameters);		

		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
	
	
	/* 测试SQL更新操作Create单任务 */
	@Test
	public void testCreateInSingleTask(){

		/* 设置配置文件和线程池活动线程数（线程池最大活动线程数 == 连接池最大连接数） */
		ThreadPoolPack tp = new ThreadPoolPack(SETTING_FILE,5);
		
		/*创建线程池，最大线程数为1，保持线程活动时间（long）0*/
		tp.createThreadPool(20, 0L);
		
		/* 提交请求到线程池 */
		String sql = "CREATE TABLE `testTable` ("
					  +"`id` int(11) NOT NULL AUTO_INCREMENT,"
					  +"`username` varchar(13) NOT NULL,"
					  +"`password` varchar(13) NOT NULL,"
					  +"`gender` varchar(6) DEFAULT NULL,"
					  +"`salary` double(8,2) DEFAULT NULL,"
					  +"PRIMARY KEY (`id`),"
					  +"UNIQUE KEY `UNIQUE` (`username`));";
        
        Future<Integer> future = tp.submitSqlUpdate(sql);		
        
        tp.printUpdateResult(future);
		
		tp.shutdownThreadPool();
		tp.closeAllConnections();
	}
}
