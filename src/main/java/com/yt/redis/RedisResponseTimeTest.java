package com.yt.redis;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.yt.dao.UserDao;
import com.yt.entity.User;
import com.yt.util.StringGenerateUitil;

/**
 * Title: RedisResponseTimeTest.java
 * Description: 
 * Company: HundSun
 * @author yangting
 * @date 2017年7月3日 上午11:02:12
 * @version 1.0
 */
public class RedisResponseTimeTest {
	private  UserDao userDao;
	private int  counter;
	private   int  dalcounter;
	static long time;
	static long dalTime;
	static  long totalTim;
	long oneSecond;
	long secSecond;
	long thiSeconed;
	long forSecond;
	long fiveSecond;
	long sixSecond;
	long seventh;
	long eighth;
	long ninth;
	long tenth;
	
	static Random random = new Random(47);
	
	public  void incrCounter(){
		synchronized (this) {
			counter++;
		}
		
	}

	
	static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

	private static ApplicationContext context;
	public static void main(String[] args) {
		RedisResponseTimeTest redisTest = new RedisResponseTimeTest();
		context = new ClassPathXmlApplicationContext("classpath:redis-config.xml");
		redisTest.setUserDao((UserDao) context.getBean("responseTimeDao"));
		if("read".equalsIgnoreCase(redisTest.userDao.getTestMethod()))
			redisTest.testGetUser();
		else if("write".equalsIgnoreCase(redisTest.userDao.getTestMethod()))
			redisTest.testAddUser();
		else
			System.out.println("请输入有效测试方法,read or test!");

		System.exit(1);
	}
	
	
	public void testAddUser(){
		time = 0;
		dalTime = System.currentTimeMillis();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		for(int i=0;i<userDao.getWriteTimes();i++){
			executorService.execute(new UserAdd(userDao,i));
			if(System.currentTimeMillis() - dalTime >= 1000){
				System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime)) 
						+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
				dalTime = System.currentTimeMillis();
				dalcounter = counter;
			}
		}

		executorService.shutdown();
		while(!executorService.isTerminated()){
				if(System.currentTimeMillis() - dalTime >= 1000){
					System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime)) 
							+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
					dalTime = System.currentTimeMillis();
					dalcounter = counter;
				}
				Thread.yield();
		}
	
		double totalTime = (System.currentTimeMillis()-time)/1000.0;	
		System.out.println("结束时间:  " + simpleDateFormat.format(new Date(System.currentTimeMillis())));
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + userDao.getWriteTimes()/totalTime);
		System.out.println("平均响应时间: " + totalTim/(counter*1000000.0) + "ms");
		System.out.println("响应时间小于1ms占比 : " + String.format("%.1f", (oneSecond*100.0)/counter)  + "%");
		System.out.println("响应时间小于2ms占比 : " + String.format("%.1f", (secSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于3ms占比 : " + String.format("%.1f", (thiSeconed*100.0)/counter)   + "%");
		System.out.println("响应时间小于4ms占比 : " + String.format("%.1f", (forSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于5ms占比 : " + String.format("%.1f", (fiveSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于6ms占比 : " + String.format("%.1f", (sixSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于7ms占比 : " + String.format("%.1f", (seventh*100.0)/counter)   + "%");
		System.out.println("响应时间小于8ms占比 : " + String.format("%.1f", (eighth*100.0)/counter)   + "%");
		System.out.println("响应时间小于9ms占比 : " + String.format("%.1f", (ninth*100.0)/counter)   + "%");
		System.out.println("响应时间小于10ms占比 : " + String.format("%.1f", (tenth*100.0)/counter)   + "%");
		System.out.println(counter);
		
	}
	public void testGetUser(){
		time = 0;
		dalTime = System.currentTimeMillis();
		ExecutorService executorService = Executors.newFixedThreadPool(userDao.getThreadNumber());
		for(int i=0;i<userDao.getReadTimes();i++){
			executorService.execute(new UserGet(userDao,random.nextInt(userDao.getWriteTimes())));
		}

		executorService.shutdown();
		while(!executorService.isTerminated()){
				if(System.currentTimeMillis() - dalTime >= 1000){
					System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime)) 
							+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
					dalTime = System.currentTimeMillis();
					dalcounter = counter;
				}
				Thread.yield();;
		}
	
		double totalTime = (System.currentTimeMillis()-time)/1000.0;	
		System.out.println("结束时间:  " + simpleDateFormat.format(new Date(System.currentTimeMillis())));
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + userDao.getReadTimes()/totalTime);
		System.out.println("平均响应时间: " + totalTim/(counter*1000000.0) + "ms");
		System.out.println("响应时间小于1ms占比 : " + String.format("%.1f", (oneSecond*100.0)/counter)  + "%");
		System.out.println("响应时间小于2ms占比 : " + String.format("%.1f", (secSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于3ms占比 : " + String.format("%.1f", (thiSeconed*100.0)/counter)   + "%");
		System.out.println("响应时间小于4ms占比 : " + String.format("%.1f", (forSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于5ms占比 : " + String.format("%.1f", (fiveSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于6ms占比 : " + String.format("%.1f", (sixSecond*100.0)/counter)   + "%");
		System.out.println("响应时间小于7ms占比 : " + String.format("%.1f", (seventh*100.0)/counter)   + "%");
		System.out.println("响应时间小于8ms占比 : " + String.format("%.1f", (eighth*100.0)/counter)   + "%");
		System.out.println("响应时间小于9ms占比 : " + String.format("%.1f", (ninth*100.0)/counter)   + "%");
		System.out.println("响应时间小于10ms占比 : " + String.format("%.1f", (tenth*100.0)/counter)   + "%");

		System.out.println(counter);
		
	}
	public void setUserDao(UserDao userDao) {
		this.userDao = userDao;
	}
	class UserAdd implements Runnable{
		private UserDao userDao;
		private long startTime;
		private int i;
		public UserAdd(UserDao userDao,int i) {
			this.userDao = userDao;
			this.i = i;
		}
		public void run() {
			
			User user = new User();
			user.setId("k" + i);
			user.setName(StringGenerateUitil.generateString(userDao.getTestDataSize()));
			startTime = System.nanoTime();
			if(time == 0)
				time = System.currentTimeMillis();
			boolean result = userDao.add(user);
			incrCounter();
			totalTim = totalTim +  System.nanoTime() - startTime;
			if(System.nanoTime() - startTime<= 1*1000000)
				oneSecond++;
			if(System.nanoTime() - startTime<= 2*1000000)
				secSecond++;
			if(System.nanoTime() - startTime<= 3*1000000)
				thiSeconed++;
			if(System.nanoTime() - startTime<= 4*1000000)
				forSecond++;
			if(System.nanoTime() - startTime<= 5*1000000)
				fiveSecond++;
			if(System.nanoTime() - startTime<= 6*1000000)
				sixSecond++;
			if(System.nanoTime() - startTime<= 7*1000000)
				seventh++;
			if(System.nanoTime() - startTime<= 8*1000000)
				eighth++;
			if(System.nanoTime() - startTime<= 9*1000000)
				ninth++;
			if(System.nanoTime() - startTime<= 10*1000000)
				tenth++;
			if(!result)
				System.out.println("error");
			
		}
		
	}
	class UserGet implements Runnable{
		private UserDao userDao;
		private long startTime;
		private int i;
		public UserGet(UserDao userDao,int i) {
			this.userDao = userDao;
			this.i = i;
		}
		public void run() {
			
			
			startTime = System.nanoTime();
			if(time == 0)
				time = System.currentTimeMillis();
			userDao.get("k" + i);
			incrCounter();
			totalTim = totalTim +  System.nanoTime() - startTime;
			if(System.nanoTime() - startTime<= 1*1000000)
				oneSecond++;
			if(System.nanoTime() - startTime<= 2*1000000)
				secSecond++;
			if(System.nanoTime() - startTime<= 3*1000000)
				thiSeconed++;
			if(System.nanoTime() - startTime<= 4*1000000)
				forSecond++;
			if(System.nanoTime() - startTime<= 5*1000000)
				fiveSecond++;
			if(System.nanoTime() - startTime<= 6*1000000)
				sixSecond++;
			if(System.nanoTime() - startTime<= 7*1000000)
				seventh++;
			if(System.nanoTime() - startTime<= 8*1000000)
				eighth++;
			if(System.nanoTime() - startTime<= 9*1000000)
				ninth++;
			if(System.nanoTime() - startTime<= 10*1000000)
				tenth++;
		
			
		}
		
	}
}
