package com.yt.redis;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
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
 * Title: RedisOPSTest.java
 * Description: 
 * Company: HundSun
 * @author yangting
 * @date 2017年7月3日 上午11:02:03
 * @version 1.0
 */
public class RedisOPSTest {
	private  UserDao userDao;
	private int  counter;
	private   int  dalcounter;
	static long time;
	static long dalTime;
	static int poke;
	static Random random = new Random(System.currentTimeMillis()%1000);
	public  void incrCounter(){
		synchronized (this) {
			counter++;
		}
		
	}


	static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

	private static ApplicationContext context;
	public static void main(String[] args) {
		RedisOPSTest redisTest = new RedisOPSTest();
		context = new ClassPathXmlApplicationContext("classpath:redis-config.xml");
		redisTest.setUserDao((UserDao) context.getBean("opsTestDao"));
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
		ExecutorService executorService = Executors.newFixedThreadPool(userDao.getThreadNumber());
		for(int i=0;i<userDao.getWriteTimes();i++){
			executorService.execute(new UserAdd(userDao,i));
			if(System.currentTimeMillis() - dalTime >= 2000){
				System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime)) 
						+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
				dalTime = System.currentTimeMillis();
				dalcounter = counter;
			}
		}

		executorService.shutdown();
		while(!executorService.isTerminated()){
				if(System.currentTimeMillis() - dalTime >= 2000){
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
		System.out.println("总次数:  " + counter);
		
	}
	public void testGetUser(){
		int dal = -1;
		time = 0;
		dalTime = System.currentTimeMillis();
		ExecutorService executorService = Executors.newFixedThreadPool(userDao.getThreadNumber());
		for(int i=0;i<userDao.getReadTimes();i++){
			executorService.execute(new UserGet(userDao,random.nextInt(userDao.getWriteTimes())));
		}

		executorService.shutdown();
		while(!executorService.isTerminated()){
				if(System.currentTimeMillis() - dalTime >= 2000){
					System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime)) 
							+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
					dalTime = System.currentTimeMillis();
					dalcounter = counter;
				}
				dal = counter;
				Thread.yield();
		}
	
		double totalTime = (System.currentTimeMillis()-time)/1000.0;	
		System.out.println("结束时间:  " + simpleDateFormat.format(new Date(System.currentTimeMillis())));
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + userDao.getReadTimes()/totalTime);
		System.out.println(counter);
		
	}
	public void setUserDao(UserDao userDao) {
		this.userDao = userDao;
	}
	class UserAdd implements Runnable{
		private UserDao userDao;
		private int i;
		public UserAdd(UserDao userDao,int i) {
			this.userDao = userDao;
			this.i = i;
		}
		public void run() {
			if(time == 0)
				time = System.currentTimeMillis();
			boolean result = false;
			User user = new User();
			user.setId("k" + i);
			HashMap<byte[],byte[]> map = new HashMap<byte[],byte[]>();
			map.put("v".getBytes(), StringGenerateUitil.generateString(userDao.getTestDataSize()).getBytes());
			user.setHashMap(map);
			//这里是具体的插入操作，对应不同数据类型写入，jedis提供了不同方法，请酌情选用，注意和查询操作使用的方法对应
			result = userDao.addHashMap(user);
			incrCounter();
			if(!result)
				System.out.println("error");
			
		}
		
	}
	class UserGet implements Runnable{
		private UserDao userDao;
		private int i;
		public UserGet(UserDao userDao,int i) {
			this.userDao = userDao;
			this.i = i;
		}
		public void run() {
			
			
			if(time == 0)
				time = System.currentTimeMillis();
			//注意和写入类型一致
			userDao.getMap("k" + i);
			incrCounter();		
		}
		
	}
}
