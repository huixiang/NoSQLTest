package com.yt.couchbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.yt.entity.User;
import com.yt.util.StringGenerateUitil;

/**
 * Title: CouchbaseResponseTimeTest.java
 * Description: 
 * Company: HundSun
 * @author yangting
 * @date 2017年7月3日 上午11:00:06
 * @version 1.0
 */
public class CouchbaseResponseTimeTest {
	private   int  counter;
	private   int  dalcounter;
	private static int index;
	static	Cluster cluster;
	static    List<Bucket> buckets = new ArrayList<Bucket>();
	static Long time;
	static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
	static Long dalTime;
	public int incrCounter(){
		synchronized (this) {
			return counter++;
		}
	}
	private int threadNumber;
	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}
	public int getThreadNumber() {
		return threadNumber;
	}
	@Autowired
	int readTimes;
	public int getReadTimes() {
		return readTimes;
	}
	public void setReadTimes(int readTimes) {
		this.readTimes = readTimes;
	}
	public int getWriteTimes() {
		return writeTimes;
	}
	public void setWriteTimes(int writeTimes) {
		this.writeTimes = writeTimes;
	}
	public int getBucketNumber() {
		return bucketNumber;
	}
	public void setBucketNumber(int bucketNumber) {
		this.bucketNumber = bucketNumber;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	public String getBucketPassword() {
		return bucketPassword;
	}
	public void setBucketPassword(String bucketPassword) {
		this.bucketPassword = bucketPassword;
	}
	public int getTestDataSize() {
		return testDataSize;
	}
	public void setTestDataSize(int testDataSize) {
		this.testDataSize = testDataSize;
	}
	public String getTestMethod() {
		return testMethod;
	}
	public void setTestMethod(String testMethod) {
		this.testMethod = testMethod;
	}
	int writeTimes;
	int bucketNumber;
	String address;
	String bucketName;
	String bucketPassword;
	int testDataSize;
	String testMethod;
	
	long totalTim;
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
	private static ApplicationContext context;    
	
	public static void main(String[] args) throws FileNotFoundException, IOException {


		context = new ClassPathXmlApplicationContext("classpath:couchbase-config.xml");
		CouchbaseResponseTimeTest couchbaseTest = (CouchbaseResponseTimeTest) context.getBean("couchbaseResponseTime");
		cluster = CouchbaseCluster.create(couchbaseTest.address);
		for(int i=0;i<couchbaseTest.bucketNumber;i++)
        	buckets.add(cluster.openBucket(couchbaseTest.bucketName, couchbaseTest.bucketPassword, 130, TimeUnit.SECONDS));
        
		try {
			if("read".equalsIgnoreCase(couchbaseTest.testMethod))
				couchbaseTest.testGetUser(couchbaseTest);
			else if("write".equalsIgnoreCase(couchbaseTest.testMethod))
				couchbaseTest.testAddUser(couchbaseTest);
			else
				System.out.println("请输入有效测试方法,read or test!");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.exit(1);

    
	}
	public  void testAddUser(CouchbaseResponseTimeTest couchbaseTest) throws InterruptedException{
		time = (long) 0;
		dalTime = System.currentTimeMillis();
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		for(int i=0;i<couchbaseTest.writeTimes;i++){
			executorService.execute(new UserAdd(buckets.get(index++%bucketNumber),i));
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
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + couchbaseTest.writeTimes/totalTime);
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
	public  void testGetUser(CouchbaseResponseTimeTest couchbaseTest) throws InterruptedException{
		int dal = -1;
		time = (long) 0;
		dalTime = System.currentTimeMillis();
		
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		for(int i=0;i<couchbaseTest.readTimes;i++){
			executorService.execute(new UserGet(buckets.get(index++%bucketNumber),random.nextInt(couchbaseTest.writeTimes)));
			if(System.currentTimeMillis() - dalTime >= 1000){
				System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime))
						+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
				dalTime = System.currentTimeMillis();
				dalcounter = counter;
			}
		}	
		executorService.shutdown();
		while((counter!=dal || counter==0) && executorService.isTerminated()){
			try {
				if(System.currentTimeMillis() - dalTime >= 1000){
					System.out.println(simpleDateFormat.format(new Date(System.currentTimeMillis())) + "-" + simpleDateFormat.format(new Date(dalTime))
							+ " OPS:  " + (counter-dalcounter) / ((System.currentTimeMillis()-dalTime)/1000.0));
					dalTime = System.currentTimeMillis();
					dalcounter = counter;
				}
				dal = counter;
				Thread.sleep(20);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		double totalTime = (System.currentTimeMillis()-time)/1000.0;	
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + couchbaseTest.readTimes/totalTime);
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
	class UserAdd implements Runnable{
		private Bucket bucket;
		private long startTime;
		private int i;
		public UserAdd(Bucket bucket,int i) {
			this.bucket = bucket;
			this.i = i;
		}
		public void run() {
			
			User user = new User();
			user.setId("k" + i);
			HashMap<String,String> map = new HashMap<String,String>();
			map.put("v", StringGenerateUitil.generateString(testDataSize));
			startTime = System.nanoTime();
			if(time == 0)
				time = System.currentTimeMillis();
			bucket.upsert(JsonDocument.create(user.getId(), 0, JsonObject.from(map)), 60, TimeUnit.SECONDS);
			incrCounter();
			
			totalTim = totalTim + System.nanoTime() - startTime;
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
	class UserGet implements Runnable{
		private Bucket bucket;
		private long startTime;
		private int i;
		public UserGet(Bucket bucket,int i) {
			this.bucket = bucket;
			this.i = i;
		}
		public void run() {
			
		
			startTime = System.nanoTime();
			if(time == 0)
				time = System.currentTimeMillis();
			 
			bucket.get("k"+i, JsonDocument.class);
//			userDao.query(select("name").from("databases").where(x(category).eq(s("NoSQL"))))
			
			incrCounter();
			totalTim = totalTim + System.nanoTime() - startTime;
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
