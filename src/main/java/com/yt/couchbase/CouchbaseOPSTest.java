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
 * Title: CouchbaseOPSTest.java
 * Description: 
 * Company: HundSun
 * @author yangting
 * @date 2017年7月3日 上午10:59:47
 * @version 1.0
 */
public class CouchbaseOPSTest {
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
	@Autowired
	int readTimes;
	private int threadNumber;
	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}
	public int getThreadNumber() {
		return threadNumber;
	}
	
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
	
	static Random random = new Random(System.currentTimeMillis()%1000);
	private static ApplicationContext context;    
	
	public static void main(String[] args) throws FileNotFoundException, IOException {


		context = new ClassPathXmlApplicationContext("classpath:couchbase-config.xml");
		CouchbaseOPSTest couchbaseTest = (CouchbaseOPSTest) context.getBean("couchbaseOPS");
		cluster = CouchbaseCluster.create(couchbaseTest.address);
		for(int i=0;i<couchbaseTest.bucketNumber;i++)
        	buckets.add(cluster.openBucket(couchbaseTest.bucketName, couchbaseTest.bucketPassword, 60, TimeUnit.SECONDS));
        
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
//		buckets.get(0).environment().autoreleaseAfter();
		//查询所有Document
//		ViewResult viewResult = buckets.get(0).query(ViewQuery.from("test", "test]"),1000,TimeUnit.SECONDS);
//		List<ViewRow> list = viewResult.allRows(3000, TimeUnit.SECONDS);
//
//		for(ViewRow viewRow:list){
//			viewRow.key();
//		}
		
// 		比对
//		int timeCounter = 0;
//		OutputStreamWriter outputStreamWriter = new OutputStreamWriter
//				(new BufferedOutputStream(new FileOutputStream("/home/web/new_yt.txt")));
//		BufferedReader inputStreamReader = new BufferedReader(new FileReader("/home/web/yt.txt"));
//		String string = null;
//		while((string=inputStreamReader.readLine()) != null){
//			if(buckets.get(0).get(string)==null){
//				outputStreamWriter.write(string);
//				timeCounter++;
//			}
//		}
//		
//		outputStreamWriter.close();
//		inputStreamReader.close();
//		System.out.println("结果集size:" + iterator.size());
//		System.out.println(timeCounter);
//		System.out.println("Finish!");
//        System.exit(1);

    
	}
	public  void testAddUser(CouchbaseOPSTest couchbaseTest) throws InterruptedException{
		time = (long) 0;
		dalTime = System.currentTimeMillis();
		
		ExecutorService executorService = Executors.newFixedThreadPool(getThreadNumber());
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
		System.out.println("结束时间:  " + simpleDateFormat.format(new Date(System.currentTimeMillis())));
		System.out.println("总耗时:  " + totalTime  + "s");
		System.out.println("OPS:  " + couchbaseTest.writeTimes/totalTime);
		System.out.println(counter);
		
	}
	public  void testGetUser(CouchbaseOPSTest couchbaseTest) throws InterruptedException{
		time = (long) 0;
		dalTime = System.currentTimeMillis();
		
		ExecutorService executorService = Executors.newFixedThreadPool(getThreadNumber());
		for(int i=0;i<couchbaseTest.readTimes;i++){
			executorService.execute(new UserGet(buckets.get(index++%bucketNumber),random.nextInt(writeTimes)));
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
		System.out.println("OPS:  " + couchbaseTest.readTimes/totalTime);
		System.out.println(counter);
		
	}
	class UserAdd implements Runnable{
		private Bucket bucket;
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
			if(time == 0)
				time = System.currentTimeMillis();	
			bucket.upsert(JsonDocument.create(user.getId(), 0, JsonObject.from(map)), 60, TimeUnit.SECONDS);
			incrCounter();

		}
		
	}
	class UserGet implements Runnable{
		private Bucket bucket;
		private int i;
		public UserGet(Bucket bucket,int i) {
			this.bucket = bucket;
			this.i = i;
		}
		public void run() {	
			if(time == 0)
				time = System.currentTimeMillis();
			bucket.get("k"+i,100,TimeUnit.SECONDS);			
			incrCounter();
		}
		
	}
	
	 

}
