package com.yt.dao;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Repository;

import com.yt.entity.User;
/**
 * Title: UserDao.java
 * Description: 
 * Company: HundSun
 * @author yangting
 * @date 2017年7月3日 上午11:01:06
 * @version 1.0
 */
@Repository
public class UserDao extends AbstractBaseRedisDao<String, User> implements IUserDao{
	private int threadNumber;
	private int readTimes;
	private int writeTimes;
	private int testDataSize;
	private String testMethod;
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
	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}
	public int getThreadNumber() {
		return threadNumber;
	}
	public boolean add(final User user) {
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {

				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				byte[] name = serializer.serialize(user.getName());
				connection.setNX(key, name);
				return true;
			}
		});
		return result;
	}
	public boolean addHashMap(final User user){
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {

				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				
				connection.hMSet(key, user.getHashMap());
				return true;
			}
		});
		return result;
	}

	public boolean add(final List<User> users) {
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {

				RedisSerializer<String> serializer = getRedisSerializer();
				for(User user : users){
					byte[] key = serializer.serialize(user.getId());
					byte[] name = serializer.serialize(user.getName());
					connection.setNX(key, name);
				}
				return true;
			}
		});
		return result;
	}

	public void delete(final String key) {
		redisTemplate.execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				connection.del(serializer.serialize(key));
				return true;
			}
		});
		return ;
	}

	public void delete(final List<String> keys) {
		redisTemplate.execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				for(String key:keys)
					connection.del(serializer.serialize(key));
				return true;
			}
		});
		return ;
	}

	public boolean update(final User user) {
	

		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
	
				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				byte[] name = serializer.serialize(user.getName());
				
				connection.set(key, name);
				return true;
			}
		});
		return result;
	}

	public User get(final String keyId) {
		User result = redisTemplate.execute(new RedisCallback<User>() {  
		public User doInRedis(RedisConnection connection)  throws DataAccessException {  

				RedisSerializer<String> serializer = getRedisSerializer();  
				byte[] key = serializer.serialize(keyId); 
				connection.get(key);
				return null;
								}  
			},false,false);  
			return result;  

	}
	public User getMap(final String keyId) {
		User result = redisTemplate.execute(new RedisCallback<User>() {  
		public User doInRedis(RedisConnection connection)  throws DataAccessException {  

				RedisSerializer<String> serializer = getRedisSerializer();  
				byte[] key = serializer.serialize(keyId); 
				connection.hGetAll(key);
				return null;
								}  
			},false,false);  
			return result;  

	}

}
