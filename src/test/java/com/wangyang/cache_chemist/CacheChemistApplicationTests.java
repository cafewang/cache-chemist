package com.wangyang.cache_chemist;

import com.wangyang.cache_chemist.entity.Person;
import com.wangyang.cache_chemist.service.CacheAsideService;
import com.wangyang.cache_chemist.service.DtmService;
import com.wangyang.cache_chemist.service.PersonService;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootTest
class CacheChemistApplicationTests {
	@Resource
	private PersonService personService;
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Resource
	private CacheAsideService cacheAsideService;
	@Resource
	private DtmService dtmService;

	@Test
	void testCacheAside() {
		String name = "alice";
		Long age = 10L;
		cacheAsideService.addPerson(new Person(1L, name, age));
        Assertions.assertNull(stringRedisTemplate.opsForValue().get(name));
		Assertions.assertEquals(age, cacheAsideService.getAge(name));
		Long newAge = 12L;
		cacheAsideService.updateAge(name, newAge);
		Assertions.assertNull(stringRedisTemplate.opsForValue().get(name));
		Assertions.assertEquals(newAge, cacheAsideService.getAge(name));
		stringRedisTemplate.opsForValue().getAndDelete(name);
	}

	@Test
	void testCacheAsideCornerCase() {
		String name = "alice";
		Long age = 10L;
		cacheAsideService.addPerson(new Person(1L, name, age));
		CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> cacheAsideService.getAge(name, 1000L));
		Long newAge = 12L;
		CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
			cacheAsideService.updateAge(name, newAge);
			cacheAsideService.getAge(name);
		});
        try {
            future1.get();
			future2.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
		Assertions.assertEquals(age, cacheAsideService.getAge(name));
		Assertions.assertEquals(newAge, personService.getByName(name).getAge());
		stringRedisTemplate.opsForValue().getAndDelete(name);
    }

	@Test
	void testDtm() throws InterruptedException {
		String name = "bob";
		Long age = 10L;
		dtmService.addPerson(new Person(1L, name, age));
		Assertions.assertEquals(age, dtmService.getAge(name));
		Long newAge = 12L;
		dtmService.updateAge(name, newAge);
		Assertions.assertEquals(age, dtmService.getAge(name));
		Thread.sleep(100);
		Assertions.assertEquals(newAge, dtmService.getAge(name));
		stringRedisTemplate.opsForHash().delete(name, "data", "owner", "lockUntil");
	}

	@Test
	void testDtmCornerCase() throws InterruptedException {
		String name = "bob";
		Long age = 10L;
		dtmService.addPerson(new Person(1L, name, age));
		CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> dtmService.getAge(name, 1000L));
		Long newAge = 12L;
		CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
			dtmService.updateAge(name, newAge);
			dtmService.getAge(name);
		});
		try {
			future1.get();
			future2.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
		Assertions.assertEquals(newAge, personService.getByName(name).getAge());
		Assertions.assertEquals(newAge, dtmService.getAge(name));
		stringRedisTemplate.opsForHash().delete(name, "data", "owner", "lockUntil");
	}

}
