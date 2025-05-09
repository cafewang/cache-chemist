package com.wangyang.cache_chemist.service;

import com.wangyang.cache_chemist.entity.Person;
import jakarta.annotation.Resource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class DtmService {
    @Resource
    private PersonService personService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CacheStatus {
        private Long data;
        private String newLock;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class FetchResult {
        private Long data;
        private Boolean success;
    }

    public Long getAge(String name, Long sleep) {
        CacheStatus cacheStatus;
        while (Objects.isNull((cacheStatus = getAndLock(name, 50)).getData())
            && Objects.isNull(cacheStatus.getNewLock())) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        String newLock = cacheStatus.getNewLock();
        if (Objects.nonNull(newLock)) {
            if (Objects.isNull(cacheStatus.getData())) {
                FetchResult fetchResult = fetchData(name, newLock, sleep);
                return fetchResult.getSuccess() ? fetchResult.getData() : getAge(name);
            }
            CompletableFuture.runAsync(() -> fetchData(name, newLock, sleep));
        }
        return cacheStatus.getData();
    }

    public CacheStatus getAndLock(String name, long lockTimeMs) {
        String luaScript = "local function timestamp() " +
                "local time = redis.call('time');" +
                "return (time[1] * 1000 + time[2] / 1000) end;" +
                "local data = redis.call('hget', KEYS[1], 'data');" +
                "local owner = redis.call('hget', KEYS[1], 'owner');" +
                "local lockUntil = redis.call('hget', KEYS[1], 'lockUntil');" +
                "if (lockUntil and tonumber(lockUntil) > timestamp()) then " +
                "return {data, false};" +
                "else " +
                "redis.call('hset', KEYS[1], 'owner', KEYS[2]);" +
                "redis.call('hset', KEYS[1], 'lockUntil', string.format('%.0f', timestamp() + tonumber(KEYS[3])));" +
                "return {data, KEYS[2]};" +
                "end";
        String owner = UUID.randomUUID().toString();
        DefaultRedisScript<List> script = new DefaultRedisScript<>(luaScript, List.class);
        List<Object> result = stringRedisTemplate.execute(script, List.of(name, owner, String.valueOf(lockTimeMs)));
        Long data = Optional.ofNullable(result.get(0)).map(Object::toString).map(Long::parseLong).orElse(null);
        String newLock = Optional.ofNullable(result.get(1)).map(Object::toString).orElse(null);
        return new CacheStatus(data, newLock);
    }

    private FetchResult fetchData(String name, String newLock, Long sleep) {
        Person person = personService.getByName(name);
        if (Objects.isNull(person)) {
            return new FetchResult(null, false);
        }
        Long age = person.getAge();
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        boolean success = setIfOwnerUnchanged(name, age, newLock);
        return new FetchResult(age, success);
    }

    public boolean setIfOwnerUnchanged(String name, Long age, String newLock) {
        String luaScript =
                "local owner = redis.call('hget', KEYS[1], 'owner');" +
                "if (owner and owner == KEYS[3]) then " +
                "redis.call('hset', KEYS[1], 'data', KEYS[2]);" +
                "redis.call('hexpire', KEYS[1], '10', 'FIELDS', '1', 'data');" +
                "redis.call('hset', KEYS[1], 'lockUntil', '0');" +
                "return 1;" +
                "else " +
                "return 0;" +
                "end";
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(luaScript, Long.class);
        Long result = stringRedisTemplate.execute(script, List.of(name, age.toString(), newLock));
        return result == 1;
    }

    public Long getAge(String name) {
        return getAge(name, 0L);
    }

    public void addPerson(Person person) {
        personService.save(person);
        invalidateCache(person.getName());
    }

    private void invalidateCache(String name) {
        String luaScript = "if redis.call('hget', KEYS[1], 'lockUntil') then " +
                "return redis.call('hset', KEYS[1], 'lockUntil', '0') " +
                "else return 0 end";
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(luaScript, Long.class);
        stringRedisTemplate.execute(script, List.of(name));
    }

    public void updateAge(String name, Long age) {
        personService.updateAge(name, age);
        invalidateCache(name);
    }
}
