package com.wangyang.cache_chemist.service;

import com.wangyang.cache_chemist.entity.Person;
import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

@Service
public class CacheAsideService {
    @Resource
    private PersonService personService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Long getAge(String name, Long sleep) {
        String ageStr = stringRedisTemplate.opsForValue().get(name);
        if (Objects.nonNull(ageStr)) {
            return Long.parseLong(ageStr);
        }
        Person person = personService.getByName(name);
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (Objects.isNull(person)) {
            return null;
        }
        String newAgeStr = person.getAge().toString();
        stringRedisTemplate.opsForValue().set(name, newAgeStr, Duration.of(1, ChronoUnit.MINUTES));
        return person.getAge();
    }

    public Long getAge(String name) {
        return getAge(name, 0L);
    }

    public void addPerson(Person person) {
        personService.save(person);
        stringRedisTemplate.opsForValue().getAndDelete(person.getName());
    }

    public void updateAge(String name, Long age) {
        personService.updateAge(name, age);
        stringRedisTemplate.opsForValue().getAndDelete(name);
    }
}
