package com.wangyang.cache_chemist.service;

import com.wangyang.cache_chemist.entity.Person;
import com.wangyang.cache_chemist.repo.PersonRepository;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PersonService {
    @Resource
    private PersonRepository personRepository;

    @Transactional
    public Person save(Person person) {
        return personRepository.save(person);
    }

    public Person getByName(String name) {
        return personRepository.findByName(name).stream().findFirst().orElse(null);
    }

    @Transactional
    public void updateAge(String name, Long age) {
        Person person = getByName(name);
        person.setAge(age);
    }
}
