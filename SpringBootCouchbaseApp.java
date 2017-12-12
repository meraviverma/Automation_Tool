package com.db.springbootcouchbase;

import com.db.springbootcouchbase.service.DataMigrationService;
import com.fasterxml.jackson.core.JsonParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@ComponentScan
public class SpringBootCouchbaseApp implements CommandLineRunner {

    @Autowired
    private DataMigrationService dataMigrationService;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootCouchbaseApp.class, args);
    }
    @Override
    public void run(String... strings) throws Exception {
       dataMigrationService.updatefields();
            System.exit(1);
    }
}
