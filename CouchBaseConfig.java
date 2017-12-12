package com.db.springbootcouchbase.config;

/**
 * Created by RV00451128 on 8/2/2017.
 */
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;

import java.util.Arrays;
import java.util.List;

/**
 * Created by RV00451128 on 7/19/2017.
 */
@Configuration
@EnableCouchbaseRepositories
public class CouchBaseConfig extends AbstractCouchbaseConfiguration {

    @Autowired
    private Environment env;

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${spring.couchbase.bucket.name}")
    private String bucket;

    @Value("${spring.couchbase.bucket.password}")
    private String password;

    @Value("${spring.couchbase.bootstrap-hosts}")
    private String hosts;

    @Override
    protected List<String> getBootstrapHosts() {
        return Arrays.asList(this.hosts);
    }

    @Override
    protected String getBucketName() {
        return this.bucket;
    }

    @Override
    protected String getBucketPassword() {
        return this.password;
    }

    @Bean
    public Cluster cluster() {
        return CouchbaseCluster.create(hosts);
    }

    @Bean
    public Bucket bucket() {
        return cluster().openBucket(bucket, password);
    }
}

