package com.db.springbootcouchbase.config;

import com.couchbase.client.java.*;

import com.couchbase.client.java.document.RawJsonDocument;

import com.couchbase.client.java.error.CASMismatchException;

import com.couchbase.client.java.error.DocumentAlreadyExistsException;

import com.couchbase.client.java.error.DocumentDoesNotExistException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.stereotype.Component;

import rx.Observable;

import rx.functions.Func1;

import rx.functions.Func2;




import javax.annotation.PostConstruct;

import java.io.IOException;
import java.util.Arrays;

import java.util.NoSuchElementException;

import java.util.concurrent.TimeUnit;

import java.util.logging.Logger;
@Component
public class CouchbaseWrapper {

    private static final Logger log = Logger.getLogger(CouchbaseWrapper.class.getName());

    @Autowired
    CouchBaseConfig couchBaseConfig;

    @Autowired
    private Bucket bucket;

    ObjectMapper mapper = new ObjectMapper();

    public <E> Observable<E> get(String id, Class<E> className) {

        return bucket.async()

                .get(id, RawJsonDocument.class)

                .retry(retryPredicate())

                .onErrorResumeNext(bucket.async().getFromReplica(id, ReplicaMode.ALL, RawJsonDocument.class))

                .doOnError(e -> log.info("failed to get document: " + e.getMessage()))

                .map(toEntity(className));

    }

    private Func2<Integer, Throwable, Boolean> retryPredicate() {

        return (count, throwable) -> !(throwable instanceof CASMismatchException) &&

                !(throwable instanceof NoSuchElementException) &&

                !(throwable instanceof DocumentAlreadyExistsException) &&

                !(throwable instanceof DocumentDoesNotExistException) &&

                count < 3;

    }

 private <E> RawJsonDocument toRawJsonDocument(String id, E entity) {
        String json = (String)entity;
        return RawJsonDocument.create(id, json);
    }


    private <E> Func1<RawJsonDocument, E> toEntity(Class<E> className) {
        return doc -> {
            String content = doc.content();
            E baseEntity = null;
            try {
                baseEntity = mapper.readValue(content, className);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return baseEntity;
        };
    }






}

