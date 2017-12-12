package com.db.springbootcouchbase.service;

import com.db.springbootcouchbase.data.DataMigrationDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DataMigrationServiceImpl implements DataMigrationService{
    @Autowired
    private DataMigrationDao dataMigrationDao;

    public void updatefields() throws Exception{
        dataMigrationDao.updatefieldvalue();
    }
}
