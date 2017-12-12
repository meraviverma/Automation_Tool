package com.db.springbootcouchbase.data;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.db.springbootcouchbase.config.CouchBaseConfig;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.stereotype.Service;

import static org.aspectj.bridge.MessageUtil.info;

@Slf4j
@Service
public class DataMigrationDaoImpl implements DataMigrationDao {

    private final String docType = "facHrr";
    @Autowired
    AbstractCouchbaseConfiguration abstractCouchbaseConfiguration;

    @Autowired
    private CouchBaseConfig couchBaseConfig;
    String bucketName="CIPE_HT";

    @Override
    public void updatefieldvalue() throws Exception{
        Bucket bucket = couchBaseConfig.couchbaseCluster().openBucket(bucketName, "");
        N1qlQuery Query = N1qlQuery.parameterized(" SELECT meta(`" + bucketName + "`).id AS docId,facCountryCode,facLocNr FROM `" + bucketName + "` WHERE docType=$1", JsonArray.from(docType));
        N1qlQueryResult queryResult = bucket.query(Query);

        try {
            for (N1qlQueryRow result: queryResult) {
                JsonObject obj = result.value();
                String docid=(String) obj.get("docId");
                String orgdoctype="crdOgzCur-"+(String) obj.get("facCountryCode")+"-"+(String) obj.get("facLocNr");
                String crdfacdoctype="crdFacCur-"+docid;

                //dataMigrationRepo.findOne(orgdoctype);
                JsonDocument jsonDocument = bucket.get(orgdoctype);
                String regionNr=(String)jsonDocument.content().get("regionNr");
                String disNr=(String)jsonDocument.content().get("districtNr");
          
                bucket.mutateIn(docid)
                        .upsert("ownRegNr",regionNr,true)
                        .upsert("ownDisNr",disNr,true)
                        .execute();

                bucket.mutateIn(crdfacdoctype)
                        .upsert("ownRegNr",regionNr,true)
                        .upsert("ownDisNr",disNr,true)
                        .execute();
               
            }
        } catch(Exception e) {
            info(e.getMessage());
            System.out.println(e.getMessage()+"Error found!!!");
        }

    }
}
