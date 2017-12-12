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

/**
 * Created by RV00451128 on 12/7/2017.
 */
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
               // String docId = (String) obj.get("facCountryCode");
                System.out.println("*******orgdoctype******"+orgdoctype);
                System.out.println("*****crdfacdoctype********"+crdfacdoctype);
                System.out.println("******obj*******"+regionNr);
                System.out.println("******obj*******"+disNr);

                bucket.mutateIn(docid)
                        .upsert("ownRegNr",regionNr,true)
                        .upsert("ownDisNr",disNr,true)
                        .execute();

                bucket.mutateIn(crdfacdoctype)
                        .upsert("ownRegNr",regionNr,true)
                        .upsert("ownDisNr",disNr,true)
                        .execute();
               /* N1qlQuery Query1 = N1qlQuery.parameterized(" SELECT meta(`" + bucketName + "`).id AS regionNr,districtNr FROM `" + bucketName + "` WHERE docType=$1", JsonArray.from(orgdoctype));
                N1qlQueryResult queryResult1 = bucket.query(Query1);
                try {
                    for (N1qlQueryRow result1: queryResult1) {
                        JsonObject obj1 = result1.value();
                        System.out.println("******obj*******"+obj1);

                    }}catch(Exception e) {
                        info(e.getMessage());
                        System.out.println("Error found!!!");
                    }*/

               /* for (JSONObject fieldsTobeAdded: fieldDetails) {
                    if(fieldsTobeAdded.get("fieldtype").toString().equals(String.class)){

                    }
                    bucket.mutateIn(docId).insert(fieldsTobeAdded.get("fieldname").toString(), null, false).execute();
                }*/
            }
        } catch(Exception e) {
            info(e.getMessage());
            System.out.println(e.getMessage()+"Error found!!!");
        }

    }
}
