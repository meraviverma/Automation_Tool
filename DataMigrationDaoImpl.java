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
 * Created by Ravi Verma on 12/7/2017.
 */
@Slf4j
@Service
public class DataMigrationDaoImpl implements DataMigrationDao {

    @Autowired
    AbstractCouchbaseConfiguration abstractCouchbaseConfiguration;

    @Autowired
    private CouchBaseConfig couchBaseConfig;

    private final String docType = "facHrr";

    @Override
    public void updateFieldValue() throws Exception{

        Bucket bucket=couchBaseConfig.bucket();
        String bucketName=couchBaseConfig.bucket().name();

        //get docId , facCountryCode,facLocNr from facility document.
        N1qlQuery Query = N1qlQuery.parameterized(" SELECT meta(`" + bucketName + "`).id AS docId,facCountryCode,facLocNr FROM `" + bucketName + "` WHERE docType=$1", JsonArray.from(docType));
        N1qlQueryResult queryResult = bucket.query(Query);

        try {
            for (N1qlQueryRow result: queryResult) {
                JsonObject obj = result.value();
                String docId=(String) obj.get("docId");

                //form orgDocType from facility document as crdOgzCur-AD-8061
                String orgDocType="crdOgzCur-"+(String) obj.get("facCountryCode")+"-"+(String) obj.get("facLocNr");
                //form crdFacDocType
                String crdFacDocType="crdFacCur-"+docId;


                JsonDocument jsonDocument = bucket.get(orgDocType);
                String regionNr=(String)jsonDocument.content().get("regionNr");//get regionNr from orgDocType
                String disNr=(String)jsonDocument.content().get("districtNr");//get districtNr from orgDocType

               /* System.out.println("*******orgdoctype******"+orgdoctype);
                System.out.println("*****crdfacdoctype********"+crdfacdoctype);
                System.out.println("******obj*******"+regionNr);
                System.out.println("******obj*******"+disNr);*/

               //update  facility document
                bucket.mutateIn(docId)
                        .upsert("ownRegNr",regionNr,true)
                        .upsert("ownDisNr",disNr,true)
                        .execute();
                
                //update crd document
                bucket.mutateIn(crdFacDocType)
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
