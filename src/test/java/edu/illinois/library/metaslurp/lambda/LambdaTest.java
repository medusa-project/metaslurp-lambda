package edu.illinois.library.metaslurp.lambda;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import edu.illinois.library.metaslurp.elasticsearch.Elasticsearch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static edu.illinois.library.metaslurp.lambda.Lambda.PURGE_DOCUMENTS_MESSAGE;
import static org.junit.Assert.*;

public class LambdaTest {

    private static final String INDEX_NAME =
            LambdaTest.class.getName().replace(".", "-").toLowerCase();

    private static final int INDEX_WAIT_MSEC = 2000;

    private Elasticsearch elasticsearch;
    private Lambda instance;

    private static SNSEvent newPurgeDocumentsEvent(String indexName,
                                                   String fieldName,
                                                   String fieldValue) {
        Map<String, SNSEvent.MessageAttribute> attrs = new HashMap<>();
        // index name
        SNSEvent.MessageAttribute attr = new SNSEvent.MessageAttribute();
        attr.setType("String");
        attr.setValue(indexName);
        attrs.put(Lambda.PURGE_DOCUMENTS_INDEX_NAME_ATTRIBUTE, attr);
        // field name
        attr = new SNSEvent.MessageAttribute();
        attr.setType("String");
        attr.setValue(fieldName);
        attrs.put(Lambda.PURGE_DOCUMENTS_FIELD_NAME_ATTRIBUTE, attr);
        // field value
        attr = new SNSEvent.MessageAttribute();
        attr.setType("String");
        attr.setValue(fieldValue);
        attrs.put(Lambda.PURGE_DOCUMENTS_FIELD_VALUE_ATTRIBUTE, attr);

        SNSEvent.SNS sns = new SNSEvent.SNS();
        sns.setMessage(PURGE_DOCUMENTS_MESSAGE);
        sns.setMessageAttributes(attrs);
        SNSEvent.SNSRecord record = new SNSEvent.SNSRecord();
        record.setSns(sns);
        SNSEvent event = new SNSEvent();
        event.setRecords(Collections.singletonList(record));
        return event;
    }

    @Before
    public void setUp() {
        System.setProperty(Lambda.ELASTICSEARCH_ENDPOINT_VM_ARGUMENT,
                "http://localhost:9200");

        elasticsearch = new Elasticsearch();
        elasticsearch.setEndpointURI(System.getProperty(
                Lambda.ELASTICSEARCH_ENDPOINT_VM_ARGUMENT));

        instance = new Lambda();
    }

    @After
    public void tearDown() throws Exception {
        elasticsearch.deleteIndex(INDEX_NAME);
    }

    @Test
    public void run() throws Exception {
        elasticsearch.deleteIndex(INDEX_NAME);
        elasticsearch.createIndex(INDEX_NAME);

        // Add some documents
        elasticsearch.indexDocument(INDEX_NAME, "doc", "1", "animal", "cat");
        elasticsearch.indexDocument(INDEX_NAME, "doc", "2", "animal", "dog");
        elasticsearch.indexDocument(INDEX_NAME, "doc", "3", "animal", "fox");

        Thread.sleep(INDEX_WAIT_MSEC);
        assertEquals(3, elasticsearch.numDocuments(INDEX_NAME));

        final String fieldName = "animal";
        final String fieldValue = "cat";
        final SNSEvent event = newPurgeDocumentsEvent(
                INDEX_NAME, fieldName, fieldValue);

        instance.run(event, null);

        Thread.sleep(INDEX_WAIT_MSEC);
        assertEquals(2, elasticsearch.numDocuments(INDEX_NAME));
    }

}
