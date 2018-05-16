package edu.illinois.library.metaslurp.elasticsearch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ElasticsearchTest {

    private static final String INDEX_NAME =
            ElasticsearchTest.class.getName().replace(".", "-").toLowerCase();

    private static final int INDEX_WAIT_MSEC = 2000;

    private Elasticsearch instance;

    @Before
    public void setUp() {
        instance = new Elasticsearch();
        instance.setEndpointURI("http://localhost:9200");
    }

    @After
    public void tearDown() throws Exception {
        instance.deleteIndex(INDEX_NAME);
    }

    @Test
    public void createIndex() {
        // TODO: write this
    }

    @Test
    public void deleteIndex() {
        // TODO: write this
    }

    @Test
    public void indexDocument() {
        // TODO: write this
    }

    @Test
    public void numDocuments() {
        // TODO: write this
    }

    @Test
    public void purgeDocuments() throws Exception {
        instance.deleteIndex(INDEX_NAME);
        instance.createIndex(INDEX_NAME);

        // Add some documents
        instance.indexDocument(INDEX_NAME, "doc", "1", "animal", "cat");
        instance.indexDocument(INDEX_NAME, "doc", "2", "animal", "dog");
        instance.indexDocument(INDEX_NAME, "doc", "3", "animal", "fox");

        Thread.sleep(INDEX_WAIT_MSEC);
        assertEquals(3, instance.numDocuments(INDEX_NAME));

        instance.purgeDocuments(INDEX_NAME, "animal", "cat");

        Thread.sleep(INDEX_WAIT_MSEC);
        assertEquals(2, instance.numDocuments(INDEX_NAME));
    }

}
