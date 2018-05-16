package edu.illinois.library.metaslurp.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

public class Elasticsearch {

    private static final int CONNECT_TIMEOUT = 10000;

    // This is locked to localhost, but that's OK for now as it's only used in
    // testing
    private static final RestHighLevelClient CLIENT = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));

    private String endpointURI;

    public void createIndex(String indexName) throws IOException {
        CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
        CLIENT.indices().create(createRequest);
    }

    public void deleteIndex(String indexName) throws IOException {
        try {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
            CLIENT.indices().delete(deleteRequest);
        } catch (ElasticsearchStatusException e) {
            if (e.status().getStatus() != 404) {
                throw e;
            }
        }
    }

    /**
     * @param indexName Index name.
     * @param type      Document type.
     * @param id        Document ID.
     * @param source    Array of key-value pairs with keys at even positions
     *                  and values at odd positions.
     */
    public void indexDocument(String indexName,
                              String type,
                              String id,
                              Object... source) throws IOException {
        IndexRequest request = new IndexRequest(indexName, type, id).source(source);
        CLIENT.index(request);
    }

    public long numDocuments(String indexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = CLIENT.search(searchRequest);
        return response.getHits().getTotalHits();
    }

    /**
     * Deletes all Elasticsearch documents containing a given field matching
     * the given value in the given index.
     *
     * @param indexName  Index name.
     * @param fieldName  Field name.
     * @param fieldValue Field value.
     * @return Response entity from Elasticsearch.
     */
    public String purgeDocuments(String indexName,
                                 String fieldName,
                                 String fieldValue) throws Exception {
        final String query = String.format(
                "{" +
                    "\"query\":{" +
                        "\"bool\":{" +
                            "\"filter\":[" +
                                "{" +
                                    "\"term\":{" +
                                        "\"%s\":\"%s\"" +
                                    "}" +
                                "}" +
                            "]" +
                        "}" +
                    "}" +
                "}", fieldName, fieldValue);

        // N.B.: Ideally we would be using an official Elasticsearch client
        // here, but the recommended client (as of 6.2) is the Java High Level
        // REST Client, which doesn't yet support delete-by-query.
        final String uri = String.format("%s/%s/_delete_by_query?pretty",
                endpointURI, indexName);
        final HttpURLConnection conn = (HttpURLConnection) new URL(uri).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(query.getBytes("UTF-8"));
        }

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return reader.lines().collect(Collectors.joining());
        }
    }

    public void setEndpointURI(String endpointURI) {
        this.endpointURI = endpointURI;
    }

}
