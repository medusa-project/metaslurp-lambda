package edu.illinois.library.metaslurp.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import edu.illinois.library.metaslurp.elasticsearch.Elasticsearch;

import java.util.Map;

/**
 * <p>Main class in an AWS Lambda context.</p>
 *
 * <p>Each public instance method corresponds to a "handler" which, on the AWS
 * Lambda configuration page, looks something like: {@literal
 * edu.illinois.library.metaslurp.lambda.Lambda::run}.</p>
 *
 * <p>Because Lambdas take some effort to set up on the AWS side, the idea is
 * to have only one function that invokes some other method depending on the
 * contents of the SNS notification. In particular, the {@literal Message} key
 * is expected to contain some kind of action (like
 * {@link #PURGE_DOCUMENTS_MESSAGE}), and the {@literal MessageAttributes} hash
 * may contain runtime arguments. This way, only one Lambda function will ever
 * be needed, probably.</p>
 */
public final class Lambda {

    static final String PURGE_DOCUMENTS_MESSAGE = "purgeDocuments";

    static final String PURGE_DOCUMENTS_INDEX_NAME_ATTRIBUTE = "IndexName";
    static final String PURGE_DOCUMENTS_FIELD_NAME_ATTRIBUTE = "FieldName";
    static final String PURGE_DOCUMENTS_FIELD_VALUE_ATTRIBUTE = "FieldValue";

    /**
     * Environment variable that should be set in AWS Lambda.
     */
    static final String ELASTICSEARCH_ENDPOINT_ENV =
            "ELASTICSEARCH_ENDPOINT";

    /**
     * VM argument that should be set in testing.
     */
    static final String ELASTICSEARCH_ENDPOINT_VM_ARGUMENT =
            "edu.illinois.library.metaslurp.elasticsearch_endpoint";

    /**
     * @return {@link #ELASTICSEARCH_ENDPOINT_ENV}, if defined, or {@link
     *         #ELASTICSEARCH_ENDPOINT_VM_ARGUMENT} otherwise.
     */
    private static String getElasticsearchEndpoint() {
        String endpoint = System.getenv(ELASTICSEARCH_ENDPOINT_ENV);
        if (endpoint == null) {
            endpoint = System.getProperty(ELASTICSEARCH_ENDPOINT_VM_ARGUMENT);
        }
        return endpoint;
    }

    /**
     * No-op constructor required by Lambda.
     */
    public Lambda() {}

    /**
     * The one available Lambda function, which branches out to some other
     * method depending on the value of the {@link SNSEvent} argument's
     * {@literal Message}.
     */
    public String run(SNSEvent event, Context context) throws Exception {
        // There should be only one record.
        final SNSEvent.SNSRecord record = event.getRecords().get(0);
        final SNSEvent.SNS sns = record.getSNS();

        switch (sns.getMessage()) {
            case PURGE_DOCUMENTS_MESSAGE:
                final Map<String, SNSEvent.MessageAttribute> attrs =
                        sns.getMessageAttributes();
                final String indexName =
                        attrs.get(PURGE_DOCUMENTS_INDEX_NAME_ATTRIBUTE).getValue();
                final String fieldName =
                        attrs.get(PURGE_DOCUMENTS_FIELD_NAME_ATTRIBUTE).getValue();
                final String fieldValue =
                        attrs.get(PURGE_DOCUMENTS_FIELD_VALUE_ATTRIBUTE).getValue();
                Elasticsearch es = new Elasticsearch();
                es.setEndpointURI(getElasticsearchEndpoint());
                return es.purgeDocuments(indexName, fieldName, fieldValue);
            default:
                throw new IllegalArgumentException(
                        "Unrecognized message: " + sns.getMessage());
        }
    }

}
