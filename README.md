# About

The [Metaslurp](https://github.com/medusa-project/metaslurp) gateway
application sends SNS notifications to trigger various tasks that are expected
to take longer than a couple seconds but no more than a few minutes to
complete. This project contains the Lambda functions that are triggered in
response to these notifications.

See the `edu.illinois.library.metaslurp.lambda.Lambda` class for available
functions.

# Requirements

* JDK 8
* Maven 3

# Deploy

## Automatic

Run `deploy.sh`

## Manual

1. `mvn clean package -DskipTests`
2. Upload `target/metaslurp-lambda-x.x.x.jar` to Lambda using the AWS web
   interface.

# Test

`mvn clean test`

You must have an Elasticsearch server listening on `http://localhost:9200`.
