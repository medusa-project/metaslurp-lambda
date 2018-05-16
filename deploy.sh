#!/bin/sh
#
# Compiles the application, bundles it into a JAR, and deploys it to AWS.
#

mvn clean package -DskipTests
aws lambda update-function-code --function-name metaslurp-dev \
    --zip-file fileb://$(ls target/metaslurp-lambda-*.jar)
