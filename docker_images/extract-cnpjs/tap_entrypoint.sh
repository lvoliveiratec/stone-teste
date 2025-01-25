#!/bin/sh
if [ -z "$B64_CONFIG" ]
then
    # NOT BASE64
    $TAP_NAME -c $CONF_LOCATION --catalog $SCHEMA_FILE -p $SCHEMA_FILE
else
    # BASE64
    mkdir /tmp/schemas
    SCHEMA_FILE='/tmp/schemas/schema${REQUEST}.json'
    echo $B64_SCHEMA | base64 -d > $SCHEMA_FILE
    CONF_LOCATION='/tmp/tap_config${REQUEST}.json'
    echo $B64_CONF_FILE | base64 -d > $CONF_LOCATION

    # EXECUTE SINGER TAP
    $TAP_NAME -c $CONF_LOCATION --catalog $SCHEMA_FILE -p $SCHEMA_FILE
fi
