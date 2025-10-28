#!/bin/bash
set -ex

cd /opt/kafka-dev/
ducktape --cluster-file /opt/kafka-dev/cluster.json \
  '/opt/kafka-dev/tests/kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_broker_rolling_bounce@{"metadata_quorum": "ISOLATED_KRAFT", "group_protocol": "consumer"}'
