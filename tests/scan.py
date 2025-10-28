import os
import json
from collections import defaultdict
from ducktape.tests.loader import TestLoader
from ducktape.tests.session import SessionContext, SessionLoggerMaker

session_id = "arbitrary"
session_context = SessionContext(session_id=session_id, results_dir=".")
session_logger = SessionLoggerMaker(session_context).logger
loader = TestLoader(
    session_context=session_context,
    logger=session_logger,
)

found_tests = loader.load(symbols=["kafkatest/tests/client/consumer_test.py::OffsetValidationTest.test_broker_rolling_bounce"])

grouped_tests = defaultdict(list)
for t in found_tests:
    key = (t.file, t.cls.__name__, t.function.__name__)
    grouped_tests[key].append(t)

script_dir = os.path.dirname(os.path.abspath(__file__))
antithesis_test_dir = os.path.join(os.path.dirname(script_dir), "antithesis", "test", "v1")
os.makedirs(antithesis_test_dir, exist_ok=True)

for (file, cls, function), tests in grouped_tests.items():
    relative_file = file.replace(os.path.dirname(script_dir) + "/", "")
    
    # Create subdirectory based on the test category (e.g., "client")
    # The path structure is: tests/kafkatest/tests/client/consumer_test.py
    # So we want to extract "client" from the path
    path_parts = relative_file.split("/")
    if len(path_parts) >= 4 and path_parts[0] == "tests" and path_parts[1] == "kafkatest" and path_parts[2] == "tests":
        test_category = path_parts[3]  # "client"
    else:
        test_category = "core"
    
    test_category_dir = os.path.join(antithesis_test_dir, test_category)
    os.makedirs(test_category_dir, exist_ok=True)
    
    for i, test in enumerate(tests, 1):
        # Generate filename based on the pattern observed
        # Extract the test file name without extension (e.g., "consumer_test" from "consumer_test.py")
        test_file_name = os.path.splitext(os.path.basename(file))[0]
        filename = f"singleton_driver__{test_file_name}__{cls}__{function}__{i}.sh"
        filepath = os.path.join(test_category_dir, filename)
        
        # Generate the shell script content
        # Remove the leading "tests/" from relative_file since we're already in /opt/kafka-dev/tests/
        clean_relative_file = relative_file.replace("tests/", "", 1) if relative_file.startswith("tests/") else relative_file
        script_content = f"""#!/bin/bash
set -ex

cd /opt/kafka-dev/
ducktape --cluster-file /opt/kafka-dev/cluster.json \\
  '/opt/kafka-dev/tests/{clean_relative_file}::{cls}.{function}@{json.dumps(test.injected_args) if hasattr(test, 'injected_args') and test.injected_args else "{}"}'"""
        
        with open(filepath, 'w') as f:
            f.write(script_content + '\n')
        os.chmod(filepath, 0o755)
