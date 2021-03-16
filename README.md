#es-2-s3-data-dump
Elasticsearch to s3 data dump 


## Prerequisites
- Python 3.6
- python-dotenv
- pytz
- boto3


## Installation

### Running Locally

To create the `venv` and install the modules execute:
```
./bin/venv.sh
```
If you don't want to create the venv, execute the follow commands:
```
./bin/install.sh
```

Execute the follow command:
```
./bin/runlocal.sh
```

[comment]: <> (## Samples)

[comment]: <> (See the project samples in this folder [here]&#40;samples&#41;.)

[comment]: <> (## Running tests)

[comment]: <> (To run the unit tests of the project you can execute the follow command:)

[comment]: <> (First you need install the tests requirements:)

[comment]: <> ( ```)

[comment]: <> ( ./bin/venv-exec.sh ./bin/tests/install-tests.sh )

[comment]: <> ( ```)

 
[comment]: <> (### Unit tests:)

[comment]: <> ( ```)

[comment]: <> (./bin/venv-exec.sh ./bin/tests/unit-tests.sh)

[comment]: <> ( ``` )

[comment]: <> (### Functional tests:)

[comment]: <> (First install the dynamodb locally:)

[comment]: <> (```)

[comment]: <> (./bin/aws/install-dynamodb-local.sh)

[comment]: <> (```)

[comment]: <> (Now run the dynamodb locally:)

[comment]: <> (```)

[comment]: <> (./bin/aws/run-dynamodb-local.sh --port 9000)

[comment]: <> (```)

[comment]: <> (Executing the tests:)

[comment]: <> ( ```)

[comment]: <> (./bin/venv-exec.sh ./bin/tests/functional-tests.sh)

[comment]: <> (```)

[comment]: <> (### All tests:)

[comment]: <> (Run the dynamodb locally:)

[comment]: <> (```)

[comment]: <> (./bin/aws/run-dynamodb-local.sh --port 9000)

[comment]: <> (``` )

[comment]: <> (Executing the tests:)

[comment]: <> (```)

[comment]: <> ( ./bin/venv-exec.sh ./bin/tests/tests.sh )

[comment]: <> ( ```)

[comment]: <> (## Generating coverage reports)

[comment]: <> (To execute coverage tests you can execute the follow commands:)

[comment]: <> (Unit test coverage:)

[comment]: <> (``` )

[comment]: <> (./bin/venv-exec.sh ./bin/tests/unit-coverage.sh)

[comment]: <> (``` )

[comment]: <> (Functional test coverage:)

[comment]: <> (``` )

[comment]: <> (./bin/venv-exec.sh ./bin/tests/functional-coverage.sh)

[comment]: <> (``` )

[comment]: <> (> Observation:)

[comment]: <> (The result can be found in the folder `target/functional` and `target/unit`.)


[comment]: <> (## License)

[comment]: <> (See the license [LICENSE.md]&#40;LICENSE.md&#41;.)

## Contributions
* Anderson de Oliveira Contreira [andersoncontreira](https://github.com/andersoncontreira)