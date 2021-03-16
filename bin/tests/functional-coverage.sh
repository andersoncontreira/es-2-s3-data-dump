coverage run -m unittest discover -s ./tests/functional -t ./
coverage report
coverage xml -o ./target/functional/report.xml
coverage html --omit="*/test*,venv/*,vendor/*" -d ./target/functional/coverage_html/
coverage2clover -i ./target/functional/report.xml -o ./target/functional/clover.xml
echo 'results generated in ./target/functional/coverage_html/'
