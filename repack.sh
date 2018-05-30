rm vsz-lambda.zip
cd .env/lib/python2.7/site-packages/
find . -type d -name tests -exec rm -rf {} +
zip -r9 ../../../../vsz-lambda.zip *
cd -
zip -g vsz-lambda.zip zipToParquet.py
