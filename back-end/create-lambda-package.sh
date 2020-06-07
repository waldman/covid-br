#!/bin/bash

#
# create-lambda-package.sh - Creates the AWS lambda package
#

# Cleanup
rm -rf ./package
rm function.zip

# Creates
pip install --target ./package -r requirements.txt
cd package
zip -r9 ${OLDPWD}/function.zip .
cd $OLDPWD
zip -g function.zip lambda_function.py

