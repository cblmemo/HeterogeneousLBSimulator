#! /bin/bash
echo "Running black..."
black *.py
echo "Running mypy..."
mypy *.py
echo "Running isort..."
isort *.py
echo "Running pylint..."
pylint --disable=missing-module-docstring,missing-class-docstring,missing-function-docstring,invalid-name,unused-import,unused-argument,too-few-public-methods,duplicate-code,too-many-locals,too-many-instance-attributes,too-many-arguments,too-many-branches,fixme,too-many-statements,unspecified-encoding,use-dict-literal *.py