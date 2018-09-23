#!/bin/bash

# Export Application home directory
APP_HOME=C:\\Users\\Amit\\Desktop
CONF_DIR=$APP_HOME\\config

# Source setup file - Environment variable
if [ -n "$APP_HOME" ]
then
    . $CONF_DIR\\setEnv.sh
else
    echo "Application Directory not set" | logger_info
    exit 1
fi

# Source setup file - Application variable
if [ -n "$APP_HOME" ]
then
    . $CONF_DIR\\setAppl.sh
else
    echo "Application Directory not set" | logger_info
    exit 1
fi

# Creating return value
iRet=0

echo "spark-submit --class com.myCompany.scalaExample.MaxPrice --master local --executor-memory ${SPEND_EXECUTOR_MEMORY} ${JAR_DIR}\\${JAR_NAME} ${INPUT_FILE} 
${OUTPUT_FILE}"

spark-submit --class com.myCompany.scalaExample.MaxPrice --master local --executor-memory ${SPEND_EXECUTOR_MEMORY} ${JAR_DIR}\\${JAR_NAME} ${INPUT_FILE} ${OUTPUT_FILE}

iRet=$?
  if [ $iRet -ne 0 ]
  then
    echo "Error: Spark program ended with error" 
  else
    echo "Spark program ended successfully"
  fi