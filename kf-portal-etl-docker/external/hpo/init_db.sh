#!/bin/bash

mysql --user=root --password=${MYSQL_ROOT_PASSWORD} ${MYSQL_DATABASE} < /HPO/${HPO_SQL_FILE_NAME}