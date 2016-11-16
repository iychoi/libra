#!/bin/bash
LIST_FILE=$1

while IFS='' read -r line || [[ -n "$line" ]]; do
    FILE=$line
    echo "downloading... ${FILE}"
    FILE_NAME=$(basename "${FILE}")
    FINAL_FILE_NAME=${FILE_NAME}
    wget -O ${FILE_NAME} ${FILE}
    case "${FILE_NAME}" in
    *.gz )
            # it's gzipped
            FINAL_FILE_NAME=$(basename "${FILE_NAME}" ".gz")
            zcat ${FILE_NAME} > ${FINAL_FILE_NAME}
            ;;
    *)
            # it's not
            ;;
    esac

    hadoop dfs -put ${FINAL_FILE_NAME} 1000genome/

    rm ${FINAL_FILE_NAME}
    rm ${FILE_NAME}
done < $LIST_FILE
