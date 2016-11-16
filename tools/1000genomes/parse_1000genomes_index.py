#! /usr/bin/env python

import os
import os.path
import sys
import random
import json

MIN_BP=1*1024*1024*1024
MAX_BP=5*1024*1024*1024
URL_FIELD_IDX=0
POPULATION_CODE_FIELD_IDX=10
BASEPAIRS_FIELD_IDX=24

def selectSamples(path, samples=3, minbp=MIN_BP, maxbp=MAX_BP):
    samples_selected = {}
    with open(path, 'r') as f:
        for line in f:
            if line.startswith("#"):
                continue

            fields = line.split('\t')
            try:
                url = fields[URL_FIELD_IDX]
                if not url.endswith("fastq.gz"):
                    continue

                population_code = fields[POPULATION_CODE_FIELD_IDX]
                basepairs = int(fields[BASEPAIRS_FIELD_IDX])

                if population_code not in samples_selected:
                    samples_selected[population_code] = []

                samples_selected[population_code].append(fields)
            except Exception as e:
                pass

    # sort
    for key in samples_selected.keys():
        bucket = samples_selected[key]
        def getSortKey(item):
            # basepairs
            return int(item[BASEPAIRS_FIELD_IDX])
        bucket.sort(key=getSortKey)

    for key in samples_selected.keys():
        bucket = samples_selected[key]
        start = 0
        for i in range(0, len(bucket)):
            if int(bucket[i][BASEPAIRS_FIELD_IDX]) < minbp:
                #print key, bucket[i][BASEPAIRS_FIELD_IDX]
                start = i+1
            else:
                break
        bucket = bucket[start:]
        samples_selected[key] = bucket

    for key in samples_selected.keys():
        bucket = samples_selected[key]
        end = 0
        for i in range(0, len(bucket)):
            if int(bucket[i][BASEPAIRS_FIELD_IDX]) <= maxbp:
                #print key, bucket[i][BASEPAIRS_FIELD_IDX]
                end = i+1
            else:
                break
        bucket = bucket[:end]
        samples_selected[key] = bucket

    # pick samples
    for key in samples_selected.keys():
        bucket = samples_selected[key]
        samples_selected[key] = random.sample(bucket, samples)

    # print
    result = []
    for key in samples_selected.keys():
        bucket = samples_selected[key]
        #print "%s: %d" % (key, len(bucket))

        for i in range(0, len(bucket)):
            item = {}
            item["population"] = key
            item["path"] = bucket[i][URL_FIELD_IDX]
            item["basepairs"] = int(bucket[i][BASEPAIRS_FIELD_IDX])
            result.append(item)
            #print bucket[i][0]

    return result

def main(argv):
    index_file = argv[0]
    arg = None
    if len(argv) > 1:
        arg = argv[1]

    selectedItems = selectSamples(index_file)
    with open("selected.json", 'w') as f:
        sample_cnt = 0
        basepairs = 0

        for item in selectedItems:
            f.write(json.dumps(item))
            f.write("\n")

            sample_cnt += 1
            basepairs += item["basepairs"]

        f.write("samples:%d basepairs:%d\n" % (sample_cnt, basepairs))
        f.write("appx. size:%d\n" % (basepairs / 1024 / 1024 / 1024))

    for item in selectedItems:
        if arg:
            print item[arg]
        else:
            print item


if __name__ == "__main__":
    main(sys.argv[1:])
