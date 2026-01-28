#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <thread>
#include <vector>

#include "storage/file.h"

#define UNUSED(p) ((void)(p))
#define CEIL(a, b) (a-1)/b + 1
#define LONGSIZE 8

namespace buzzdb {

using heapPair = std::pair<uint64_t, std::pair<uint64_t, uint64_t>>;

class KMergeExternalSort {
    private:

    uint64_t maxNumsPerBucket, totalBuckets, numsPerPartialBuffer;
    std::vector<std::unique_ptr<File>> temp_files;
    uint64_t num_values, mem_size;
    std::vector<uint64_t*> bucketPartialBuffers;

    void readNextElements(uint64_t bucketIndex, uint64_t offset) {
        uint64_t numElements = std::min(numsPerPartialBuffer, temp_files[bucketIndex]->size() / LONGSIZE - offset);
        temp_files[bucketIndex]->read_block(offset*LONGSIZE, numElements * LONGSIZE, (char*)bucketPartialBuffers[bucketIndex]);
    }

    void partial_sort(File &input) {
        uint64_t* tmp_array = (uint64_t*)malloc(mem_size);
    
        for(uint64_t i = 0; i < totalBuckets; i++) {
            int bucketSize = std::min(maxNumsPerBucket, num_values - i*maxNumsPerBucket);
            input.read_block(i*mem_size, bucketSize*LONGSIZE, (char*)tmp_array);
            std::sort(tmp_array, tmp_array + bucketSize);
            temp_files[i] = File::make_temporary_file();
            temp_files[i]->resize(bucketSize*LONGSIZE);
            temp_files[i]->write_block((char*) tmp_array, 0, bucketSize*LONGSIZE);
        }
        free(tmp_array);
    }

    void kMerge(File &output) {

        uint64_t headBucket[totalBuckets] = {0};
        std::priority_queue<heapPair, std::vector<heapPair>, std::greater<heapPair>> sortedHeap;
        for(uint64_t bucketIndex = 0; bucketIndex < totalBuckets; bucketIndex++) {
            bucketPartialBuffers[bucketIndex] = (uint64_t*)malloc(numsPerPartialBuffer * LONGSIZE);
            readNextElements(bucketIndex, headBucket[bucketIndex]);
            sortedHeap.push({bucketPartialBuffers[bucketIndex][0], {0, bucketIndex}});
        }
        uint64_t* sortedElements = (uint64_t*)malloc(numsPerPartialBuffer * LONGSIZE);

        uint64_t bufferIndex = 0, totalNumbersWritten = 0;
        while(true) {
            if(sortedHeap.empty()) break;
            heapPair topNode = sortedHeap.top();
            sortedHeap.pop();
            sortedElements[bufferIndex] = topNode.first;
            bufferIndex++;

            if(bufferIndex == numsPerPartialBuffer) {
                output.write_block((char*)sortedElements, totalNumbersWritten * LONGSIZE, numsPerPartialBuffer*LONGSIZE);
                totalNumbersWritten += bufferIndex; 
                bufferIndex = 0;
            }
            uint64_t bucketIndex = topNode.second.second;
            uint64_t *bucketBuffer = bucketPartialBuffers[bucketIndex];
            headBucket[bucketIndex]++;
            if(headBucket[bucketIndex] == temp_files[bucketIndex]->size() / LONGSIZE) {
                continue;
            }
            if(headBucket[bucketIndex] % numsPerPartialBuffer == 0) {
                readNextElements(bucketIndex, headBucket[bucketIndex]);
                sortedHeap.push({bucketBuffer[0], {0, bucketIndex}});
            } else{
                topNode.second.first++;
                sortedHeap.push({bucketBuffer[topNode.second.first], topNode.second});
            }
        }
        if(bufferIndex) {
            output.write_block((char*)sortedElements, totalNumbersWritten * LONGSIZE, bufferIndex*LONGSIZE);
        }

        for(uint64_t bucketIndex = 0; bucketIndex < totalBuckets; bucketIndex++) {
            free(bucketPartialBuffers[bucketIndex]);
        }
        free(sortedElements);
        return;
    }

    public:
    KMergeExternalSort(size_t numValues, size_t memSize)
    :num_values(numValues), mem_size(memSize) {

        maxNumsPerBucket = mem_size/LONGSIZE;
        totalBuckets = CEIL(num_values, maxNumsPerBucket);
        temp_files = std::vector<std::unique_ptr<File>>(totalBuckets);
        numsPerPartialBuffer = (mem_size / (totalBuckets + 1)) / LONGSIZE;
        bucketPartialBuffers = std::vector<uint64_t*>(totalBuckets);
    }

    void external_sort(File &input, File &output) {
        if(num_values == 0) return;
        output.resize(num_values*LONGSIZE);
        partial_sort(input);
        kMerge(output);
    }
};


void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
    /* To be implemented
    ** Remove these before you start your implementation
    */
   if(num_values == 0) return;
   KMergeExternalSort sortObj(num_values, mem_size);
   sortObj.external_sort(input, output);
}

}