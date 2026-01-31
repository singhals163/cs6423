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

namespace buzzdb {

constexpr size_t WORD_SIZE = sizeof(uint64_t);

struct MergeNode {
    uint64_t value;
    uint64_t bucket_idx;
    uint64_t element_idx;
    
    bool operator>(const MergeNode& other) const {
        return value > other.value;
    }
};


class KMergeExternalSort {
    private:

    uint64_t maxNumsPerBucket, totalBuckets, numsPerPartialBuffer;
    std::vector<std::unique_ptr<File>> temp_files;
    uint64_t num_values, mem_size;
    std::vector<std::vector<uint64_t>> bucketPartialBuffers;

    void readNextElements(uint64_t bucketIndex, uint64_t offset) {
        uint64_t numElements = std::min(numsPerPartialBuffer, temp_files[bucketIndex]->size() / WORD_SIZE - offset);
        auto& buffer = bucketPartialBuffers[bucketIndex];
        buffer.resize(numElements);
        if(numElements == 0) return;
        temp_files[bucketIndex]->read_block(offset*WORD_SIZE, numElements * WORD_SIZE, reinterpret_cast<char*>(buffer.data()));
    }

    void partial_sort(File &input) {
        std::vector<uint64_t> tmp_array(maxNumsPerBucket);
    
        for(uint64_t i = 0; i < totalBuckets; i++) {
            int bucketSize = std::min(maxNumsPerBucket, num_values - i*maxNumsPerBucket);
            input.read_block(i*mem_size, bucketSize*WORD_SIZE, reinterpret_cast<char*>(tmp_array.data()));
            std::sort(tmp_array.begin(), tmp_array.begin() + bucketSize);
            temp_files[i] = File::make_temporary_file();
            temp_files[i]->resize(bucketSize*WORD_SIZE);
            temp_files[i]->write_block(reinterpret_cast<char*>(tmp_array.data()), 0, bucketSize*WORD_SIZE);
        }
    }

    void kMerge(File &output) {

        std::vector<uint64_t> bucketHeadPtr(totalBuckets, 0);
        std::priority_queue<MergeNode, std::vector<MergeNode>, std::greater<MergeNode>> sortedHeap;
        
        for(uint64_t bucketIdx = 0; bucketIdx < totalBuckets; bucketIdx++) {
            readNextElements(bucketIdx, bucketHeadPtr[bucketIdx]);
            if(!bucketPartialBuffers[bucketIdx].empty()) {
                sortedHeap.push({bucketPartialBuffers[bucketIdx][0], bucketIdx, 0});
                bucketHeadPtr[bucketIdx] += bucketPartialBuffers[bucketIdx].size();
            }
        }

        std::vector<uint64_t> sortedElements;
        sortedElements.reserve(numsPerPartialBuffer);

        uint64_t totalNumbersWritten = 0;
        while(!sortedHeap.empty()) {

            auto [val, bucketIdx, elementIdx] = sortedHeap.top();
            sortedHeap.pop();
            sortedElements.push_back(val);

            if(sortedElements.size() == numsPerPartialBuffer) {
                output.write_block(reinterpret_cast<char*>(sortedElements.data()), totalNumbersWritten * WORD_SIZE, numsPerPartialBuffer*WORD_SIZE);
                totalNumbersWritten += numsPerPartialBuffer; 
                sortedElements.clear();
            }

            std::vector<uint64_t>& bucketBuffer = bucketPartialBuffers[bucketIdx];
            if(elementIdx + 1 < bucketBuffer.size()) {
                sortedHeap.push({bucketBuffer[elementIdx+1], bucketIdx, elementIdx+1});
            } else {
                readNextElements(bucketIdx, bucketHeadPtr[bucketIdx]);
                if(!bucketBuffer.empty()) {
                    sortedHeap.push({bucketBuffer[0], bucketIdx, 0});
                    bucketHeadPtr[bucketIdx] += bucketPartialBuffers[bucketIdx].size();
                }
            }
        }
        if(!sortedElements.empty()) {
            output.write_block(reinterpret_cast<char*>(sortedElements.data()), totalNumbersWritten * WORD_SIZE, sortedElements.size()*WORD_SIZE);
        }
        return;
    }

    public:
    KMergeExternalSort(size_t numValues, size_t memSize)
    :num_values(numValues), mem_size(memSize) {
        maxNumsPerBucket = mem_size/WORD_SIZE;
        totalBuckets = CEIL(num_values, maxNumsPerBucket);
        temp_files = std::vector<std::unique_ptr<File>>(totalBuckets);
        numsPerPartialBuffer = (mem_size / (totalBuckets + 1)) / WORD_SIZE;
        bucketPartialBuffers = std::vector<std::vector<uint64_t>>(totalBuckets);
    }

    void external_sort(File &input, File &output) {
        if(num_values == 0) return;
        output.resize(num_values*WORD_SIZE);
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