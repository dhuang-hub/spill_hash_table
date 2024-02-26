#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <vector>
#include <string>
#include <cassert>
#include <random>

#include "hash_table.h"

std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<int> dis(INT_MIN, INT_MAX);

void assertCondition(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "Assertion failed: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

#define ASSERT(condition) assertCondition((condition), #condition)

void runTest(void (*testFunc)(), const std::string& testName) {
    std::cout << "Running " << testName << "..." << std::endl;
    testFunc();
    std::cout << testName << " passed." << std::endl;
}

int randint32() {
    return dis(gen);
}

void cleanDirectory(const std::string& dir) {
    std::filesystem::remove_all(dir);
}

void testSetup() {
    std::string tableDir = "_test_setup";
    std::filesystem::remove_all(tableDir);

    PersistentHashTable ht(tableDir);
    ASSERT(std::filesystem::exists(tableDir));
    ASSERT(std::filesystem::is_directory(tableDir));
    auto logFile = std::filesystem::path(tableDir) / "log.txt";
    ASSERT(std::filesystem::exists(logFile));

    std::filesystem::remove_all(tableDir);
}

void testCorrectness() {
    std::string tableDir = "_test_correctness";
    std::filesystem::remove_all(tableDir);

    PersistentHashTable ht(tableDir);
    const int n = 100;
    for (int i = 0; i < n; ++i) {
        int key = randint32();
        int value = randint32();
        ht.setItem(key, value);
        int retrieved = ht.getItem(key);
        ASSERT(retrieved == value);
    }

    std::filesystem::remove_all(tableDir);
}

void testReload() {
    std::string origDir = "_test_reload_orig";
    std::string reloadDir = "_test_reload_new";
    cleanDirectory(origDir);
    cleanDirectory(reloadDir);

    // initial hash table
    PersistentHashTable origHt(origDir);
    int key = randint32(), value = randint32();
    origHt.setItem(key, value);

    // reload hash table from log
    std::filesystem::path log_file = origHt.getLogFile();
    PersistentHashTable reloadedHt = PersistentHashTable(log_file, reloadDir, 3);

    // verify the reloaded data
    ASSERT(reloadedHt.getItem(key) == value);

    cleanDirectory(origDir);
    cleanDirectory(reloadDir);
}

void threadTask(PersistentHashTable* ht, int key, int value) {
    ht->setItem(key, value);
    int retrieved = ht->getItem(key);
    ASSERT(retrieved == value);
}

void testThreadSafety() {
    std::string tableDir = "_test_thread_safety";
    cleanDirectory(tableDir);

    PersistentHashTable ht(tableDir);
    const int n = 100; // number of threads

    std::vector<std::thread> threads;
    for (int i = 0; i < n; ++i) {
        threads.emplace_back(threadTask, &ht, i, randint32());
    }

    for (auto& t : threads) {
        t.join();
    }

    cleanDirectory(tableDir);
}



int main() {
    runTest(testSetup, "Test Setup");
    runTest(testCorrectness, "Test Correctness");
    // runTest(testSetError, "Test Error Handling");
    runTest(testReload, "Test Reload");
    runTest(testThreadSafety, "Test Thread Safety");

    std::cout << "All tests passed." << std::endl;
    return 0;
}
