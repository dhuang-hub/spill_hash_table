#include "hash_table.h"
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <nlohmann/json.hpp>


using json = nlohmann::json;


PersistentHashTable::PersistentHashTable(const std::string& table_dir, bool log, int bucket_mag)
    : table_dir(table_dir), bucket_mag(bucket_mag) {
    std::filesystem::create_directories(this->table_dir);
    if (log) {
        this->log_file = this->table_dir / "log.txt";
        if (!std::filesystem::exists(this->log_file)) {
            std::ofstream(this->log_file);
        }
    }
}

PersistentHashTable::PersistentHashTable(const std::string& log_path, const std::string& table_dir, int bucket_mag)
    : table_dir(table_dir), bucket_mag(bucket_mag) {
    if (!std::filesystem::exists(log_path)) {
        throw std::runtime_error("Log path not found!");
    }

    // wipe pre-existing table dir
    std::filesystem::path tableDirPath(table_dir);
    if (std::filesystem::exists(tableDirPath)) {
        std::filesystem::remove_all(tableDirPath);
    }

    // initialize
    std::filesystem::create_directories(this->table_dir);
    this->log_file = this->table_dir / "log.txt";
    if (!std::filesystem::exists(this->log_file)) {
        std::ofstream(this->log_file);
    }

    std::ifstream logFile(log_path);
    std::string line;
    while (getline(logFile, line)) {
        std::istringstream iss(line);
        int key, value;
        char delim;
        if (!(iss >> key >> delim >> value) || delim != ',') {
            throw std::runtime_error("Invalid log format");
        }
        this->setItem(key, value);
    }
}

std::string PersistentHashTable::hash(int key) {
    std::stringstream ss;
    ss << std::hex << (key % (1 << (bucket_mag * 4)));
    return ss.str();
}

std::mutex& PersistentHashTable::getBucketLock(const std::filesystem::path& bucket_path) {
    std::lock_guard<std::mutex> lock(bucket_lock_lock);
    return bucket_lock[bucket_path.string()];
}

std::filesystem::path PersistentHashTable::getBucketPath(int key) {
    auto hashedKey = hash(key);
    auto bucketPath = table_dir / (hashedKey + ".json");
    if (!std::filesystem::exists(bucketPath)) {
        std::ofstream(bucketPath) << "{}"; // empty JSON file if not exists
    }
    return bucketPath;
}

void PersistentHashTable::log(int key, int value) {
    if (!log_file.empty()) {
        std::lock_guard<std::mutex> lock(log_lock);
        std::ofstream logStream(log_file, std::ios::app);
        logStream << key << "," << value << std::endl;
    }
}

std::filesystem::path PersistentHashTable::getLogFile() {
    return log_file;
}

int PersistentHashTable::getItem(int key) {
    auto bucketPath = getBucketPath(key);
    std::lock_guard<std::mutex> lock(getBucketLock(bucketPath));
    std::ifstream bucketFile(bucketPath);
    json bucket;
    bucketFile >> bucket;
    std::string keyStr = std::to_string(key);
    if (bucket.contains(keyStr)) {
        return bucket[keyStr];
    }
    throw std::runtime_error("Key not found");
}

void PersistentHashTable::setItem(int key, int value) {
    auto bucketPath = getBucketPath(key);
    std::lock_guard<std::mutex> lock(getBucketLock(bucketPath));
    std::ifstream bucketFileRead(bucketPath);
    json bucket;
    bucketFileRead >> bucket;
    bucketFileRead.close();
    bucket[std::to_string(key)] = value;
    std::ofstream bucketFileWrite(bucketPath);
    bucketFileWrite << bucket.dump();
    log(key, value);
}

