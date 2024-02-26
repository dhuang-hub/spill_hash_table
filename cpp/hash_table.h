#include <string>
#include <filesystem>
#include <mutex>
#include <unordered_map>

class PersistentHashTable {
public:
    PersistentHashTable(const std::string& table_dir, bool log = true, int bucket_mag = 3);
    PersistentHashTable(const std::string& log_path, const std::string& table_dir, int bucket_mag = 3);

    int getItem(int key);
    void setItem(int key, int value);

    std::filesystem::path getLogFile();

private:
    std::filesystem::path table_dir;
    int bucket_mag;
    std::unordered_map<std::string, std::mutex> bucket_lock;
    std::mutex bucket_lock_lock;
    std::mutex log_lock;
    std::filesystem::path log_file;

    std::string hash(int key);
    std::mutex& getBucketLock(const std::filesystem::path& bucket_path);
    std::filesystem::path getBucketPath(int key);
    void log(int key, int value);
};
