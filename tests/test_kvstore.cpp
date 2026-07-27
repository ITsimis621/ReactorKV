#include <gtest/gtest.h>
#include "KVStore.h"
#include <filesystem>

/**
 * @class KVStoreTest
 * @brief Google Test fixture for isolated database testing.
 * 
 * Ensures a pristine file system state before and after each test 
 * by wiping the AOF data directory.
 */
class KVStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("data");
    }

    void TearDown() override {
        std::filesystem::remove_all("data");
    }
};

TEST_F(KVStoreTest, BasicSetAndGet) {
    KVStore db;
    db.set("name", "ilias");
    EXPECT_EQ(db.get("name"), "ilias");
}

TEST_F(KVStoreTest, UpdateExistingKey) {
    KVStore db;
    db.set("counter", "1");
    db.set("counter", "2"); 
    EXPECT_EQ(db.get("counter"), "2");
}

TEST_F(KVStoreTest, RemoveKey) {
    KVStore db;
    db.set("session", "xyz");
    db.remove("session");
    
    EXPECT_EQ(db.get("session"), "[DB] ERROR: Key not found");
}

TEST_F(KVStoreTest, GetNonExistentKey) {
    KVStore db;
    EXPECT_EQ(db.get("ghost"), "[DB] ERROR: Key not found");
}

TEST_F(KVStoreTest, PersistenceAndRecovery) {
    // Trigger AOF persistence and shutdown by scoping the database instance
    {
        KVStore db1;
        db1.set("hero", "batman");
        db1.set("villain", "joker");
    } 

    // Boot up a new instance to verify AOF parsing and memory reconstruction
    KVStore db2;
    EXPECT_EQ(db2.get("hero"), "batman");
    EXPECT_EQ(db2.get("villain"), "joker");
}