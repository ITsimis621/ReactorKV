cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0135 NEW)
project(ReactorKV VERSION 0.4.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enforce strict compiler warnings
if(MSVC)
    add_compile_options(/W4 /WX)
else()
    add_compile_options(-Wall -Wextra -Werror -pedantic)
endif()

# Dependencies
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)

# Primary Executable
add_executable(reactorkv
    src/main.cpp
    src/Server.cpp
    src/KVStore.cpp
    src/AOFManager.cpp
)

# Tell the compiler where to find the header files
target_include_directories(reactorkv PRIVATE include)

target_link_libraries(reactorkv PRIVATE nlohmann_json::nlohmann_json Threads::Threads)

# ------------------------------------------------------------------------------
# Unit Testing Configuration (Google Test)
# ------------------------------------------------------------------------------
enable_testing()

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# Force shared CRT for Windows/WSL compatibility
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(kvstore_tests 
    tests/test_kvstore.cpp 
    src/KVStore.cpp 
    src/AOFManager.cpp
)

# Test executable also needs to know where the headers are
target_include_directories(kvstore_tests PRIVATE include)

target_link_libraries(kvstore_tests
    GTest::gtest_main
    nlohmann_json::nlohmann_json 
    Threads::Threads
)

include(GoogleTest)
gtest_discover_tests(kvstore_tests)