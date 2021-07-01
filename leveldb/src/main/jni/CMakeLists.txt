cmake_minimum_required(VERSION 3.19)
project(leveldbjni)

set(CMAKE_CXX_STANDARD 14)

find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})
add_library(leveldbjni MODULE leveldb_jni.cc site_ycsb_db_leveldb_LevelDB.h)
set_target_properties(leveldbjni PROPERTIES SUFFIX ".jnilib")
target_link_libraries(leveldbjni ${JNI_LIBRARIES})
