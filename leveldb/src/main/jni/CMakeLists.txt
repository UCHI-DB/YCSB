cmake_minimum_required(VERSION 3.19)
project(leveldbjni)

set(CMAKE_CXX_STANDARD 14)

find_package(JNI REQUIRED)

macro(build_colsm)

    set(COLSM_REPO_URL "https://github.com/UCHI-DB/colsm.git")

    if(CMAKE_BUILD_TYPE MATCHES DEBUG)
        set(CMAKE_COLSM_DEBUG_EXTENSION "d")
    else()
        set(CMAKE_COLSM_DEBUG_EXTENSION "")
    endif()

    set(COLSM_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/colsm_ep-prefix/src/colsm_ep")
    set(_COLSM_LIBRARY_SUFFIX "${CMAKE_COLSM_DEBUG_EXTENSION}${CMAKE_STATIC_LIBRARY_SUFFIX}")


    set(COLSM_STATIC_LIB ${COLSM_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}colsm${_COLSM_LIBRARY_SUFFIX})
    set(COLSM_INCLUDE_DIR ${COLSM_PREFIX}/include)


    set(COLSM_CMAKE_ARGS
            ${EP_COMMON_TOOLCHAIN}
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            "-DCMAKE_INSTALL_PREFIX=${COLSM_PREFIX}"
            -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
            -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})


    externalproject_add(colsm_ep
            GIT_REPOSITORY ${COLSM_REPO_URL}
            GIT_TAG master
            BUILD_BYPRODUCTS ${COLSM_STATIC_LIB}
            CMAKE_ARGS ${COLSM_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    add_library(colsm_static STATIC IMPORTED)
    set_target_properties(colsm_static
            PROPERTIES IMPORTED_LOCATION "${COLSM_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${COLSM_INCLUDE_DIR}")
    add_dependencies(colsm_static sboost_ep)
    file(MAKE_DIRECTORY ${COLSM_INCLUDE_DIR})
    include_directories(${COLSM_INCLUDE_DIR})
#    set(COLSM_SIMD_FLAGS -msse4.1 -mavx -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2)
endmacro()


include_directories(${JNI_INCLUDE_DIRS})
add_library(leveldbjni MODULE leveldb_jni.cc site_ycsb_db_leveldb_LevelDB.h)
#set_target_properties(leveldbjni PROPERTIES SUFFIX ".jnilib")
target_link_libraries(leveldbjni ${JNI_LIBRARIES})
