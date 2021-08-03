cmake_minimum_required(VERSION 3.16)
project(leveldbjni)

set(CMAKE_CXX_STANDARD 14)

find_package(JNI REQUIRED)

include(ExternalProject)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")

set(EP_COMMON_TOOLCHAIN -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

if(CMAKE_AR)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_AR=${CMAKE_AR})
endif()

if(CMAKE_RANLIB)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_RANLIB=${CMAKE_RANLIB})
endif()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
        ${EP_COMMON_TOOLCHAIN}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_C_FLAGS=${EP_C_FLAGS}
        -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})

set(EP_LOG_OPTIONS
        LOG_CONFIGURE
        1
        LOG_BUILD
        1
        LOG_INSTALL
        1
        LOG_DOWNLOAD
        1)

macro(build_leveldb)
    set(LEVELDB_REPO_URL "https://github.com/google/leveldb.git")

    set(LEVELDB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/leveldb_ep-prefix/src/leveldb_ep")
    set(_LEVELDB_LIBRARY_SUFFIX "${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(LEVELDB_STATIC_LIB ${LEVELDB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}leveldb${_LEVELDB_LIBRARY_SUFFIX})
    set(LEVELDB_INCLUDE_DIR ${LEVELDB_PREFIX}/include)


    set(LEVELDB_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_INSTALL_PREFIX=${LEVELDB_PREFIX}
            )


    externalproject_add(leveldb_ep
            GIT_REPOSITORY ${LEVELDB_REPO_URL}
            GIT_TAG master
            BUILD_BYPRODUCTS ${LEVELDB_STATIC_LIB}
            CMAKE_ARGS ${LEVELDB_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    add_library(leveldb_static STATIC IMPORTED)
    set_target_properties(leveldb_static
            PROPERTIES IMPORTED_LOCATION "${LEVELDB_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}")
    add_dependencies(leveldb_static leveldb_ep)
    file(MAKE_DIRECTORY ${LEVELDB_INCLUDE_DIR})
    include_directories(${LEVELDB_INCLUDE_DIR})
endmacro()


build_leveldb()

if(CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(CMAKE_DEBUG_EXTENSION "d")
else()
    set(CMAKE_DEBUG_EXTENSION "")
endif()

include_directories(${JNI_INCLUDE_DIRS})
set(LEVELDB_JNI_SHARED_LIB "leveldbjni${CMAKE_DEBUG_EXTENSION}")
add_library(${LEVELDB_JNI_SHARED_LIB} MODULE jni.cc site_ycsb_db_leveldb_LevelDB.h)
target_link_libraries(${LEVELDB_JNI_SHARED_LIB} PUBLIC ${JNI_LIBRARIES} leveldb_static)
