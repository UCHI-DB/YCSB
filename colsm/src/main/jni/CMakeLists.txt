cmake_minimum_required(VERSION 3.16)
project(colsmjni)

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

macro(build_colsm)
    set(COLSM_REPO_URL "https://github.com/UCHI-DB/colsm.git")

    set(COLSM_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/colsm_ep-prefix/src/colsm_ep")
    set(_COLSM_LIBRARY_SUFFIX "${CMAKE_STATIC_LIBRARY_SUFFIX}")


    set(COLSM_STATIC_LIB ${COLSM_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}leveldb${_COLSM_LIBRARY_SUFFIX})
    set(COLSM_INCLUDE_DIR ${COLSM_PREFIX}/include)


    set(COLSM_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_INSTALL_PREFIX=${COLSM_PREFIX}
            )


    externalproject_add(colsm_ep
            GIT_REPOSITORY ${COLSM_REPO_URL}
            GIT_TAG master
            BUILD_BYPRODUCTS ${COLSM_STATIC_LIB}
            CMAKE_ARGS ${COLSM_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    add_library(colsm_static STATIC IMPORTED)
    set_target_properties(colsm_static
            PROPERTIES IMPORTED_LOCATION "${COLSM_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${COLSM_INCLUDE_DIR}")
    add_dependencies(colsm_static colsm_ep)
    file(MAKE_DIRECTORY ${COLSM_INCLUDE_DIR})
    include_directories(${COLSM_INCLUDE_DIR})
endmacro()

# This build file is not actually being invoked by Maven.
# Maven will download colsm from git and build it.
# build_colsm()

if(CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(CMAKE_DEBUG_EXTENSION "d")
else()
    set(CMAKE_DEBUG_EXTENSION "")
endif()

include_directories(${JNI_INCLUDE_DIRS})
set(COLSM_JNI_SHARED_LIB "colsmjni2${CMAKE_DEBUG_EXTENSION}")
add_library(${COLSM_JNI_SHARED_LIB} MODULE jni_dummy.cc site_ycsb_db_colsm_CoLSM.h)
# target_link_libraries(${COLSM_JNI_SHARED_LIB} PUBLIC ${JNI_LIBRARIES} colsm_static)
