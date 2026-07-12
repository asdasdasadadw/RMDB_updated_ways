cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)

# Preserve CMake defaults and append aggressive release optimizations.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O3 -DNDEBUG -fno-strict-aliasing -fomit-frame-pointer -march=native -pthread")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

# Enable LTO only if the toolchain supports it (some evaluation environments may not).
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-flto" COMPILER_SUPPORTS_LTO)
if(COMPILER_SUPPORTS_LTO)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O3 -g -DNDEBUG -fno-strict-aliasing")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O3 -g -DNDEBUG -fno-strict-aliasing")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
