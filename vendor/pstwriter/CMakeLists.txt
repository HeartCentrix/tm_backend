cmake_minimum_required(VERSION 3.20)

# Vendored slim CMake for the restore-worker Docker build. Builds the
# pstwriter static library and the pst_convert CLI only — no tests, no
# pst_info, no Catch2 fetch. The full upstream tree at d:/Work Qfion/PST Dev
# remains the source of truth; refresh this vendor dir by re-syncing
# src/, include/pstwriter/, and tools/pst_convert.cpp.

project(pstwriter_vendor
    VERSION 0.1.0
    DESCRIPTION "Vendored pstwriter — build pst_convert for restore-worker"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

option(PSTWRITER_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

add_library(pstwriter STATIC
    src/crc.cpp
    src/encoding.cpp
    src/ndb.cpp
    src/page.cpp
    src/block.cpp
    src/writer.cpp
    src/ltp.cpp
    src/m5_allocator.cpp
    src/nbt.cpp
    src/messaging.cpp
    src/graph_convert.cpp
    src/graph_message.cpp
    src/mail.cpp
    src/graph_contact.cpp
    src/contact.cpp
    src/graph_event.cpp
    src/event.cpp
    src/pst_baseline.cpp
)

target_include_directories(pstwriter
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/pstwriter
)

target_compile_features(pstwriter PUBLIC cxx_std_17)

if(MSVC)
    target_compile_options(pstwriter PRIVATE /W4 /permissive- /Zc:__cplusplus /Zc:preprocessor /utf-8)
    if(PSTWRITER_WARNINGS_AS_ERRORS)
        target_compile_options(pstwriter PRIVATE /WX)
    endif()
    target_compile_definitions(pstwriter PRIVATE
        _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN UNICODE _UNICODE)
else()
    target_compile_options(pstwriter PRIVATE
        -Wall -Wextra -Wpedantic -Wshadow -Wconversion)
    if(PSTWRITER_WARNINGS_AS_ERRORS)
        target_compile_options(pstwriter PRIVATE -Werror)
    endif()
endif()

add_executable(pst_convert tools/pst_convert.cpp)
target_link_libraries(pst_convert PRIVATE pstwriter)
target_include_directories(pst_convert PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/pstwriter)
set_target_properties(pst_convert PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
