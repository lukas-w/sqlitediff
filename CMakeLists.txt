cmake_minimum_required(VERSION 3.0)
project(sqlitediff)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

add_library(${PROJECT_NAME} SHARED
	diff
	patch
	sqliteint
)

add_executable(sqlite-diff main-diff.cpp)
add_executable(sqlite-patch main-patch.cpp)

target_link_libraries(sqlitediff sqlite3)
target_link_libraries(sqlite-diff sqlitediff)
target_link_libraries(sqlite-patch sqlitediff)

set_property(TARGET sqlitediff PROPERTY CXX_STANDARD 11)
set_property(TARGET sqlite-diff PROPERTY CXX_STANDARD 11)
set_property(TARGET sqlite-patch PROPERTY CXX_STANDARD 11)

add_subdirectory(test)