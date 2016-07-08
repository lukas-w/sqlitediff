cmake_minimum_required(VERSION 2.8.0)
project(sqlitediff C CXX)

add_library(${PROJECT_NAME} SHARED
	patch.cpp
	patch.h
	diff.c
	diff.h
	sqliteint.c
	sqliteint.h
)

add_executable(sqlite-diff main-diff.cpp)
add_executable(sqlite-patch main-patch.cpp)

target_link_libraries(sqlitediff sqlite3)
target_link_libraries(sqlite-diff sqlitediff)
target_link_libraries(sqlite-patch sqlitediff)

set_source_files_properties(patch.cpp patch.h main-diff.cpp main-patch.cpp
                            PROPERTIES COMPILE_FLAGS -std=c++11)

add_subdirectory(test)
