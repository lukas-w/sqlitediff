include_directories(..)

add_executable(sqldiff-test
	test.cpp
)

set_target_properties(sqldiff-test PROPERTIES COMPILE_FLAGS -std=c++11)

target_link_libraries(sqldiff-test sqlitediff sqlite3)