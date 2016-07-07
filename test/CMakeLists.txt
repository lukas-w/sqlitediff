include_directories(..)

add_executable(sqldiff-test
	test.cpp
)
set_property(TARGET sqldiff-test PROPERTY CXX_STANDARD 11)
target_link_libraries(sqldiff-test sqlitediff sqlite3)