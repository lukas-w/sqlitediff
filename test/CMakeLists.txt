include_directories(..)

add_executable(sqldiff-test
	test.cpp
)

if(${CMAKE_VERSION} VERSION_LESS 3.1)
	add_definitions(-std=c++11)
else()
	set_property(TARGET sqldiff-test PROPERTY CXX_STANDARD 11)
endif()

set_target_properties(sqldiff-test PROPERTIES COMPILE_FLAGS -std=c++11)

target_link_libraries(sqldiff-test sqlitediff sqlite3)