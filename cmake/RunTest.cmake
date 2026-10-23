# Run one test in an isolated working directory with a fresh copy of the
# SQLite fixtures. Invoked by CTest; see CMakeLists.txt.

if(NOT DEFINED TEST_EXE OR NOT DEFINED DATA_DIR OR NOT DEFINED WORK_DIR)
    message(FATAL_ERROR "TEST_EXE, DATA_DIR and WORK_DIR are required")
endif()

file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")
file(COPY "${DATA_DIR}/" DESTINATION "${WORK_DIR}/data")

execute_process(
    COMMAND "${TEST_EXE}"
    WORKING_DIRECTORY "${WORK_DIR}"
    RESULT_VARIABLE test_result)

if(NOT test_result EQUAL 0)
    message(FATAL_ERROR "Test exited with code ${test_result}")
endif()
