CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(base_test)

# definitions
#

# includes
SET(includes
    "../../include"
    )
INCLUDE_DIRECTORIES(${includes})

SET(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/bin")

# sources
FILE(GLOB sources_c "../../test/base/*.c")

# targets
ADD_EXECUTABLE(base_test ${sources_c})
TARGET_LINK_LIBRARIES(base_test wt pthread)