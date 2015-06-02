CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(pack_test)

# definitions
#

# includes
SET(includes
    "../../include"
    )
INCLUDE_DIRECTORIES(${includes})

SET(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/bin")

# sources
FILE(GLOB sources_c "../../test/pack/*.c")

# targets
ADD_EXECUTABLE(pack ${sources_c})
TARGET_LINK_LIBRARIES(pack wt pthread)