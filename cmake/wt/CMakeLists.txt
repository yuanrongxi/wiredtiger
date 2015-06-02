CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(wt)

# definitions
#

# includes
SET(includes
    "../../include"
    )
INCLUDE_DIRECTORIES(${includes})

# sources
FILE(GLOB sources_c "../../posix/*.c")
FILE(GLOB support_c "../../support/*.c")
FILE(GLOB cursor_c "../../cursor/*.c")
FILE(GLOB packing_c "../../packing/*.c")

# targets
ADD_LIBRARY (wt STATIC ${sources_c} ${support_c} ${cursor_c} ${packing_c})