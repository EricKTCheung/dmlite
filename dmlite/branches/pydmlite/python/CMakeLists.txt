cmake_minimum_required (VERSION 2.6)

find_package(Boost)
find_package(PythonLibs)

include_directories (${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})

add_library(pydmlite SHARED pydmlite.cpp)
target_link_libraries(pydmlite boost_python dmlite dmliteutils)

install(TARGETS       pydmlite
        LIBRARY       DESTINATION ${PYTHON_INCLUDE_PATH}
        PERMISSIONS   OWNER_EXECUTE OWNER_WRITE OWNER_READ
                      GROUP_EXECUTE GROUP_READ
                      WORLD_EXECUTE WORLD_READ
)
