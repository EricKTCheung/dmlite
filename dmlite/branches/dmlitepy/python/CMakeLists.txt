cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS python REQUIRED)
find_package(PythonLibs)

execute_process (COMMAND python -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)" 
		 OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
		 OUTPUT_STRIP_TRAILING_WHITESPACE)

include_directories (${Boost_INCLUDE_DIRS} ${PYTHON_INCLUDE_PATH})
link_directories(${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)

add_library(pydmlite SHARED pydmlite.cpp authn.cpp base.cpp catalog.cpp errno.cpp exceptions.cpp extensible.cpp inode.cpp 
io.cpp pooldriver.cpp poolmanager.cpp types.cpp embedding/src/PythonINode.cpp
embedding/src/PythonINodeFactories.cpp)
target_link_libraries(pydmlite ${Boost_LIBRARIES} ${PYTHON_LIBRARIES} dmlite)
set_target_properties (pydmlite PROPERTIES PREFIX "")

install(TARGETS       pydmlite
        LIBRARY       DESTINATION ${PYTHON_SITE_PACKAGES}
        PERMISSIONS   OWNER_EXECUTE OWNER_WRITE OWNER_READ
                      GROUP_EXECUTE GROUP_READ
                      WORLD_EXECUTE WORLD_READ
)

#add_subdirectory(embedding)
