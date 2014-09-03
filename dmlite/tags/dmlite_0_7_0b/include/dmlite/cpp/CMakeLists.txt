cmake_minimum_required (VERSION 2.6)

add_subdirectory (dummy)
add_subdirectory (utils)

# Install headers
install (FILES		authn.h base.h catalog.h dmlite.h exceptions.h inode.h
			io.h pooldriver.h poolmanager.h 
         DESTINATION	usr/include/dmlite/cpp
)
