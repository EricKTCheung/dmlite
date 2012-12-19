cmake_minimum_required (VERSION 2.6)

# Install
install (DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/
         DESTINATION /usr/share/tests/dmlite/python
         USE_SOURCE_PERMISSIONS
         FILES_MATCHING PATTERN *.py
                        PATTERN .svn  EXCLUDE
                        PATTERN CMakeFiles EXCLUDE)
