cmake_minimum_required (VERSION 2.6)

# Generate documentation
find_package (Doxygen)

if (DOXYGEN_FOUND)
  configure_file (Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  configure_file (Doxyfile-man.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile-man @ONLY)

  add_custom_target(doc ALL)

  add_custom_command(TARGET doc
                     COMMAND ${DOXYGEN_EXECUTABLE} ARGS ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                     COMMENT "Generating API documentation with Doxygen" VERBATIM)

  add_custom_command(TARGET doc
                     COMMAND ${DOXYGEN_EXECUTABLE} ARGS ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile-man
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                     COMMENT "Generating man documentation with Doxygen" VERBATIM)


  install (DIRECTORY   ${CMAKE_CURRENT_BINARY_DIR}/html
           DESTINATION usr/share/doc/${PROJECT_NAME}-${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}/api
           COMPONENT   dmlite-devel)
  install (DIRECTORY   ${CMAKE_CURRENT_BINARY_DIR}/man
           DESTINATION usr/share/man
           COMPONENT   dmlite-devel)

endif (DOXYGEN_FOUND)

# Tutorial
install (DIRECTORY   tutorial
         DESTINATION usr/share/doc/${PROJECT_NAME}-${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
         COMPONENT   dmlite-devel
         PATTERN ".svn" EXCLUDE)
