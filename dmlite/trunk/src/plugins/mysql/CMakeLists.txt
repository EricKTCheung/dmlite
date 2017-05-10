cmake_minimum_required (VERSION 2.6)

set (BOOST_INCLUDEDIR "/usr/include/boost141") # EPEL5


find_package (MySQL REQUIRED)

include_directories (${Boost_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR})

add_library (mysql MODULE AuthnMySql.cpp
                          DpmMySql.cpp
                          MySqlFactories.cpp
                          ../../utils/MySqlWrapper.cpp
                          NsMySql.cpp
                          Queries.cpp
                          MySqlIO.cpp
                          ../../utils/MySqlPools.cpp
)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries (mysql boost_system dmlite ${MYSQL_LIBRARIES})
ELSE()
  target_link_libraries (mysql dmlite ${MYSQL_LIBRARIES})
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set_target_properties (mysql PROPERTIES PREFIX "plugin_")

install(TARGETS       mysql
        LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/)


# Configuration files
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/mysql.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/mysql.conf.example
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/mysql.conf.example
         DESTINATION ${INSTALL_PFX_ETC}/dmlite.conf.d)
