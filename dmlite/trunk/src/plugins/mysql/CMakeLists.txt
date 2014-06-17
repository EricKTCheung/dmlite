cmake_minimum_required (VERSION 2.6)

set (BOOST_INCLUDEDIR "/usr/include/boost141") # EPEL5


find_package (MySQL REQUIRED)

include_directories (${Boost_INCLUDE_DIR} ${DMLITE_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR})

add_library (mysql MODULE AuthnMySql.cpp
                          DpmMySql.cpp
                          MySqlFactories.cpp
                          MySqlWrapper.cpp
                          NsMySql.cpp
                          Queries.cpp
            )

target_link_libraries (mysql ${DMLITE_LIBRARIES} ${MYSQL_LIBRARIES})
set_target_properties (mysql PROPERTIES PREFIX "plugin_")

install(TARGETS       mysql
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/)


# Configuration files
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/mysql.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/mysql.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/mysql.conf
         DESTINATION etc/dmlite.conf.d)
