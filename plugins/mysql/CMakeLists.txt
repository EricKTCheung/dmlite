cmake_minimum_required (VERSION 2.6)

link_directories (/usr/lib64/mysql /usr/lib/mysql)

add_library (mysql MODULE MySql.cpp NsMySql.cpp DpmMySql.cpp)

target_link_libraries (mysql dmlite dmlitecommon mysqlclient_r)
set_target_properties (mysql PROPERTIES PREFIX "plugin_")

install(TARGETS       mysql
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
