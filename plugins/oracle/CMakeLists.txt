cmake_minimum_required (VERSION 2.6)

link_directories (/usr/lib64/oracle/10.2.0.5/client/lib64
                  /usr/lib/oracle/10.2.0.5/client/lib)

include_directories (/usr/include/oracle/10.2.0.5/client)

add_library (oracle MODULE OracleFactories.cpp NsOracle.cpp)

target_link_libraries (oracle dmlite dmlitecommon clntsh occi)
set_target_properties (oracle PROPERTIES PREFIX "plugin_")

install(TARGETS       oracle
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
