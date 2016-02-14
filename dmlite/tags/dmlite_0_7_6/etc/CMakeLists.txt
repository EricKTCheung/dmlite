cmake_minimum_required (VERSION 2.6)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/dmlite.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf
         DESTINATION ${INSTALL_PFX_ETC})

# Create empty directories
file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf.d)

install (DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf.d
         DESTINATION ${INSTALL_PFX_ETC})


IF(RSYSLOG_SUPPORT)
message(STATUS "rsyslog support enabled")

# install syslog scripts
install(FILES "conf/20-log-dmlite.conf"
              DESTINATION ${INSTALL_PFX_ETC}/rsyslog.d/ )
ENDIF(RSYSLOG_SUPPORT)


IF(LOGROTATE_SUPPORT)
message(STATUS "logrotate support enabled")

# install syslog scripts
install(FILES "conf/logrotate/dmlite"
              DESTINATION ${INSTALL_PFX_ETC}/logrotate.d/ )
ENDIF(LOGROTATE_SUPPORT)
