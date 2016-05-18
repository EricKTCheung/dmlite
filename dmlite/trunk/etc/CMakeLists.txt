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

IF(BUILD_DOME)
  set(STARTUP_SCRIPT_NAME "domehead")
  configure_file (${CMAKE_CURRENT_SOURCE_DIR}/rc.d/init.d/dome.in
                  ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/${STARTUP_SCRIPT_NAME}
                  @ONLY)

  set(STARTUP_SCRIPT_NAME "domedisk")
  configure_file (${CMAKE_CURRENT_SOURCE_DIR}/rc.d/init.d/dome.in
                  ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/${STARTUP_SCRIPT_NAME}
                  @ONLY)

  install(FILES   ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/domehead
                  ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/domedisk
          PERMISSIONS
                  OWNER_READ OWNER_WRITE OWNER_EXECUTE
                  GROUP_READ             GROUP_EXECUTE
                  WORLD_READ             WORLD_EXECUTE
          DESTINATION
                  ${INSTALL_PFX_ETC}/rc.d/init.d)
ENDIF(BUILD_DOME)

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
