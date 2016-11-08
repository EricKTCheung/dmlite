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
  foreach(STARTUP_SCRIPT "domehead" "domedisk")
    configure_file (${CMAKE_CURRENT_SOURCE_DIR}/rc.d/init.d/dome.in
                    ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/${STARTUP_SCRIPT}
                    @ONLY)

    configure_file (${CMAKE_CURRENT_SOURCE_DIR}/systemd/dome.in
                    ${CMAKE_CURRENT_BINARY_DIR}/systemd/${STARTUP_SCRIPT}.service
                    @ONLY)

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/rc.d/init.d/${STARTUP_SCRIPT}
            PERMISSIONS
                  OWNER_READ OWNER_WRITE OWNER_EXECUTE
                  GROUP_READ             GROUP_EXECUTE
                  WORLD_READ             WORLD_EXECUTE
            DESTINATION
                  ${INSTALL_PFX_ETC}/rc.d/init.d)

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/systemd/${STARTUP_SCRIPT}.service
            PERMISSIONS
                  OWNER_READ OWNER_WRITE OWNER_EXECUTE
                  GROUP_READ             GROUP_EXECUTE
                  WORLD_READ             WORLD_EXECUTE
            DESTINATION
                  /usr/lib/systemd/system)
  endforeach()
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
