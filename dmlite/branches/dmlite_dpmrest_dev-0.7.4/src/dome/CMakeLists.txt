cmake_minimum_required (VERSION 2.6)

include_directories(${Boost_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR} ${DAVIX_PKG_INCLUDE_DIRS})


set(Dome_SOURCES dpmrest.cpp DomeGenQueue.cpp DomeCore.cpp DomeLog.cpp DomeTaskExec.cpp DomeReq.cpp)

add_executable        (dome ${Dome_SOURCES})
target_link_libraries (dome fcgi ${DAVIX_PKG_LIBRARIES} ${MYSQL_LIBRARIES} ${Boost_LIBRARIES} crypto dmlite pthread dl)

# Install
install (TARGETS dome
         DESTINATION            ${INSTALL_PFX_LIB}/var/www/fcgi-bin/
         PERMISSIONS            OWNER_EXECUTE OWNER_WRITE OWNER_READ
                                GROUP_EXECUTE GROUP_READ
                                WORLD_EXECUTE WORLD_READ )
                        
                        