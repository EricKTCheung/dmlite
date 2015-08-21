cmake_minimum_required (VERSION 2.6)

include_directories(${Boost_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR} ${DAVIX_PKG_INCLUDE_DIRS} )

add_executable        (dpmrest dpmrest.cpp)
target_link_libraries (dpmrest fcgi ${DAVIX_PKG_LIBRARIES} ${MYSQL_LIBRARIES} ssl crypto dmlite pthread dl)

# Install
install (TARGETS dpmrest
         DESTINATION            ${INSTALL_PFX_LIB}/var/www/fcgi-bin/
         PERMISSIONS       OWNER_EXECUTE OWNER_WRITE OWNER_READ
                                GROUP_EXECUTE GROUP_READ
                                WORLD_EXECUTE WORLD_READ )
                        
                        