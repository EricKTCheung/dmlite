cmake_minimum_required (VERSION 2.6)

# Install the tests
install (FILES    ${CMAKE_CURRENT_SOURCE_DIR}/dpm-tester.py
         DESTINATION  ${INSTALL_PFX_USR}/bin
         PERMISSIONS            OWNER_EXECUTE OWNER_WRITE OWNER_READ
                                GROUP_EXECUTE GROUP_READ
                                WORLD_EXECUTE WORLD_READ
)