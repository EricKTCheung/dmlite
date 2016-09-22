cmake_minimum_required (VERSION 2.6)


install(DIRECTORY      dbscripts
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/)

install(DIRECTORY      filepull
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/
        FILE_PERMISSIONS       OWNER_EXECUTE OWNER_WRITE OWNER_READ
                               GROUP_EXECUTE GROUP_READ
                               WORLD_EXECUTE WORLD_READ)
                               
