cmake_minimum_required (VERSION 2.6)

install(FILES      cns_mysql_db.sql dpm_mysql_db.sql
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/dbscripts)
        
install(DIRECTORY      upgrade
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/dbscripts)