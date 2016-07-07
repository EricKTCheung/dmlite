cmake_minimum_required (VERSION 2.6)

install(FILES      *.sql
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/dbscripts)
        
install(FILES      upgrade/*
        DESTINATION   ${INSTALL_PFX_USR}/share/dmlite/dbscripts/upgrade)