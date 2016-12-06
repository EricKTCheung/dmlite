cmake_minimum_required (VERSION 2.6)

# Install headers
install (FILES		checksums.h extensible.h poolcontainer.h security.h urls.h logger.h mysqlpools.h
         DESTINATION	${INSTALL_PFX_USR}/include/dmlite/cpp/utils
)
