cmake_minimum_required (VERSION 2.6)

# Install headers
install (FILES		any.h catalog.h checksums.h dmlite.h inode.h io.h pool.h utils.h
         DESTINATION	${INSTALL_PFX_USR}/include/dmlite/c
)
