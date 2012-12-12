/*
 * pydmlite.h
 *
 * Includes required by all the pydmlite module files.
 */

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

#include "authn.h"
#include "base.h"
#include "catalog.h"
#include "dmlite.h"
#include "exceptions.h"
#include "inode.h"
#include "io.h"
#include "pooldriver.h"
#include "poolmanager.h"
#include "utils/urls.h"
#include "utils/security.h"

using namespace dmlite;
using namespace boost::python;
