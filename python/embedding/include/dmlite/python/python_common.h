/// @file   python/embedding/include/dmlite/python/python_common.h
/// @brief  Python Common Parts.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#ifndef DMLITE_PYTHON_PYTHONCOMMON_H
#define DMLITE_PYTHON_PYTHONCOMMON_H

#include <boost/python.hpp>

#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/utils/extensible.h>

namespace dmlite {

class PythonMain: public Extensible {

};

void extractException() throw (DmException);

};

#endif // DMLITE_PYTHON_PYTHONCOMMON_H
