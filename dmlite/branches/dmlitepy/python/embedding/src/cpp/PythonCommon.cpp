/// @file   python/embedding/src/PythonCommon.h
/// @brief  Python Common Parts.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>

#include <dmlite/python/python_common.h>

using namespace dmlite;
using namespace boost::python;


/*
 * this definition comes mainly from 
 * http://wiki.python.org/moin/boost.python/EmbeddingPython#CA-818d2ac860917b563514f26addeb4719b72aac95_22
 */
void PythonExceptionHandler::extractException() throw (DmException)
{
  std::string excString;

  PyObject *exc,*val,*tb;
  PyErr_Fetch(&exc,&val,&tb);
  PyErr_NormalizeException(&exc,&val,&tb);
  handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb));

  int code = 0;
  std::string what = "";

  if(!hval || !htb) {
    excString = extract<std::string>(str(hexc));
  }
    else {
    object oval(hval);
    if (PyObject_HasAttrString(oval.ptr(), "code") && PyObject_HasAttrString(oval.ptr(), "what")) {
      extract<int> get_code(oval.attr("code")());
      extract<std::string> get_what(oval.attr("what")());
//      if (get_code.check() && get_what.check()) {
        code = get_code();
        what = get_what();
        throw DmException(code, what);
//      }
    }  

    object traceback(import("traceback"));
    object format_exception(traceback.attr("format_exception"));
    object formatted_list(format_exception(hexc,hval,htb));
    object formatted(str("").join(formatted_list));
    excString = extract<std::string>(formatted);
  }

  throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
                    excString);
}

