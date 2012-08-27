/*
 * helpers.cpp
 *
 * Various Python bindings for classes from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	boost::python::object anyExtract(boost::any const& self) 
	{ 
		if (self.empty())
			return boost::python::object(); // None 
		else if (self.type() == typeid(int)) 
			return boost::python::object(boost::any_cast<int>(self)); 
		else if (self.type() == typeid(bool)) 
			return boost::python::object(boost::any_cast<bool>(self)); 
		else if (self.type() == typeid(long)) 
			return boost::python::object(boost::any_cast<long>(self)); 
		else if (self.type() == typeid(float))
			return boost::python::object(boost::any_cast<float>(self)); 
		else if (self.type() == typeid(double))
			return boost::python::object(boost::any_cast<double>(self)); 
		else if (self.type() == typeid(std::string))
			return boost::python::object(boost::any_cast<std::string>(self));
		else
		{
			try
			{
				return boost::python::object(boost::any_cast<const char *>(self));
			}
			catch(const boost::bad_any_cast &)
			{}
		}
		
		// boost::any could not be identified
		return boost::python::object(); // None 
	} 

	void anySetBool(boost::any &self, const bool value)
	{
		self = value;
	}

	void anySetLong(boost::any &self, const long value)
	{
		self = value;
	}

	void anySetInt(boost::any &self, const int value)
	{
		self = value;
	}

	void anySetFloat(boost::any &self, const float value)
	{
		self = value;
	}

	void anySetDouble(boost::any &self, const double value)
	{
		self = value;
	}

	void anySetString(boost::any &self, const std::string value)
	{
		self = value;
	}
	
	void ExtensibleSetBool(Extensible &self, const std::string key, const bool value)
	{
		self[key] = value;
	}

	void ExtensibleSetLong(Extensible &self, const std::string key, const long value)
	{
		self[key] = value;
	}

	void ExtensibleSetUnsigned(Extensible &self, const std::string key, const unsigned value)
	{
		self[key] = value;
	}

	void ExtensibleSetDouble(Extensible &self, const std::string key, const double value)
	{
		self[key] = value;
	}

	void ExtensibleSetString(Extensible &self, const std::string key, const std::string value)
	{
		self[key] = value;
	}
	
	
	bool StatIsDir(struct stat &self)
	{
		return S_ISDIR(self.st_mode);
	}

	bool StatIsReg(struct stat &self)
	{
		return S_ISREG(self.st_mode);
	}

	bool StatIsLnk(struct stat &self)
	{
		return S_ISLNK(self.st_mode);
	}

	time_t StatGetATime(struct stat &self)
	{
		return self.st_atime;
	}

	time_t StatGetCTime(struct stat &self)
	{
		return self.st_ctime;
	}

	time_t StatGetMTime(struct stat &self)
	{
		return self.st_mtime;
	}
