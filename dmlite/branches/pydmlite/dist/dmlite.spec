%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}

%if 0%{?rhel} == 5
%global with_python26 1
%endif

%if 0%{?with_python26}
%global __python26 %{_bindir}/python2.6
%global py26dir %{_builddir}/python26-%{name}-%{version}-%{release}
%{!?python26_sitelib: %global python26_sitelib %(%{__python26} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python26_sitearch: %global python26_sitearch %(%{__python26} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}
# Update rpm byte compilation script so that we get the modules compiled by the
# correct inerpreter
%global __os_install_post %__multiple_python_os_install_post
%endif

Name:		dmlite
Version:	0.4.0
Release:	1%{?dist}
Summary:	Common libraries for grid data management and storage
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/Dev/Dmlite
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite/tags/dmlite_0_4_0 dmlite-0.4.0
# tar -czvf dmlite-0.4.0.tar.gz dmlite-0.4.0
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake
BuildRequires:	cppunit-devel
BuildRequires:	boost-devel
BuildRequires:	doxygen
BuildRequires:	openssl-devel
BuildRequires:	python-devel
%if 0%{?with_python26}
BuildRequires:  python26-devel, python26-distribute
%endif

%description
This package provides a set of common libraries and plugins that implement
logic for data management and storage on the grid.

%package libs
Summary:	Common libraries for all dmlite packages
Group:		Applications/Internet

%description libs
This package provides the libraries used by dmlite components.

%package devel
Summary:	Development libraries and headers for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description devel
This package provides headers and development libraries for dmlite.

%package docs
Summary:	API documentation for dmlite
Group:		Applications/Internet

%description docs
Man pages and HTML documentation for dmlite.

%package -n python-dmlite
Summary:	Python wrapper for dmlite
Group:		Development/Libraries

%description -n python-dmlite
This package provides a python wrapper for dmlite.

%if 0%{?with_python26}
%package -n python26-dmlite
Summary:        Python 2.6 wrapper for dmlite
Group:          Development/Libraries
Requires:       python(abi) = 2.6

%description -n python26-dmlite
This package provides a python26 wrapper for dmlite.
%endif #end of python2.6

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_INSTALL_PREFIX=/

make %{?_smp_mflags}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}

make install DESTDIR=%{buildroot}

%if 0%{?with_python26}
mkdir -p %{buildroot}%{python26_sitearch}
install -m 755 -p %{buildroot}%{python_sitearch}/pydmlite.so %{buildroot}%{python26_sitearch}/pydmlite.so
%endif

%clean
rm -rf %{buildroot}

%post libs -p /sbin/ldconfig

%postun libs -p /sbin/ldconfig

%files libs
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/dmlite.conf
%{_libdir}/libdmlite.so.*
%{_libdir}/libdmlitec.so.*
%{_libdir}/libdmliteutils.so.*
%doc README LICENSE

%files devel
%defattr(-,root,root,-)
%{_includedir}/dmlite
%{_libdir}/libdmlite.so
%{_libdir}/libdmlitec.so
%{_libdir}/libdmliteutils.so

%files docs
%defattr(-,root,root,-)
%{_mandir}/man3/*
%{_defaultdocdir}/%{name}-%{version}

%files -n python-dmlite
%{python_sitearch}/pydmlite.so

%if 0%{?with_python26}
%files -n python26-dmlite
%{python26_sitearch}/pydmlite.so
%endif

%changelog
* Fri Jul 13 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.3.0-1
- Update for new upstream release

* Tue Jun 05 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-3
- Removed subversion build dep
- Added patches for proper tests compilation (missing include, wrong cmake dep)

* Sun May 20 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.3.0-1
- Update for new upstream release

* Tue Feb 28 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-2
- Split plugins into multiple packages, added dependencies
- Updated package descriptions

* Tue Jan 31 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.2.0-1
- Added documentation to the build process

* Mon Jan 23 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Added cppunit-devel as a build dependency

* Tue Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
