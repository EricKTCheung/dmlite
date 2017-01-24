%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}

Name:		dmlite-shell
Version:	0.6.2
Release:	2%{?dist}
Summary:	Shell environment for dmlite
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite-shell/tags/dmlite-shell_0_6_2 dmlite-shell-0.6.2
# tar -czvf dmlite-shell-0.6.2.tar.gz dmlite-shell-0.6.2
Source0:	%{name}-%{version}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:	cmake
BuildRequires:	python2-devel
BuildArch:		noarch

Requires:		python-dateutil
Requires:		python-dmlite

%description
This package provides a shell environment for dmlite. It includes useful
commands for system administration, testers and power users.

%prep
%setup -q 

%build
%cmake \
-DCMAKE_INSTALL_PREFIX=/ \
.

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%clean
rm -rf %{buildroot}

%files 
%defattr(-,root,root,-)
%{_bindir}/*
%{python_sitelib}/dmliteshell
%doc LICENSE README RELEASE-NOTES

%changelog
* Wed Mar 12 2014 Adrien Devresse <adevress at cern.ch> - 0.6.2-2
 - Fedora/EPEL push dmlite shell 0.6.2 

* Mon Mar 03 2014 Ivan Calvet <ivan.calvet at cern.ch> - 0.6.2-1
 - Release of dmlite shell version 6.2

* Mon Nov 04 2013 Adrien Devresse <adevress at cern.ch> - 0.2.1-2
 - Initial release of dmlite shell
