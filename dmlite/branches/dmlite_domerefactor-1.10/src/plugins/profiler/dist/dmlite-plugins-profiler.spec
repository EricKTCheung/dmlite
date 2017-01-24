Name:		dmlite-plugins-profiler
Version:	0.6.2
Release:	1%{?dist}
Summary:	Profiler plugin for dmlite
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite-plugins-profiler/tags/dmlite-plugins-profiler_0_5_0 dmlite-plugins-profiler-0.5.0
# tar -czvf dmlite-plugins-profiler-0.5.0.tar.gz dmlite-plugins-profiler-0.5.0
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:	boost-devel >= 1.41.0
%else
BuildRequires:	boost141-devel
%endif
BuildRequires:	cmake
BuildRequires:	dmlite-devel >= 0.6.2

Requires:       dmlite-libs >= 0.6.2


%description
This package provides the profiler plug-in for dmlite. This plug-in is a simple
wrapper around a real plug-in implementation, and is used to do multiple
measurements regarding the performance of each call to dmlite.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_INSTALL_PREFIX=/

make %{?_smp_mflags}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}

make install DESTDIR=%{buildroot}

%clean
rm -rf %{buildroot}

%files 
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_profiler.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/*

%changelog
* Thu Feb 20 2014 Fabrizio Furano <fabrizio.furano@cern.ch> - 0.6.2-1
- Rebuild for dmlite core 0.6 update

* Wed Dec 19 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.5.0-2
- Rebuild for dmlite core 0.6 update

* Thu Oct 25 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.5.0-1
- Update for new upstream release

* Tue Sep 25 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.4.0-1
- Update for new upstream release

* Wed Apr 11 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.3.0-1
- Initial build after dmlite package split
