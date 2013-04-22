# this is a VERY ROUGH / DUMMY specfile for manifold
# in essence it's only here because the PlanetLab build is brain-damaged and
# expects a specfile to be present first-stage

%define name manifold
%define version 0.9
%define taglevel 0

%define release %{taglevel}%{?pldistro:.%{pldistro}}%{?date:.%{date}}

Summary: Manifold Backend
Name: %{name}
Version: %{version}
Release: %{release}
License: GPLv3
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Vendor: OpenLab
Packager: OpenLab <thierry.parmentelat@inria.fr>
URL: %{SCMURL}

# xxx to be refined later 
Requires: python >= 2.7
BuildRequires: python-setuptools make

%description

Manifold offers an infrastructure for easily mixing various feeds,
either testbeds, or measurement frameworks, and aggregate the results
in some unified/consolidated way. Manifold is bhind TopHat and
MySlice, but is designed to be re-usable in any similar context.

%prep
%setup -q

%build
%{__make} buildrpm

%install
rm -rf $RPM_BUILD_ROOT
%{__make} installrpm DESTDIR="$RPM_BUILD_ROOT" datadir="%{_datadir}" bindir="%{_bindir}"

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%dir %{_datadir}/manifold
%{_datadir}/manifold/*

%changelog

