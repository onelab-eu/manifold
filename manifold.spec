# <<<<<<<<<<<<<< DO NOT USE, CONFIGURE setup.cfg INSTEAD >>>>>>>>>>>>>>>>>>>>>>>>>

# this is a VERY ROUGH / DUMMY specfile for manifold
# in essence it's only here because the PlanetLab build is brain-damaged and
# expects a specfile to be present first-stage

%define name manifold
# usually (with the PL build) the specfile is the reference location
# where the version number is set; here whoever we keep the original 
# way of managing version number in the python source
# module-tools probably won't work nice here
# xxx this would be the right way to go but unfortunately it won't run
# under spec2make...
# %define version %(python -c "import tophat; print tophat.__version__")
# so we need to keep both places in sync (tophat/__init__.py)
#mando:%define version 2.0
%define version %(python -c 'import manifold; print ".".join(["%s" % x for x in manifold.__version__])')
%define taglevel 0

%define release %{taglevel}%{?pldistro:.%{pldistro}}%{?date:.%{date}}

Summary: Manifold Backend
Name: %{name}
Version: %{version}
Release: %{release}
License: GPLv3
Source0: %{name}-%{version}.tar.bz2
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
in some unified/consolidated way. Manifold is behind TopHat and
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

# this specfile being not really used for now we don't bother to define
# fine-grained packages in here for now
%files
%defattr(-,root,root,-)
%dir %{_datadir}/manifold
%{_datadir}/manifold/*

%changelog

