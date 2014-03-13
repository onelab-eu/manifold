SRCDIR      = $(CURDIR)/src
TESTDIR     = $(CURDIR)/test
TESTLIB     = $(TESTDIR)/lib
BUILDDIR    = $(CURDIR)/build
DISTDIR     = $(CURDIR)/dist

# overwritten by the specfile
DESTDIR     = /
# it's important to keep this ?= form here please
# as PREFIX gets overridden when calling debuild through an env. variable

# This value must match with the prefix value set in setup.cfg
# Entry points will be put in $(PREFIX)/bin.
# Be careful if you change this path because you might break
# entry-points (try to run manifold-shell if you do so)
#mando|PREFIX      ?= /usr
PREFIX ?= $(shell if [ -f /etc/fedora-release ] ; then echo "/usr"; else echo "/usr/local"; fi)

# stupid distutils, it's broken in so many ways
SUBBUILDDIR = $(shell python -c 'import distutils.util, sys; \
	      print "lib.%s-%s" % (distutils.util.get_platform(), \
	      sys.version[0:3])')
PYTHON25 := $(shell python -c 'import sys; v = sys.version_info; \
    print (1 if v[0] <= 2 and v[1] <= 5 else 0)')

ifeq ($(PYTHON25),0)
BUILDDIR := $(BUILDDIR)/$(SUBBUILDDIR)
else
BUILDDIR := $(BUILDDIR)/lib
endif

PYPATH = $(BUILDDIR):$(TESTLIB):$(PYTHONPATH)
COVERAGE = $(or $(shell which coverage), $(shell which python-coverage), \
	   coverage)

# Added for convenience during development
debug: mrpropre clean all install

all: setup.cfg
	python setup.py build

mrpropre:
	rm -rf "/usr/lib/python*/site-packages/manifold*"
	rm -rf "$(PREFIX)/lib/python*/dist-packages/manifold*"

install: all
	@echo "PREFIX = $(PREFIX)"
	python setup.py install --prefix=$(PREFIX) --root=$(DESTDIR)

test: all
	retval=0; \
	       for i in `find "$(TESTDIR)" -iname '*.py' -perm -u+x -type f`; do \
	       echo $$i; \
	       TESTLIBPATH="$(TESTLIB)" PYTHONPATH="$(PYPATH)" $$i -v || retval=$$?; \
	       done; exit $$retval

coverage: all
	rm -f .coverage
	for i in `find "$(TESTDIR)" -perm -u+x -type f`; do \
		set -e; \
		TESTLIBPATH="$(TESTLIB)" PYTHONPATH="$(PYPATH)" $(COVERAGE) -x $$i -v; \
		done
	$(COVERAGE) -c
	$(COVERAGE) -r -m `find "$(BUILDDIR)" -name \\*.py -type f`
	rm -f .coverage

clean:
	python setup.py clean
	rm -f `find -name \*.pyc` .coverage *.pcap
	rm -Rf dist deb_dist tophat.egg-info

distclean: clean
	rm -rf "$(DISTDIR)"

MANIFEST:
	find . -path ./.hg\* -prune -o -path ./build -prune -o \
		-name \*.pyc -prune -o -name \*.swp -prune -o \
		-name MANIFEST -prune -o -type f -print | \
		sed 's#^\./##' | sort > MANIFEST

dist: MANIFEST
	./setup.py sdist

deb:
	# http://pypi.python.org/pypi/stdeb#stdeb-cfg-configuration-file
	python setup.py --command-packages=stdeb.command bdist_deb
	@echo "Debian package in subdirectory 'deb_dist'."

.PHONY: all clean distclean dist test coverage install MANIFEST deb

########################################
#################### Thierry's additions for the packaging system
########################################
# general stuff
DATE=$(shell date -u +"%a, %d %b %Y %T")

# This is called from the build with the following variables set 
# (see build/Makefile and target_debian)
# (.) RPMTARBALL
# (.) RPMVERSION
# (.) RPMRELEASE
# (.) RPMNAME
DEBVERSION=$(RPMVERSION).$(RPMRELEASE)
DEBTARBALL=../$(RPMNAME)_$(DEBVERSION).orig.tar.bz2

# for fedora/rpm - not used yet ..
buildrpm:
	python setup.py build --prefix=$(PREFIX) --root=$(DESTDIR)

installrpm:
	python setup.py install --prefix=$(PREFIX) --root=$(DESTDIR)

# for debian/ubuntu
debian: debian/changelog debian.source debian.package

force:

debian/changelog: debian/changelog.in
	sed -e "s|@VERSION@|$(DEBVERSION)|" -e "s|@DATE@|$(DATE)|" debian/changelog.in > debian/changelog

debian.source: force 
	rsync -a $(RPMTARBALL) $(DEBTARBALL)

# overriding PREFIX here
# at one point the PL build invoked 'make debian PREFIX=/usr RPMNAME=...'
# so in principle we could have said PREFIX=$(PREFIX)
# however this would be fragile and we want this in /usr anyways
debian.package:
	debuild --set-envvar PREFIX=/usr -uc -us -b 

debian.clean:
	$(MAKE) -f debian/rules clean
	rm -rf build/ MANIFEST ../*.tar.gz ../*.dsc ../*.build
	find . -name '*.pyc' -delete

####################
tags:
	git ls-files | xargs etags

#################### convenience, for debugging only
# make +foo : prints the value of $(foo)
# make ++foo : idem but verbose, i.e. foo=$(foo)
++%: varname=$(subst +,,$@)
++%:
	@echo "$(varname)=$($(varname))"
+%: varname=$(subst +,,$@)
+%:
	@echo "$($(varname))"
