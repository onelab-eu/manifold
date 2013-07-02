#!/bin/bash

[ -d /var/myslice ] || mkdir -p /var/myslice
[ -f /var/myslice/db.sqlite ] || manifold-init-db 

echo "I: Creating platforms"

echo "  > ple"
manifold-add-platform "ple" "PlanetLab Europe" "sfa" "{\"sm\": \"https://www.planet-lab.eu:12346/\", \"registry\": \"https://www.planet-lab.eu:12345/\"}"

echo "  > enable ple"
manifold-enable-platform ple

#echo "  > plc"
#myslice-add-platform "plc" "PlanetLab Central" "SFA" "{\"auth\": \"ple.upmc\", \"user\": \"ple.upmc.slicebrowser\", \"sm\": \"http://www.planet-lab.org:12346/\", \"registry\": \"http://www.planet-lab.org:12345/\", \"user_private_key\": \"/var/myslice/myslice.pkey\"}" "{\"auth_ref\": \"ple\"}"

echo "  > omf"
manifold-add-platform "omf" "NITOS" "sfa" "{\"sm\": \"https://sfa-omf.pl.sophia.inria.fr:12346/\", \"registry\": \"https://sfa-omf.pl.sophia.inria.fr:12345/\"}"

echo "  > enable omf"
manifold-enable-platform omf

echo "  > tophat"
manifold-add-platform "tophat" "TopHat" "XMLRPC" "{\"url\": \"https://api.top-hat.info/API/\"}"

echo "  > enable tophat"
manifold-enable-platform tophat

echo "  > senslab"
manifold-add-platform "senslab" "SensLab" "sfa" "{\"sm\": \"https://sfa.senslab.info:12346/\", \"registry\": \"https://sfa.senslab.info:12345/\"}"

echo "  > enable tophat"
manifold-enable-platform tophat

echo "I: Creating MySlice admin account"
manifold-add-user admin

echo "I: Adding an account for MySlice admin on SFA platforms"

echo "  > ple"
PKEYFILE=/var/myslice/ple.upmc.slicebrowser.pkey
PKEY=$(sed ':a;N;$!ba;s/\n/\\n/g' $PKEYFILE)
# 
# Get the content of the file with newlines appearing in the string as \n
#
# 1. create a register via :a
# 2. append the current and next line to the register via N
# 3. if we are before the last line, branch to the created register $!ba (`$! means not to do it on the last line (as there should be one final newline)).
# 4. finally the substitution replaces every newline with a space on the pattern space (which is the contents of the a register = the whole file.
#
manifold-add-account "admin" "ple" "managed" "{\"user_hrn\": \"ple.upmc.slicebrowser\", \"user_private_key\": \"$PKEY\"}"

echo "  > omf"
manifold-add-account "admin" "omf" "reference" "{\"reference_platform\": \"ple\"}"

echo "  > senslab"
PKEYFILE=/var/myslice/senslab.myslice.pkey
PKEY=$(sed ':a;N;$!ba;s/\n/\\n/g' $PKEYFILE)
manifold-add-account "admin" "senslab" "managed" "{\"user_hrn\": \"senslab.myslice\", \"user_private_key\": \"$PKEY\"}"

echo "I: Creating user accounts"

echo "  > demo"
manifold-add-user demo
manifold-add-account "demo" "ple" "user" "{\"user_hrn\": \"ple.upmc.jordan_auge\"}"
manifold-add-account "demo" "omf" "reference" "{\"reference_platform\": \"ple\"}"

echo "  > jordan.auge@lip6.fr"
manifold-add-user jordan.auge@lip6.fr
manifold-add-account "jordan.auge@lip6.fr" "ple" "user" "{\"user_hrn\": \"ple.upmc.jordan_auge\"}"
manifold-add-account "jordan.auge@lip6.fr" "omf" "reference" "{\"reference_platform\": \"ple\"}"
manifold-add-account "jordan.auge@lip6.fr" "senslab" "user" "{\"user_hrn\": \"senslab.testuser\"}"

# TODO delegate ple and senslab credentials 

