# This holds for both queries and records by default
manifold-tables -F
#manifold-tables -A -o local:user -f password -a R -j LOG
#manifold-tables -A -o local:user -f password -a R -j DROP
manifold-tables -A -o \* 		 -f \* 		 -a R -j CACHE
manifold-tables -A -o ple:authority         -f \*       -a R -j CACHE
