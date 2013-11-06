manifold-tables -F
manifold-tables -A -o local:user -f password -a R -j LOG
manifold-tables -A -o local:user -f password -a R -j DROP
manifold-tables -A -o \* 		 -f \* 		 -a R -j CACHE
