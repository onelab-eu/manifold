enum as_type {
    "t1-tier1",
    "t2-tier2",
    "edu-university",
    "comp-company",
    "nic-network information centre; old name for a domain name registry operator",
    "ix-IXP",
    "n/a"
};

class node {
    const unsigned asn;       /**< Autonomous System Number */
    const as_type  as_type;   /**< Autonomous System type */
    const text     as_name;   /**< Autonomous System name */
    const text     hostname;
    const text     city;      /**< Based on the latitude and longitude information */
    const text     country;   /**< Based on the latitude and longitude information */
    const text     region;    /**< Based on the latitude and longitude information */
    const text     continent; /**< Based on the latitude and longitude information */
    const inet     ip;
    const text     peer_name;
    KEY(hostname);
};

//class traceroute {
//    const inet      src_ip;
//    const inet      dst_ip;
//    const hop       hops[];
//    const timestamp first;
//    const timestamp last;
//    KEY(src_ip, dst_ip, first);
//};

//class hop {
//    const inet ip;
//    const int  ttl;
//};

class ip {
    const inet ip;
    const text hostname;
    KEY(ip);
};

