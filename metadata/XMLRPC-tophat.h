enum as_type {
    "t1-tier1",
    "t2-tier2",
    "edu-university",
    "comp-company",
    "nic-network information centre; old name for a domain name registry operator",
    "ix-IXP",
    "n/a"
};

class nodes {
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

