class hop {
    const inet     ip;             /**< IP discovered */
    const unsigned ttl;            /**< TTL value (0: source, 1: 1st hop ...) */
    const unsigned hop_probecount; /**< Identifies the hop among different paths (LB) */
    const unsigned path;           /**< Probe ID */
    const ip       source;
    const ip       destination;

    CAPABILITY(retrieve, join);
    KEY(source, destination, ttl);
};

class traceroute {
    ip             source;         /**< The measurement agent */
    ip             destination;    /**< The target IP */
    hop            hops[];         /**< IP hops discovered on the measurement */
    unsigned       hop_count;      /**< Number of IP hops */
    timestamp      first;          /**< Birth date of this IP path */
    timestamp      last;           /**< Death date of this IP path */
    unsigned       ts;

    CAPABILITY(join);
    KEY(source, destination);
};
