class hop {
    const inet        ip;             /**< IP discovered */
    const unsigned    ttl;            /**< TTL value (0: source, 1: 1st hop ...) */
    const unsigned    hop_probecount; /**< Identifies the hop among different paths (LB) */
    const unsigned    path;           /**< Probe ID */

    const agent       agent; 
    const destination destination; 
    KEY(agent, destination, ttl);

    // - join is an crappy way to allows to connect traceroute -> hop in the db_graph
    CAPABILITY(join);
};

class traceroute {
    agent       agent;          /**< The measurement agent */
    destination destination;    /**< The target IP */
    hop         hops[];         /**< IP hops discovered on the measurement */
    unsigned    hop_count;      /**< Number of IP hops */
    timestamp   first;          /**< Birth date of this IP path */
    timestamp   last;           /**< Death date of this IP path */

    CAPABILITY(join, selection, projection);
    KEY(agent, destination);
};
