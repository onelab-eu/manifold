class hop {
#    unsigned  hop_id;           /**< Dummy field */
    inet      ip;               /**< IP discovered */
    unsigned  ttl;              /**< TTL value (0: source, 1: 1st hop ...) */
    unsigned  hop_probecount;   /**< Identifies the hop among different paths (LB) */
    unsigned  path;             /**< Probe ID */

    CAPABILITY(retrieve, join);
#    KEY(hop_id);
    KEY(ip, ttl, hop_probecount, path);
};

class traceroute {
    agent       agent;          /**< The measurement agent */
    destination destination;    /**< The target IP */
    hop         hops[];         /**< IP hops discovered on the measurement */
    unsigned    hop_count;      /**< Number of IP hops */
    timestamp   first;          /**< Birth date of this IP path */
    timestamp   last;           /**< Death date of this IP path */

    CAPABILITY(join, selection, projection);
#CAPABILITY(retrieve, join, selection, projection);
    KEY(agent, destination);
};
