class hop {
    unsigned  hop_id;           /**< Dummy field */
    inet      ip;               /**< IP discovered */
    unsigned  ttl;              /**< TTL value (0: source, 1: 1st hop ...) */
    unsigned  hop_probecount;   /**< Identifies the hop among different paths (LB) */
    unsigned  path;             /**< Probe ID */

    CAPABILITY(retrieve, join);
    KEY(hop_id);
};

class traceroute {
    unsigned    agent_id;       /**< DUMMY FIELD Needed to optimize traceroute queries */
    unsigned    destination_id; /**< DUMMY FIELD Needed to optimize traceroute queries */
    agent       agent;          /**< The measurement agent */
    destination destination;    /**< The target IP */
    inet        src_ip;         /**< The agent which has performed the measurement */
    inet        dst_ip;         /**< The destination of the traceroute measurement */
    hop         hops[];         /**< IP hops discovered on the measurement */
    unsigned    hop_count;      /**< Number of IP hops */
    timestamp   first;          /**< Birth date of this IP path */
    timestamp   last;           /**< Death date of this IP path */

    CAPABILITY(retrieve, join, selection, projection);
    KEY(src_ip, dst_ip, first);
};
