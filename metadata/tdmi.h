class hops {
    inet      ip;             /**< IP discovered */
    unsigned  ttl;            /**< TTL value (0: source, 1: 1st hop ...) */
    unsigned  hop_probecount; /**< Identifies the hop among different paths (LB) */
    unsigned  path;           /**< Probe ID */
    CAPABILITY(join);
};

class traceroute {
   inet        src_ip;         /**< The agent which has performed the measurement */
   inet        dst_ip;         /**< The destination of the traceroute measurement */
//   ip_hop      hops[];         /**< IP hops discovered on the measurement */
   hops        hops[];         /**< IP hops discovered on the measurement */
   unsigned    hop_count;      /**< Number of IP hops */
   timestamp   first;          /**< Birth date of this IP path */
   timestamp   last;           /**< Death date of this IP path */
   timestamp   first_tr;       /**< Birth date of this IP pattern */
   timestamp   last_tr;        /**< Death date of this IP pattern */
   timestamp   first_ts;       /**< Birth date of this mask of stars */
   timestamp   last_ts;        /**< Death date of this mask of stars */
   tool        tool_id;        /**< Tool used to perform the measurement */

   CAPABILITY(retrieve, join, selection);
   KEY(src_ip, dst_ip, first);
};
