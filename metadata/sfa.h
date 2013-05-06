
class slice {
    const text  slice_hrn;   /**< Slice Human Readable name */
    resource    resource[];    /**< List of resources associated to the slice */
    lease       lease[];       /**< List of leases associated to the slice */
    user        user[];        /**< List of users associated to the slice */
    KEY(slice_hrn);
};

class lease {
    const text  urn;
    timestamp   start_time;  /**< Start of the lease */ 
    interval    granularity; 
    interval    duration;
    text        network;
    const text  hrn;
    const text  lease_type;
    KEY(urn);
};

enum boot_state {
    "online-up and running",
    "good-up and running recently",
    "offline-unreachable today",
    "down-node unreachable for more than one day",
    "failboot-reachable but only by administrators for debugging purposes"
};

enum pl_distro {
    "f8",
    "f12",
    "Cent/OS",
    "other",
    "n/a"
};

class resource {
    text            arch;                   /**< Platform architecture */
    int             authority_id;           /**< The authority of the global PlanetLab federation that the site of the node belongs to */
    boot_state      boot_state;             /**< The current status of the node */
    unsigned        bw_limit;               /**< Bandwidth limits in effect on the node */
    unsigned        bw_unallocated;
    const int       client_id;
    const int       component_id;
    const int       component_manager_id;
    const text      component_name;
    const text      disk_images;
    bool            exclusive;
    const text      fc_distro;
    const text      hardware_types;
    const text      hostname;               /**< Hostname */
    const text      hrn;                    /**< Human Readable name */
    const text      interfaces;
    const int       latitude;               /**< Latitude */
    const int       longitude;              /**< Longitude */
    const network   network;
    const int       node_id;
    const bool      pl_initscripts;
    const pl_distro pldistro;               /**< Fedora or CentOS distribution to use for node or slivers */
    const text      services;
    const int       site_id;
    const text      resource_type;
    KEY(hrn);
};

class network {
    const text network_hrn;
    const text network_name;
    KEY(network_hrn);
};

class user {
    const text first_name;
    const text last_name;
    const text email;
    const text telephone;
    const text user_hrn;
    const text password;
    const text site;
    KEY(user_hrn);
};

