
class slice {
    const text  slice_hrn;   /**< Slice Human Readable name */
	const text  slice_type;
    lease       lease[];       /**< List of leases associated to the slice */
    user        user[];        /**< List of users associated to the slice */
    KEY(slice_hrn);
	CAPABILITY(retrieve,join,fullquery);
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
	CAPABILITY(retrieve,join);
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
	const text  urn;
##    text            arch;                   /**< Platform architecture */
#    int             authority_id;           /**< The authority of the global PlanetLab federation that the site of the node belongs to */
#    boot_state      boot_state;             /**< The current status of the node */
#    unsigned        bw_limit;               /**< Bandwidth limits in effect on the node */
#    unsigned        bw_unallocated;
#    const int       client_id;
    const int       component_id;
    const int       component_manager_id;
    const text      component_name;
#    const text      disk_images;
    bool            exclusive;
##    const text      fcdistro;
#    const text      hardware_types;
    const text      hostname;               /**< Hostname */
    const text      resource_hrn;                    /**< Human Readable name */
#    const text      interfaces;
    const int       latitude;               /**< Latitude */
    const int       longitude;              /**< Longitude */
    const network   network;
#    const int       node_id;
#    const bool      pl_initscripts;
##    const pl_distro pldistro;               /**< Fedora or CentOS distribution to use for node or slivers */
#    const text      services;
    const int       site_id;
#    const text      resource_type;
    slice    slice[];
    KEY(resource_hrn);
	CAPABILITY(retrieve,join,fullquery);
};

#>>> prov - need
#set(['load', 'interface.ipv4', 'cpuy', 'cpuw', 'services.login.authentication', 'cpum', 'services.login.username', 'city', 'reliabilityw', 'services.login.port', 'slicesm', 'slices', 'astype', 'bwm', 'memm', 'slicesw', 'type', 'reliabilityy', 'sliver', 'responsew', 'bww', 'mem', 'bwy', 'responsey', 'urn', 'memw', 'interface.client_id', 'fcdistro', 'responsem', 'memy', 'response', 'loadm', 'interface.component_id', 'country', 'region', 'services.login.hostname', 'slicesy', 'loady', 'asnumber', 'bw', 'reliabilitym', 'reliability', 'cpu', 'loadw'])
#>>> need-prov
#set(['disk_images', 'fc_distro', 'bw_limit', 'boot_state', 'interfaces', 'authority_id', 'bw_unallocated', 'hardware_types', 'node_id', 'pl_initscripts', 'client_id', 'services', 'resource_type'])


class network {
    const text network_hrn;
    const text network_name;
    KEY(network_hrn);
	CAPABILITY(retrieve,join);
};

class user {
    const text first_name;
    const text last_name;
    const text email;
    const text telephone;
    const text user_hrn;
    const text password;
    const text site;
    slice slice[];
    KEY(user_hrn);
	CAPABILITY(retrieve,join,fullquery);
};

