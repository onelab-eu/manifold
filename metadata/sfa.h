
class slice {
    const text  slice_hrn;   /**< Slice Human Readable name */
    resource    resource;    /**< List of resources associated to the slice */
    lease       lease;       /**< List of leases associated to the slice */
    user        user;        /**< List of users associated to the slice */
    KEY(slice_hrn);
};

class lease {
    const text  urn;
    timestamp   start_time;  /**< Start of the lease */ 
    interval    duration;
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

