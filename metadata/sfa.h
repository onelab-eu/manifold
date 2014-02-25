
class slice {
    const string  slice_hrn;   /**< Slice Human Readable name */
    const string  slice_type;
    lease       lease[];       /**< List of leases associated to the slice */
    user        user[];        /**< List of users associated to the slice */

    KEY(slice_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

class lease {
    const resource resource;    /**< Resource URN attached to this lease */
    const string     lease_type;
    const string     network;
    timestamp      start_time;  /**< Start of the lease */ 
    interval       granularity; 
    interval       duration;

    KEY(start_time, duration, resource);
    CAPABILITY(retrieve, join);
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


#class location {
#    const string country;
#    const string longitude;
#    const string latitude;
#
#    CAPABILITY(retrieve, join);
#};

#class position {
#    const string x;
#    const string y;
#    const string z;
#
#    KEY(x, y, z);
#    CAPABILITY(retrieve, join);
#};

class hardware_type {
    const string name;

    CAPABILITY(retrieve, join);
};

class interface {
    const string component_id;

    CAPABILITY(retrieve, join);
};

class initscript {
    const string name;

    CAPABILITY(retrieve, join);
};

class tag {
    const string tagname;
    const string value;

    CAPABILITY(retrieve, join);
};

class resource {
    const string          urn;
    const string          hrn;
    const string          type;
    const string          network_hrn;
    const string          hostname;
    const string          component_manager_id;
    const string          component_id;
    const bool          exclusive;
    const string          component_name;
    const hardware_type hardware_types[];
    const location      location;
    const interface     interfaces[];
    const string          boot_state;
    const string          country;
    const string          longitude;
    const string          latitude;
    const string          x;
    const string          y;
    const string          z;
    initscript          initscripts[];         
    tag                 tags[];  
    slice               slice[];
#   sliver              slivers[];
#   service             services[];
#   position            position;

    KEY(urn);
    CAPABILITY(retrieve, join, fullquery);
};


class network {
    const string network_hrn;
    const string network_name;
    const string platform;

    KEY(network_hrn);
    CAPABILITY(retrieve, join);
};

class user {
    const string first_name;
    const string last_name;
    const string email;
    const string telephone;
    const string user_hrn;
    const string password;
    const string site;
    slice slice[];

    KEY(user_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

class authority {
    const string name;
    const string abbreviated_name;
    const string authority_hrn;
    slice      slice[];
    user       user[];

    KEY(authority_hrn);
    CAPABILITY(retrieve, join, fullquery);
};
