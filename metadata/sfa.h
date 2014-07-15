class slice {
    const text slice_urn;   /**< Slice Human Readable name */
    const text slice_hrn;   /**< Slice Human Readable name */
    const text slice_type;
    user        users[];        /**< List of users associated to the slice */
    user        pi_users[];        /**< List of users associated to the slice */

    const text slice_description;
    const text created;
    const text slice_expires;
    const text slice_last_updated;
    const text nodes;
    const text slice_url;
    const authority parent_authority;

	resource resource[];
	lease lease[];
# lease       lease[];       /**< List of leases associated to the slice */

    KEY(slice_urn);
    CAPABILITY(retrieve, join, fullquery);
};

class lease {
	slice 		   slice;
    timestamp      start_time;  /**< Start of the lease */ 
	timestamp      end_time;
    interval       duration;
    const resource resource;    /**< Resource URN attached to this lease */
    const string   lease_type;
	const string lease_id;

    interval       granularity; 

    KEY(start_time, end_time, resource);
    CAPABILITY(retrieve, join, fullquery);
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

#   only in nitos
    const string granularity;

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
    const string version;

    KEY(network_hrn);
    CAPABILITY(retrieve, join);
};

class user {
    const string user_hrn;
    const string user_urn;
    const string user_type;
    const string user_email;
    const string user_gid;
    const authority parent_authority;
    const string keys;
    slice slices[];
    authority pi_authorities[];

    const string user_first_name;
    const string user_last_name;
    const string user_phone;
    const string user_enabled;

    KEY(user_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

class authority {
    const string authority_hrn;
    const string name;
    const string abbreviated_name;
    const authority parent_authority;
    user       pi_users[];

    KEY(authority_hrn);
    CAPABILITY(retrieve, join, fullquery);
};
#slice      slices[];
#user       users[];
