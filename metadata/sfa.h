
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


#class location {
#    const text country;
#    const text longitude;
#    const text latitude;
#        CAPABILITY(retrieve,join);
#};

#class position {
#	const text x;
#	const text y;
#	const text z;
#	KEY(x,y,z);
#	CAPABILITY(retrieve,join);
#};

class hardware_type {
    const text name;
        CAPABILITY(retrieve,join);
};

class interface {
    const text component_id;
        CAPABILITY(retrieve,join);
};

class initscript {
    const text name;
        CAPABILITY(retrieve,join);
};

class tag {
    const text tagname;
    const text value;
        CAPABILITY(retrieve,join);
};

class resource {
    const text  	urn;
    const text  	hrn;
    const text  	type;
    const text  	network_hrn;
    const text  	hostname;
    const text		component_manager_id;
    const text		component_id;
    bool		exclusive;
    const text		component_name;
    hardware_type 	hardware_types[];
    location 		location;
    interface		interfaces[];
    const text		boot_state;
    initscript		initscripts[]; 		
#    sliver		slivers[];
#    service		services[];
#    location		location;
#	position		position;
    const text country;
    const text longitude;
    const text latitude;
	const text x;
	const text y;
	const text z;
    tag			tags[];  
#    slice		slice[];
	slice	slice[];
    KEY(hrn);
    CAPABILITY(retrieve,join,fullquery);
};


class network {
    const text network_hrn;
    const text network_name;
	const text platform;
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

class authority {
	const text name;
	const text abbreviated_name;
	const text authority_hrn;
	slice slice[];
	user user[];
	KEY(authority_hrn);
	CAPABILITY(retrieve,join,fullquery);
};
