/* Get a Request RSpec based on a list of resources and leases */
class req_rspec {
    const string    xml;
    resource        resource[];
    lease           lease[];
    slice           slice;
    KEY(xml);
    CAPABILITY(retrieve, join, fullquery);
};

/* Get an Advertisement RSpec about a slice */
class ad_rspec {
    const string ad_xml;
    slice slice;
    KEY(ad_xml);
    CAPABILITY(retrieve, join, fullquery);
};

class slice {
    const string slice_urn;             /**< Slice Unique Resource Name */
    const string slice_hrn;             /**< Slice Human Readable name */
    const string slice_type;
    user        users[];                /**< List of users associated to the slice */
    user        pi_users[];             /**< List of users associated to the slice */

    const string slice_date_created;
    const string slice_expires;
    const string slice_last_updated;
#    const string nodes;
    const string slice_enabled;         /**< MyPLC field slice_enabled >**/
    const authority parent_authority;

#    const string slice_url;            /**< MyPLC field slice_url >**/
#    const string slice_description;    /**< MyPLC field slice_description >**/

    geni_slivers sliver[];

	resource resource[];
	lease lease[];                      /**< List of leases associated to the slice */
#	flowspace flowspace[];              /**< List of flowspaces associated to the slice */
#	vms vms[];              /**< List of flowspaces associated to the slice */

#    initscript initscript;         
    KEY(slice_urn);
    CAPABILITY(retrieve, join, fullquery);
};

class sliver {
    const string geni_sliver_urn;
    const string geni_expires;
    const string geni_allocation_status;
    const string geni_operational_status;
    KEY(geni_sliver_urn);
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

    KEY(lease_id, start_time, end_time, resource);
    CAPABILITY(retrieve, join, fullquery);
};

class flowspace {
    slice slice;
    const string controller;
    const string groups[];
    const string matches[];
    KEY(controller, groups, matches);
    CAPABILITY(retrieve, join, fullquery);
};

#class vms {
#    slice slice;
#    const resource resource;    /**< Resource URN attached to this lease */
#    const string vm;
#    KEY(vm);
#    CAPABILITY(retrieve, join, fullquery);
#};

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
	const string          facility_name;
	const string          testbed_name;

    const string          hostname;
    const string          component_manager_id;
    const string          component_id;
    const bool            exclusive;
    const string          component_name;
    const hardware_type   hardware_types[];
    const location        location;
    const interface       interfaces[];
    const string          boot_state;
    const string          country;
    const string          longitude;
    const string          latitude;

# For Nitos and iMinds
    const bool            available;

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
    const string name;                 /**< MyPLC field authority name >**/
#    const string abbreviated_name;     /**< MyPLC field authority abv name >**/
    const authority parent_authority;
    user       pi_users[];

    KEY(authority_hrn);
    CAPABILITY(retrieve, join, fullquery);
};
#slice      slices[];
#user       users[];
