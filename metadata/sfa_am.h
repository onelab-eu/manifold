class slice {
    const text  slice_hrn;     /**< Slice Human Readable name */
    const text  slice_type;
    lease       lease[];       /**< List of leases associated to the slice */
    user        user[];        /**< List of users associated to the slice */
    KEY(slice_hrn);
    CAPABILITY(retrieve, join, fullquery);
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
    CAPABILITY(retrieve, join);
};

class location {
    const text country;
    const text longitude;
    const text latitude;
    CAPABILITY(retrieve, join);
};

class hardware_type {
    const text name;
    CAPABILITY(retrieve, join);
};

class interface {
    const text component_id;
    CAPABILITY(retrieve, join);
};

class resource {
    const text      urn;
    const text      hrn;
    const text      hostname;
    const text        component_manager_id;
    const text        component_id;
    bool        exclusive;
    const text        component_name;
    hardware_type     hardware_types[];
    location         location;
    interface        interfaces[];
    const text        boot_state;
    initscript        initscripts[];         
#    sliver        slivers[];
#    service        services[];
    location        location;
    tag            tags[];  
    slice        slices[];
    KEY(hrn);
    CAPABILITY(retrieve, join, fullquery);
};

class network {
    const text network_hrn;
    const text network_name;
    KEY(network_hrn);
    CAPABILITY(retrieve, join);
};


