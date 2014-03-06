// Those metadata wrap an SFA Ressource Manager.
// http://opensfa.info/doc/opensfa.html
//
// Copyright (C) UPMC Paris Universitas
// Authors:
//   Jordan Auge       <jordan.auge@lip6.fr>
//   Loic Baron        <loic.baron@lip6.fr> 
//   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
//   Amine Larabi      <mohamed.larabi@inria.fr>

class lease {
    const resource resource;
    timestamp      start_time;  /**< Start of the lease */ 
    interval       granularity; 
    interval       duration;
    string           network;
    const string     lease_type;
    slice          slices[];    /**< Backward reference to sfa_rm::slice */ 

    KEY(start_time, duration, resource);
    CAPABILITY(retrieve, join);
};

class location {
    const string     country;
    const string     longitude;
    const string     latitude;

    CAPABILITY(retrieve, join);
};

#class hardware_type {
#    const          string name;
#
#    CAPABILITY(retrieve, join);
#};

class interface {
    const string     component_id;

    CAPABILITY(retrieve, join);
};

class resource {
    const string     urn;
    const string     hrn;
    const string     hostname;
    const string     component_manager_id;
    const string     component_id;
    bool           exclusive;
    const string     component_name;
    hardware_type  hardware_types[];
    location       location;
    interface      interfaces[];
    const string     boot_state;
    initscript     initscripts[];         
#    sliver         slivers[];
#    service        services[];
    location       location;
    tag            tags[];  
    slice          slices[];

    KEY(hrn);
    CAPABILITY(retrieve, join, fullquery);
};

class network {
    const string     network_hrn;
    const string     network_name;

    KEY(network_hrn);
    CAPABILITY(retrieve, join);
};


