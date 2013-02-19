
class ip {
    const inet ip;
    const text hostname;
    KEY(ip);
};

class agent {
    const ip   ip;
    const text agentinf;
    KEY(ip);
};

class destination {
    const ip   ip;
    const bool inpl;
    KEY(ip);
};

class hop {
    const ip   ip;
    const int  ttl;
};

class traceroute {
    const agent       source;
    const destination destination;
    const timestamp   ts;
    const hop         hops[];
    KEY(source, destination, ts);
};


