
class ip {
    const inet ip;
    const text hostname;
    KEY(ip);
};

class agent {
    const inet ip;
    const text agentinf;
    KEY(ip);
};

class destination {
    const inet ip;
    const bool inpl;
    KEY(ip);
};

class hop {
    const inet ip;
    const int  ttl;
};

class traceroute {
    const agent       source;
    const destination destination;
    const timestamp   ts;
    const hop         hops[];
    KEY(source, destination, ts);
};


